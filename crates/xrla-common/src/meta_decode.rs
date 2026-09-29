/// Minimal generic decoder for xrpld's canonical binary STObject format, applied to
/// transaction `meta_blob`s. Goal: find every `AccountID`-typed field value anywhere in a
/// transaction's metadata (nested arbitrarily inside `AffectedNodes`/`FinalFields`/etc.),
/// without needing a full field-name table — just the type-level wire rules, which are far
/// less error-prone than hand-copying a field-code table (see the 2026-07-08 sparse-inner-node
/// bit-order bug: getting one detail of a binary format wrong is silent and easy to miss).
///
/// Field/type codes confirmed against xrpld's `SField.h` `SerializedTypeID` enum and
/// `ripple-binary-codec`'s `definitions.json` (2026-07-28).
use anyhow::{bail, Result};

const TYPE_UINT16: u32 = 1;
const TYPE_UINT32: u32 = 2;
const TYPE_UINT64: u32 = 3;
const TYPE_UINT128: u32 = 4; // Hash128
const TYPE_UINT256: u32 = 5; // Hash256
const TYPE_AMOUNT: u32 = 6;
const TYPE_VL: u32 = 7; // Blob
const TYPE_ACCOUNT: u32 = 8; // AccountID
const TYPE_OBJECT: u32 = 14;
const TYPE_ARRAY: u32 = 15;
const TYPE_UINT8: u32 = 16;
const TYPE_UINT160: u32 = 17; // Hash160
const TYPE_VECTOR256: u32 = 19;
const TYPE_UINT96: u32 = 20;
const TYPE_UINT192: u32 = 21;
const TYPE_UINT384: u32 = 22;
const TYPE_UINT512: u32 = 23;
const TYPE_ISSUE: u32 = 24; // 20-byte currency, +20-byte issuer if non-native

struct Cursor<'a> {
    b: &'a [u8],
    i: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.i + n > self.b.len() {
            bail!("unexpected end of buffer (need {n} bytes at offset {})", self.i);
        }
        let s = &self.b[self.i..self.i + n];
        self.i += n;
        Ok(s)
    }
    fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn eof(&self) -> bool {
        self.i >= self.b.len()
    }

    /// xrpld variable-length prefix: inverse of `tx_tree::write_vl`.
    fn read_vl_len(&mut self) -> Result<usize> {
        let b1 = self.byte()? as usize;
        if b1 <= 192 {
            Ok(b1)
        } else if b1 <= 240 {
            let b2 = self.byte()? as usize;
            Ok(193 + (b1 - 193) * 256 + b2)
        } else {
            let b2 = self.byte()? as usize;
            let b3 = self.byte()? as usize;
            Ok(12_481 + (b1 - 241) * 65_536 + b2 * 256 + b3)
        }
    }

    /// Field header: (type_code, field_code). See xrpld `SField::getField` wire format.
    fn read_field_header(&mut self) -> Result<(u32, u32)> {
        let b1 = self.byte()? as u32;
        let mut type_code = b1 >> 4;
        let mut field_code = b1 & 0x0F;
        if type_code == 0 {
            type_code = self.byte()? as u32;
        }
        if field_code == 0 {
            field_code = self.byte()? as u32;
        }
        Ok((type_code, field_code))
    }
}

/// Every distinct 20-byte AccountID found anywhere in the metadata (deduped).
pub fn accounts_touched_by_meta(meta_blob: &[u8]) -> Result<Vec<[u8; 20]>> {
    let mut out = Vec::new();
    let mut c = Cursor { b: meta_blob, i: 0 };
    walk_object(&mut c, &mut out)?;
    out.sort_unstable();
    out.dedup();
    Ok(out)
}

/// Parse fields until the object end-marker (type=OBJECT, field=1) or buffer end.
fn walk_object(c: &mut Cursor, out: &mut Vec<[u8; 20]>) -> Result<()> {
    loop {
        if c.eof() {
            return Ok(());
        }
        let (type_code, field_code) = c.read_field_header()?;
        if type_code == TYPE_OBJECT && field_code == 1 {
            return Ok(()); // ObjectEndMarker
        }
        read_value(c, type_code, out)?;
    }
}

/// Parse array elements (each itself a single-field object) until the array end-marker
/// (type=ARRAY, field=1).
fn walk_array(c: &mut Cursor, out: &mut Vec<[u8; 20]>) -> Result<()> {
    loop {
        let (type_code, field_code) = c.read_field_header()?;
        if type_code == TYPE_ARRAY && field_code == 1 {
            return Ok(()); // ArrayEndMarker
        }
        // One array element: a single field (usually STObject, e.g. CreatedNode/ModifiedNode),
        // itself terminated by its own ObjectEndMarker.
        read_value(c, type_code, out)?;
    }
}

fn read_value(c: &mut Cursor, type_code: u32, out: &mut Vec<[u8; 20]>) -> Result<()> {
    match type_code {
        TYPE_UINT8 => { c.take(1)?; }
        TYPE_UINT16 => { c.take(2)?; }
        TYPE_UINT32 => { c.take(4)?; }
        TYPE_UINT64 => { c.take(8)?; }
        TYPE_UINT96 => { c.take(12)?; }
        TYPE_UINT128 => { c.take(16)?; }
        TYPE_UINT160 => { c.take(20)?; }
        TYPE_UINT192 => { c.take(24)?; }
        TYPE_UINT256 => { c.take(32)?; }
        TYPE_UINT384 => { c.take(48)?; }
        TYPE_UINT512 => { c.take(64)?; }
        TYPE_AMOUNT => {
            let first = c.take(1)?[0];
            if first & 0x80 == 0 {
                // native XRP: 8 bytes total, 1 already consumed
                c.take(7)?;
            } else {
                // issued currency: 8 (value, 1 consumed) + 20 (currency) + 20 (issuer)
                c.take(7)?;
                c.take(20)?;
                c.take(20)?;
            }
        }
        TYPE_VL => {
            let len = c.read_vl_len()?;
            c.take(len)?;
        }
        TYPE_ACCOUNT => {
            let len = c.read_vl_len()?;
            let bytes = c.take(len)?;
            if bytes.len() == 20 {
                let mut a = [0u8; 20];
                a.copy_from_slice(bytes);
                out.push(a);
            }
            // Non-20-byte AccountID payloads shouldn't occur on real ledger data; ignore
            // rather than fail, since this decoder's job is best-effort account extraction.
        }
        TYPE_VECTOR256 => {
            let len = c.read_vl_len()?;
            c.take(len)?;
        }
        TYPE_ISSUE => {
            let currency = c.take(20)?;
            if currency.iter().any(|&b| b != 0) {
                c.take(20)?; // issuer, only present for non-native currency
            }
        }
        TYPE_OBJECT => walk_object(c, out)?,
        TYPE_ARRAY => walk_array(c, out)?,
        other => bail!("unhandled STI type code {other} in metadata (unsupported field type)"),
    }
    Ok(())
}

/// Base58check (XRPL alphabet) encode of a 20-byte AccountID into a classic `r...` address.
pub fn account_id_to_classic_address(account_id: &[u8; 20]) -> String {
    const ALPHABET: &[u8] = b"rpshnaf39wBUDNEGHJKLM4PQRST7VWXYZ2bcdeCg65jkm8oFqi1tuvAxyz";
    let mut payload = Vec::with_capacity(25);
    payload.push(0x00u8); // classic address type prefix
    payload.extend_from_slice(account_id);
    let checksum = {
        let h1 = crate::serialize::sha256(&payload);
        let h2 = crate::serialize::sha256(&h1);
        h2[0..4].to_vec()
    };
    payload.extend_from_slice(&checksum);

    // Big-integer base58 encode.
    let mut digits: Vec<u8> = vec![0];
    for &byte in &payload {
        let mut carry = byte as u32;
        for d in digits.iter_mut() {
            let v = (*d as u32) * 256 + carry;
            *d = (v % 58) as u8;
            carry = v / 58;
        }
        while carry > 0 {
            digits.push((carry % 58) as u8);
            carry /= 58;
        }
    }
    // Leading zero bytes -> leading '1'-equivalent (alphabet[0] = 'r') in the encoding.
    let leading_zeros = payload.iter().take_while(|&&b| b == 0).count();
    let mut s: String = std::iter::repeat(ALPHABET[0] as char).take(leading_zeros).collect();
    s.extend(digits.iter().rev().map(|&d| ALPHABET[d as usize] as char));
    s
}
