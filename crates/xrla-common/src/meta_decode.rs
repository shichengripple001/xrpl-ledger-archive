//! Decoder for xrpld's canonical binary STObject format, applied to transaction `meta_blob`s,
//! implementing **exactly** xrpld's rule for which accounts a transaction affected.
//!
//! The rule is `TxMeta::getAffectedAccounts` (`src/libxrpl/protocol/TxMeta.cpp`), which is also
//! what xrpld writes into `AccountTransactions` and what `account_tx` answers from:
//!
//! ```text
//! for node in meta[sfAffectedNodes]:
//!     inner = node[sfNewFields] if CreatedNode else node[sfFinalFields]
//!     if inner absent: skip the node
//!     for field in inner:                       // one level only, no recursion
//!         ACCOUNT field, unless empty-encoded   -> add it (a 20-byte zero account IS added)
//!         sfTakerPays/sfTakerGets/sfLowLimit/sfHighLimit -> add the amount's issuer
//!         sfMPTokenIssuanceID                   -> add the issuer inside the ID
//! ```
//!
//! What is deliberately *not* counted: `sfPreviousFields`, anything nested deeper than the first
//! level of `NewFields`/`FinalFields`, and the issuer of every other amount.
//!
//! An earlier version of this module collected every `AccountID` anywhere in the metadata and
//! skipped amount issuers. Measured against xrpld's own rows for 400 real transactions it was
//! wrong on 191 of them (299 account rows missing, none extra): a trustline (`RippleState`) has no
//! account field at all — both parties exist only as the issuers of `LowLimit`/`HighLimit` — so the
//! recipient of any issued-currency payment was silently absent.
//!
//! This module therefore depends on a small set of field codes (the constants below). They are
//! protocol-frozen, and were taken from `include/xrpl/protocol/detail/sfields.macro`. Everything
//! else is skipped using only the type-level wire rules. Any type this decoder does not know how
//! to skip is an error, never silently ignored: a desync here produces silently wrong accounts.
use std::collections::BTreeSet;

use anyhow::{anyhow, bail, Result};

// SerializedTypeID (include/xrpl/protocol/SField.h)
const TYPE_UINT16: u32 = 1;
const TYPE_UINT32: u32 = 2;
const TYPE_UINT64: u32 = 3;
const TYPE_UINT128: u32 = 4; // Hash128
const TYPE_UINT256: u32 = 5; // Hash256
const TYPE_AMOUNT: u32 = 6;
const TYPE_VL: u32 = 7; // Blob
const TYPE_ACCOUNT: u32 = 8; // AccountID
const TYPE_NUMBER: u32 = 9; // 8-byte mantissa + 4-byte exponent
const TYPE_INT32: u32 = 10;
const TYPE_INT64: u32 = 11;
const TYPE_OBJECT: u32 = 14;
const TYPE_ARRAY: u32 = 15;
const TYPE_UINT8: u32 = 16;
const TYPE_UINT160: u32 = 17; // Hash160
const TYPE_VECTOR256: u32 = 19;
const TYPE_UINT96: u32 = 20;
const TYPE_UINT192: u32 = 21;
const TYPE_UINT384: u32 = 22;
const TYPE_UINT512: u32 = 23;
const TYPE_ISSUE: u32 = 24;
const TYPE_CURRENCY: u32 = 26; // 20 bytes

// Field codes the rule depends on (sfields.macro)
const F_AFFECTED_NODES: u32 = 8; // ARRAY
const F_CREATED_NODE: u32 = 3; // OBJECT; DeletedNode = 4, ModifiedNode = 5
const F_FINAL_FIELDS: u32 = 7; // OBJECT
const F_NEW_FIELDS: u32 = 8; // OBJECT
const F_TAKER_PAYS: u32 = 4; // AMOUNT
const F_TAKER_GETS: u32 = 5; // AMOUNT
const F_LOW_LIMIT: u32 = 6; // AMOUNT
const F_HIGH_LIMIT: u32 = 7; // AMOUNT
const F_MPT_ISSUANCE_ID: u32 = 1; // UINT192
const F_TRANSACTION_INDEX: u32 = 28; // UINT32

/// `STAmount` flag bits in the first byte of the 64-bit value (STAmount.h):
/// not-native (an issued currency) and MPT.
const AMOUNT_ISSUED_CURRENCY: u8 = 0x80;
const AMOUNT_MPT: u8 = 0x20;

/// `noAccount()` — the 20-byte account `0x00…01` that marks an MPT inside an `STIssue`.
const NO_ACCOUNT: [u8; 20] = {
    let mut a = [0u8; 20];
    a[19] = 1;
    a
};

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
    fn peek(&self) -> Result<u8> {
        self.b
            .get(self.i)
            .copied()
            .ok_or_else(|| anyhow!("unexpected end of buffer at offset {}", self.i))
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

fn nonzero(b: &[u8]) -> Option<[u8; 20]> {
    let a = <[u8; 20]>::try_from(b).ok()?;
    if a == [0u8; 20] {
        None
    } else {
        Some(a)
    }
}

/// Read an `STAccount`.
///
/// A zero-length payload is xrpld's "default" account and is never indexed. A 20-byte payload is
/// *always* indexed — even if every byte is zero — because xrpld's `STAccount::isDefault()` is a
/// flag set only by the empty encoding (`STAccount.cpp`), not a test of the value. (The issuer
/// paths below do use a zero test, because `getAffectedAccounts` calls `isNonZero()` there.)
/// Any other length is invalid; xrpld throws on it too.
fn read_account(c: &mut Cursor) -> Result<Option<[u8; 20]>> {
    match c.read_vl_len()? {
        0 => Ok(None),
        20 => {
            let mut a = [0u8; 20];
            a.copy_from_slice(c.take(20)?);
            Ok(Some(a))
        }
        n => bail!("invalid AccountID length {n}"),
    }
}

/// Read an `STAmount` and return its issuer if it has a non-zero one.
///
/// Three wire shapes (STAmount.cpp, `STAmount(SerialIter&, SField const&)`):
/// - issued currency (bit 0x80): 8-byte value, 20-byte currency, 20-byte issuer
/// - MPT (bit 0x20, 0x80 clear): 8-byte value, 1 further byte, 24-byte MPT issuance ID. The
///   issuer is the trailing 20 bytes of the ID (`makeMptID` = 4-byte sequence ‖ 20-byte issuer)
/// - native XRP: 8 bytes, no issuer
fn read_amount(c: &mut Cursor) -> Result<Option<[u8; 20]>> {
    let first = c.peek()?;
    if first & AMOUNT_ISSUED_CURRENCY != 0 {
        c.take(8)?;
        c.take(20)?; // currency
        Ok(nonzero(c.take(20)?))
    } else if first & AMOUNT_MPT != 0 {
        c.take(8)?;
        c.take(1)?;
        let id = c.take(24)?;
        Ok(nonzero(&id[4..24]))
    } else {
        c.take(8)?;
        Ok(None)
    }
}

/// `STIssue`: 20 bytes; all-zero means XRP and nothing follows. Otherwise another 20 bytes
/// follow, and if those are `noAccount()` it is an MPT with a 4-byte sequence after them
/// (44 bytes total) — not an issued currency (40 bytes).
fn skip_issue(c: &mut Cursor) -> Result<()> {
    let first = c.take(20)?;
    if first.iter().all(|&b| b == 0) {
        return Ok(());
    }
    let second = c.take(20)?;
    if second == NO_ACCOUNT {
        c.take(4)?;
    }
    Ok(())
}

/// Consume one value of `type_code` without interpreting it. Containers are walked to their end
/// marker. Unknown types are an error rather than a guess.
fn skip_value(c: &mut Cursor, type_code: u32) -> Result<()> {
    match type_code {
        TYPE_UINT8 => {
            c.take(1)?;
        }
        TYPE_UINT16 => {
            c.take(2)?;
        }
        TYPE_UINT32 | TYPE_INT32 => {
            c.take(4)?;
        }
        TYPE_UINT64 | TYPE_INT64 => {
            c.take(8)?;
        }
        TYPE_NUMBER => {
            c.take(12)?;
        }
        TYPE_UINT96 => {
            c.take(12)?;
        }
        TYPE_UINT128 => {
            c.take(16)?;
        }
        TYPE_UINT160 | TYPE_CURRENCY => {
            c.take(20)?;
        }
        TYPE_UINT192 => {
            c.take(24)?;
        }
        TYPE_UINT256 => {
            c.take(32)?;
        }
        TYPE_UINT384 => {
            c.take(48)?;
        }
        TYPE_UINT512 => {
            c.take(64)?;
        }
        TYPE_AMOUNT => {
            read_amount(c)?;
        }
        TYPE_VL | TYPE_VECTOR256 | TYPE_ACCOUNT => {
            let len = c.read_vl_len()?;
            c.take(len)?;
        }
        TYPE_ISSUE => skip_issue(c)?,
        TYPE_OBJECT => loop {
            let (t, f) = c.read_field_header()?;
            if t == TYPE_OBJECT && f == 1 {
                break; // ObjectEndMarker
            }
            skip_value(c, t)?;
        },
        TYPE_ARRAY => loop {
            let (t, f) = c.read_field_header()?;
            if t == TYPE_ARRAY && f == 1 {
                break; // ArrayEndMarker
            }
            skip_value(c, t)?;
        },
        other => bail!("unhandled STI type code {other} in metadata (unsupported field type)"),
    }
    Ok(())
}

/// The first level of a node's `NewFields`/`FinalFields`: the only place accounts are read from.
fn collect_inner(c: &mut Cursor, out: &mut BTreeSet<[u8; 20]>) -> Result<()> {
    loop {
        let (t, f) = c.read_field_header()?;
        if t == TYPE_OBJECT && f == 1 {
            return Ok(());
        }
        match t {
            TYPE_ACCOUNT => {
                if let Some(a) = read_account(c)? {
                    out.insert(a);
                }
            }
            TYPE_AMOUNT if matches!(f, F_TAKER_PAYS | F_TAKER_GETS | F_LOW_LIMIT | F_HIGH_LIMIT) => {
                if let Some(issuer) = read_amount(c)? {
                    out.insert(issuer);
                }
            }
            TYPE_UINT192 if f == F_MPT_ISSUANCE_ID => {
                let id = c.take(24)?;
                if let Some(issuer) = nonzero(&id[4..24]) {
                    out.insert(issuer);
                }
            }
            // Everything else — including nested objects and arrays — is skipped, not searched.
            _ => skip_value(c, t)?,
        }
    }
}

/// One element of `AffectedNodes` (a CreatedNode/ModifiedNode/DeletedNode body).
fn collect_node(c: &mut Cursor, wanted: u32, out: &mut BTreeSet<[u8; 20]>) -> Result<()> {
    loop {
        let (t, f) = c.read_field_header()?;
        if t == TYPE_OBJECT && f == 1 {
            return Ok(());
        }
        if t == TYPE_OBJECT && f == wanted {
            collect_inner(c, out)?;
        } else {
            // PreviousFields, LedgerEntryType, LedgerIndex, ... are skipped.
            skip_value(c, t)?;
        }
    }
}

fn collect_affected_nodes(c: &mut Cursor, out: &mut BTreeSet<[u8; 20]>) -> Result<()> {
    loop {
        let (t, f) = c.read_field_header()?;
        if t == TYPE_ARRAY && f == 1 {
            return Ok(());
        }
        if t != TYPE_OBJECT {
            bail!("unexpected field type {t} inside AffectedNodes");
        }
        // CreatedNode reads NewFields; Modified/Deleted read FinalFields.
        let wanted = if f == F_CREATED_NODE { F_NEW_FIELDS } else { F_FINAL_FIELDS };
        collect_node(c, wanted, out)?;
    }
}

/// What the index needs from one transaction's metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaSummary {
    /// xrpld's affected-account set, sorted ascending and deduplicated.
    pub affected_accounts: Vec<[u8; 20]>,
    /// `sfTransactionIndex`: the transaction's apply-order position within its ledger. This is
    /// `TxnSeq` in xrpld's `AccountTransactions`, and is *not* the position in the hash-sorted
    /// list a chunk stores.
    pub transaction_index: Option<u32>,
}

/// Decode one transaction's metadata in a single pass.
///
/// Metadata with no `AffectedNodes` field is an error, not an empty result: xrpld requires the
/// field (`TxMeta` throws without it), and every real transaction modifies at least its sender's
/// account for the fee. An empty or truncated blob must not be indexed as "touched nobody".
/// (An `AffectedNodes` array that is present but yields no accounts is fine — a pseudo-
/// transaction such as `SetFee` legitimately affects no account.)
pub fn summarize_meta(meta_blob: &[u8]) -> Result<MetaSummary> {
    let mut c = Cursor { b: meta_blob, i: 0 };
    let mut accounts = BTreeSet::new();
    let mut transaction_index = None;
    let mut saw_affected_nodes = false;

    // The top-level object carries no end marker; it simply ends with the buffer.
    while !c.eof() {
        let (t, f) = c.read_field_header()?;
        match (t, f) {
            (TYPE_OBJECT, 1) => break,
            (TYPE_ARRAY, F_AFFECTED_NODES) => {
                saw_affected_nodes = true;
                collect_affected_nodes(&mut c, &mut accounts)?
            }
            (TYPE_UINT32, F_TRANSACTION_INDEX) => {
                let b = c.take(4)?;
                transaction_index = Some(u32::from_be_bytes([b[0], b[1], b[2], b[3]]));
            }
            _ => skip_value(&mut c, t)?,
        }
    }

    if !saw_affected_nodes {
        bail!("metadata has no AffectedNodes (empty or malformed blob, {} bytes)", meta_blob.len());
    }

    Ok(MetaSummary { affected_accounts: accounts.into_iter().collect(), transaction_index })
}

/// The three transaction fields xrpld's `Transactions` row needs besides the blobs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxFields {
    /// `sfTransactionType` (UINT16, 2); see `tx_types::tx_type_name`.
    pub tx_type: u16,
    /// `sfAccount` (ACCOUNT, 1): the sender. A pseudo-transaction's is the zero account.
    pub account: [u8; 20],
    /// `sfSequence` (UINT32, 4): 0 for a ticketed transaction.
    pub sequence: u32,
}

/// Read `TransactionType`, `Sequence` and `Account` from a serialized transaction.
///
/// Fields are serialized in (type, field) order and `Account` is type 8, so all three precede any
/// array, object or path set and the scan stops as soon as it has them — nothing after is parsed.
pub fn parse_tx_fields(tx_blob: &[u8]) -> Result<TxFields> {
    let mut c = Cursor { b: tx_blob, i: 0 };
    let (mut tx_type, mut sequence, mut account) = (None, None, None);
    while tx_type.is_none() || sequence.is_none() || account.is_none() {
        if c.eof() {
            bail!("transaction ended before TransactionType, Sequence and Account were all found");
        }
        let (t, f) = c.read_field_header()?;
        match (t, f) {
            (TYPE_UINT16, 2) => {
                let b = c.take(2)?;
                tx_type = Some(u16::from_be_bytes([b[0], b[1]]));
            }
            (TYPE_UINT32, 4) => {
                let b = c.take(4)?;
                sequence = Some(u32::from_be_bytes([b[0], b[1], b[2], b[3]]));
            }
            (TYPE_ACCOUNT, 1) => account = Some(read_account(&mut c)?.unwrap_or([0u8; 20])),
            _ if t > TYPE_ACCOUNT => {
                bail!("reached field type {t} before finding TransactionType, Sequence and Account")
            }
            _ => skip_value(&mut c, t)?,
        }
    }
    Ok(TxFields {
        tx_type: tx_type.unwrap(),
        account: account.unwrap(),
        sequence: sequence.unwrap(),
    })
}

/// The accounts xrpld records for this transaction in `AccountTransactions`.
pub fn affected_accounts(meta_blob: &[u8]) -> Result<Vec<[u8; 20]>> {
    Ok(summarize_meta(meta_blob)?.affected_accounts)
}

/// The XRPL base58 alphabet. Index 0 (`r`) is the digit zero, so a leading `0x00` byte encodes as
/// a leading `r`.
const ALPHABET: &[u8] = b"rpshnaf39wBUDNEGHJKLM4PQRST7VWXYZ2bcdeCg65jkm8oFqi1tuvAxyz";

/// Decode a classic `r...` address into its 20-byte AccountID, verifying the version byte and
/// the 4-byte double-SHA256 checksum. The inverse of `account_id_to_classic_address`.
pub fn classic_address_to_account_id(address: &str) -> Result<[u8; 20]> {
    // Big-integer base58 decode into little-endian base-256 digits.
    let mut digits: Vec<u8> = Vec::new();
    for ch in address.bytes() {
        let value = ALPHABET
            .iter()
            .position(|&a| a == ch)
            .ok_or_else(|| anyhow!("invalid base58 character {:?} in address", ch as char))?;
        let mut carry = value as u32;
        for d in digits.iter_mut() {
            let v = (*d as u32) * 58 + carry;
            *d = (v & 0xFF) as u8;
            carry = v >> 8;
        }
        while carry > 0 {
            digits.push((carry & 0xFF) as u8);
            carry >>= 8;
        }
    }
    // Each leading `r` is a leading zero byte that the integer representation cannot hold.
    let leading_zeros = address.bytes().take_while(|&c| c == ALPHABET[0]).count();
    let mut payload = vec![0u8; leading_zeros];
    payload.extend(digits.iter().rev());

    if payload.len() != 25 {
        bail!("address decodes to {} bytes, expected 25", payload.len());
    }
    if payload[0] != 0x00 {
        bail!("not a classic account address (version byte {:#04x})", payload[0]);
    }
    let checksum = crate::serialize::sha256(&crate::serialize::sha256(&payload[..21]));
    if payload[21..25] != checksum[..4] {
        bail!("address checksum does not match (mistyped address?)");
    }
    let mut id = [0u8; 20];
    id.copy_from_slice(&payload[1..21]);
    Ok(id)
}

/// Base58check (XRPL alphabet) encode of a 20-byte AccountID into a classic `r...` address.
pub fn account_id_to_classic_address(account_id: &[u8; 20]) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;

    // ---- a tiny STObject builder, so every case below is an explicit byte string ----

    fn hdr(t: u32, f: u32) -> Vec<u8> {
        match (t < 16, f < 16) {
            (true, true) => vec![((t << 4) | f) as u8],
            (true, false) => vec![(t << 4) as u8, f as u8],
            (false, true) => vec![f as u8, t as u8],
            (false, false) => vec![0, t as u8, f as u8],
        }
    }
    fn a(n: u8) -> [u8; 20] {
        [n; 20]
    }
    fn acct(f: u32, id: [u8; 20]) -> Vec<u8> {
        let mut v = hdr(TYPE_ACCOUNT, f);
        v.push(20);
        v.extend(id);
        v
    }
    fn u8f(f: u32, x: u8) -> Vec<u8> {
        let mut v = hdr(TYPE_UINT8, f);
        v.push(x);
        v
    }
    fn u16f(f: u32, x: u16) -> Vec<u8> {
        let mut v = hdr(TYPE_UINT16, f);
        v.extend(x.to_be_bytes());
        v
    }
    fn u32f(f: u32, x: u32) -> Vec<u8> {
        let mut v = hdr(TYPE_UINT32, f);
        v.extend(x.to_be_bytes());
        v
    }
    fn amt_native(f: u32) -> Vec<u8> {
        let mut v = hdr(TYPE_AMOUNT, f);
        v.extend([0x40, 0, 0, 0, 0, 0, 0, 1]);
        v
    }
    fn amt_iou(f: u32, issuer: [u8; 20]) -> Vec<u8> {
        let mut v = hdr(TYPE_AMOUNT, f);
        v.extend([0xC0, 0, 0, 0, 0, 0, 0, 1]);
        v.extend([9u8; 20]); // currency
        v.extend(issuer);
        v
    }
    fn mpt_id(seq: u32, issuer: [u8; 20]) -> [u8; 24] {
        let mut id = [0u8; 24];
        id[..4].copy_from_slice(&seq.to_be_bytes());
        id[4..].copy_from_slice(&issuer);
        id
    }
    fn amt_mpt(f: u32, id: [u8; 24]) -> Vec<u8> {
        let mut v = hdr(TYPE_AMOUNT, f);
        v.extend([0x60, 0, 0, 0, 0, 0, 0, 5]);
        v.push(0);
        v.extend(id);
        v
    }
    fn obj(f: u32, inner: Vec<u8>) -> Vec<u8> {
        let mut v = hdr(TYPE_OBJECT, f);
        v.extend(inner);
        v.push(0xE1);
        v
    }
    fn arr(f: u32, inner: Vec<u8>) -> Vec<u8> {
        let mut v = hdr(TYPE_ARRAY, f);
        v.extend(inner);
        v.push(0xF1);
        v
    }
    fn cat(parts: &[Vec<u8>]) -> Vec<u8> {
        parts.concat()
    }

    const CREATED: u32 = 3;
    const DELETED: u32 = 4;
    const MODIFIED: u32 = 5;
    const PREVIOUS: u32 = 6;
    const FINAL: u32 = 7;
    const NEW: u32 = 8;

    /// A whole metadata blob: TransactionIndex, one array of nodes, TransactionResult.
    fn meta(nodes: Vec<u8>) -> Vec<u8> {
        cat(&[u32f(F_TRANSACTION_INDEX, 7), arr(F_AFFECTED_NODES, nodes), u8f(3, 0)])
    }

    fn accts(m: &[u8]) -> Vec<[u8; 20]> {
        affected_accounts(m).unwrap()
    }

    #[test]
    fn trustline_parties_come_only_from_limit_issuers() {
        // A RippleState has no AccountID field. Its two parties are the issuers of
        // LowLimit/HighLimit — the case the old decoder dropped.
        let fin = cat(&[amt_iou(F_LOW_LIMIT, a(1)), amt_iou(F_HIGH_LIMIT, a(2))]);
        let node = obj(MODIFIED, cat(&[u16f(1, 0x72), obj(FINAL, fin)]));
        assert_eq!(accts(&meta(node)), vec![a(1), a(2)]);
    }

    #[test]
    fn offer_taker_issuers_are_counted_and_native_amounts_are_not() {
        let fin = cat(&[amt_iou(F_TAKER_PAYS, a(3)), amt_native(F_TAKER_GETS)]);
        let node = obj(MODIFIED, obj(FINAL, fin));
        assert_eq!(accts(&meta(node)), vec![a(3)]);
    }

    #[test]
    fn other_amounts_are_not_issuer_sources() {
        // sfBalance is AMOUNT field 2: its issuer must NOT be indexed (on a RippleState it is the
        // placeholder noAccount()).
        let fin = cat(&[amt_iou(2, a(4)), acct(1, a(5))]);
        let node = obj(MODIFIED, obj(FINAL, fin));
        assert_eq!(accts(&meta(node)), vec![a(5)]);
    }

    #[test]
    fn previous_fields_are_excluded() {
        let node = obj(
            MODIFIED,
            cat(&[obj(PREVIOUS, acct(1, a(1))), obj(FINAL, acct(1, a(2)))]),
        );
        assert_eq!(accts(&meta(node)), vec![a(2)]);
    }

    #[test]
    fn created_reads_new_fields_and_modified_deleted_read_final_fields() {
        let created = obj(CREATED, obj(NEW, acct(1, a(1))));
        let modified = obj(MODIFIED, obj(FINAL, acct(1, a(2))));
        let deleted = obj(DELETED, obj(FINAL, acct(1, a(3))));
        assert_eq!(accts(&meta(cat(&[created, modified, deleted]))), vec![a(1), a(2), a(3)]);

        // The wrong container for the node kind is ignored, as xrpld does.
        let wrong = obj(CREATED, obj(FINAL, acct(1, a(9))));
        assert_eq!(accts(&meta(wrong)), Vec::<[u8; 20]>::new());
    }

    #[test]
    fn a_node_with_no_fields_container_is_skipped() {
        let node = obj(MODIFIED, u16f(1, 0x61));
        assert_eq!(accts(&meta(node)), Vec::<[u8; 20]>::new());
    }

    #[test]
    fn nested_objects_inside_final_fields_are_not_searched() {
        // e.g. an AMM AuctionSlot holds an Account one level down: xrpld does not count it.
        let fin = cat(&[acct(1, a(1)), obj(26, acct(1, a(2)))]);
        let node = obj(MODIFIED, obj(FINAL, fin));
        assert_eq!(accts(&meta(node)), vec![a(1)]);
    }

    #[test]
    fn a_20_byte_zero_account_is_kept_but_an_empty_one_is_skipped_and_odd_lengths_fail() {
        // xrpld's `isDefault()` is set only by the zero-LENGTH encoding. A 20-byte all-zero
        // AccountID is a real, non-default STAccount and xrpld indexes it, so we must too.
        let node = obj(MODIFIED, obj(FINAL, cat(&[acct(1, [0u8; 20]), acct(2, a(7))])));
        assert_eq!(accts(&meta(node)), vec![[0u8; 20], a(7)]);

        let mut empty = hdr(TYPE_ACCOUNT, 1);
        empty.push(0);
        let node = obj(MODIFIED, obj(FINAL, empty));
        assert_eq!(accts(&meta(node)), Vec::<[u8; 20]>::new());

        let mut bad = hdr(TYPE_ACCOUNT, 1);
        bad.push(5);
        bad.extend([1u8; 5]);
        let node = obj(MODIFIED, obj(FINAL, bad));
        assert!(affected_accounts(&meta(node)).is_err());
    }

    #[test]
    fn mpt_amount_is_33_bytes_and_yields_its_issuer_without_desync() {
        // The field after it must still be found, which proves the reader consumed exactly 33.
        let fin = cat(&[amt_mpt(F_TAKER_PAYS, mpt_id(1, a(5))), acct(1, a(9))]);
        let node = obj(MODIFIED, obj(FINAL, fin));
        assert_eq!(accts(&meta(node)), vec![a(5), a(9)]);
    }

    #[test]
    fn mpt_issuance_id_field_yields_its_issuer() {
        let mut f = hdr(TYPE_UINT192, F_MPT_ISSUANCE_ID);
        f.extend(mpt_id(1, a(6)));
        let node = obj(MODIFIED, obj(FINAL, cat(&[f, acct(1, a(9))])));
        assert_eq!(accts(&meta(node)), vec![a(6), a(9)]);
    }

    #[test]
    fn issue_type_xrp_iou_and_mpt_have_20_40_and_44_bytes() {
        let xrp = cat(&[hdr(TYPE_ISSUE, 3), vec![0u8; 20]]);
        let iou = cat(&[hdr(TYPE_ISSUE, 3), vec![1u8; 20], vec![2u8; 20]]);
        let mpt = cat(&[hdr(TYPE_ISSUE, 3), vec![1u8; 20], NO_ACCOUNT.to_vec(), vec![0, 0, 0, 7]]);
        for issue in [xrp, iou, mpt] {
            // The trailing account is only found if the issue consumed exactly the right length.
            let node = obj(MODIFIED, obj(FINAL, cat(&[issue, acct(1, a(9))])));
            assert_eq!(accts(&meta(node)), vec![a(9)]);
        }
    }

    #[test]
    fn number_int_and_currency_fields_are_skipped_at_the_right_width() {
        let number = cat(&[hdr(TYPE_NUMBER, 2), vec![0u8; 12]]);
        let int32 = cat(&[hdr(TYPE_INT32, 3), vec![0u8; 4]]);
        let int64 = cat(&[hdr(TYPE_INT64, 4), vec![0u8; 8]]);
        let currency = cat(&[hdr(TYPE_CURRENCY, 1), vec![0u8; 20]]);
        let node = obj(MODIFIED, obj(FINAL, cat(&[number, int32, int64, currency, acct(1, a(9))])));
        assert_eq!(accts(&meta(node)), vec![a(9)]);
    }

    #[test]
    fn unknown_type_is_an_error_not_a_guess() {
        let unknown = cat(&[hdr(25, 1), vec![0u8; 8]]); // STI_XCHAIN_BRIDGE, not handled
        let node = obj(MODIFIED, obj(FINAL, cat(&[unknown, acct(1, a(9))])));
        assert!(affected_accounts(&meta(node)).is_err());
    }

    #[test]
    fn transaction_index_is_read_from_the_top_level_only() {
        let nested = obj(MODIFIED, obj(FINAL, cat(&[u32f(F_TRANSACTION_INDEX, 999), acct(1, a(1))])));
        let s = summarize_meta(&meta(nested)).unwrap();
        assert_eq!(s.transaction_index, Some(7));
        assert_eq!(s.affected_accounts, vec![a(1)]);

        // No TransactionIndex present -> None (an empty AffectedNodes array is still valid).
        let bare = summarize_meta(&arr(F_AFFECTED_NODES, vec![])).unwrap();
        assert_eq!(bare.transaction_index, None);
        assert!(bare.affected_accounts.is_empty());
    }

    #[test]
    fn metadata_without_affected_nodes_is_an_error_not_an_empty_result() {
        // Every real transaction touches at least its sender; "touched nobody" must never be
        // produced by an empty or truncated blob.
        assert!(summarize_meta(&[]).is_err());
        assert!(summarize_meta(&u8f(3, 0)).is_err());
        assert!(summarize_meta(&u32f(F_TRANSACTION_INDEX, 1)).is_err());
        assert!(affected_accounts(&[]).is_err());
    }

    #[test]
    fn an_empty_affected_nodes_array_is_valid_for_pseudo_transactions() {
        let s = summarize_meta(&cat(&[u32f(F_TRANSACTION_INDEX, 0), arr(F_AFFECTED_NODES, vec![])]))
            .unwrap();
        assert!(s.affected_accounts.is_empty());
        assert_eq!(s.transaction_index, Some(0));
    }

    #[test]
    fn results_are_sorted_and_deduplicated() {
        let n1 = obj(MODIFIED, obj(FINAL, cat(&[acct(1, a(3)), acct(2, a(1))])));
        let n2 = obj(MODIFIED, obj(FINAL, acct(1, a(3))));
        assert_eq!(accts(&meta(cat(&[n1, n2]))), vec![a(1), a(3)]);
    }

    #[test]
    fn truncating_inside_affected_nodes_is_always_an_error() {
        // The top-level object has no end marker, so a cut exactly between top-level fields is
        // indistinguishable from a shorter object. But a cut anywhere *inside* the AffectedNodes
        // array must never decode "successfully" with fewer accounts.
        let fin = cat(&[amt_iou(F_LOW_LIMIT, a(1)), amt_iou(F_HIGH_LIMIT, a(2)), acct(1, a(3))]);
        let nodes = cat(&[
            obj(MODIFIED, cat(&[u16f(1, 0x72), obj(FINAL, fin)])),
            obj(CREATED, obj(NEW, acct(1, a(4)))),
        ]);
        let head = u32f(F_TRANSACTION_INDEX, 7);
        let array = arr(F_AFFECTED_NODES, nodes);
        let full = cat(&[head.clone(), array.clone(), u8f(3, 0)]);
        assert_eq!(accts(&full), vec![a(1), a(2), a(3), a(4)]);

        let start = head.len() + 1; // just inside the array header
        let end = head.len() + array.len(); // exclusive: the end marker must be present
        for cut in start..end {
            assert!(
                affected_accounts(&full[..cut]).is_err(),
                "a cut at byte {cut} inside AffectedNodes decoded without error"
            );
        }
    }

    #[test]
    fn address_decode_is_the_inverse_of_encode_and_checks_the_checksum() {
        for id in [[0u8; 20], a(1), a(0xFF), {
            let mut x = [0u8; 20];
            x[19] = 1;
            x
        }] {
            let addr = account_id_to_classic_address(&id);
            assert_eq!(classic_address_to_account_id(&addr).unwrap(), id, "round trip of {addr}");
        }
        assert_eq!(
            classic_address_to_account_id("rHb9CJAWyB4rj91VRWn96DkukG4bwdtyTh").unwrap(),
            [
                0xB5, 0xF7, 0x62, 0x79, 0x8A, 0x53, 0xD5, 0x43, 0xA0, 0x14, 0xCA, 0xF8, 0xB2, 0x97,
                0xCF, 0xF8, 0xF2, 0xF9, 0x37, 0xE8
            ]
        );

        // A single wrong character must be rejected by the checksum, never decode to some
        // other account.
        assert!(classic_address_to_account_id("rHb9CJAWyB4rj91VRWn96DkukG4bwdtyTi").is_err());
        assert!(classic_address_to_account_id("rHb9CJAWyB4rj91VRWn96Dkuk0G4bwdtyTh").is_err());
        assert!(classic_address_to_account_id("").is_err());
        assert!(classic_address_to_account_id("r").is_err());
    }

    #[test]
    fn tx_fields_are_read_in_front_of_everything_that_cannot_be_skipped() {
        // Real field order: TransactionType, Flags, Sequence, ..., SigningPubKey (VL), Account,
        // Destination — then a Memos array and a Paths path set that must never be touched.
        let blob = cat(&[
            u16f(2, 0),                              // TransactionType = Payment
            u32f(2, 0x8000_0000),                    // Flags
            u32f(4, 77),                             // Sequence
            amt_native(1),                           // Amount
            {
                let mut v = hdr(TYPE_VL, 3); // SigningPubKey
                v.push(2);
                v.extend([0xAA, 0xBB]);
                v
            },
            acct(1, a(5)),                           // Account
            acct(3, a(6)),                           // Destination
            hdr(18, 1),                              // a PATHSET after Account: unparseable here
        ]);
        let f = parse_tx_fields(&blob).unwrap();
        assert_eq!(f, TxFields { tx_type: 0, account: a(5), sequence: 77 });

        // A ticketed transaction has Sequence 0, which is a value, not "missing".
        let ticketed = cat(&[u16f(2, 7), u32f(4, 0), acct(1, a(2))]);
        assert_eq!(parse_tx_fields(&ticketed).unwrap().sequence, 0);

        // Anything missing is an error, never a default.
        assert!(parse_tx_fields(&cat(&[u16f(2, 0), acct(1, a(5))])).is_err());
        assert!(parse_tx_fields(&[]).is_err());
        assert!(parse_tx_fields(&cat(&[u16f(2, 0), u32f(4, 1), hdr(14, 3)])).is_err());
    }

    #[test]
    fn classic_address_known_vectors() {
        assert_eq!(account_id_to_classic_address(&[0u8; 20]), "rrrrrrrrrrrrrrrrrrrrrhoLvTp");
        let mut one = [0u8; 20];
        one[19] = 1;
        assert_eq!(account_id_to_classic_address(&one), "rrrrrrrrrrrrrrrrrrrrBZbvji");
        let genesis: [u8; 20] = [
            0xB5, 0xF7, 0x62, 0x79, 0x8A, 0x53, 0xD5, 0x43, 0xA0, 0x14, 0xCA, 0xF8, 0xB2, 0x97,
            0xCF, 0xF8, 0xF2, 0xF9, 0x37, 0xE8,
        ];
        assert_eq!(
            account_id_to_classic_address(&genesis),
            "rHb9CJAWyB4rj91VRWn96DkukG4bwdtyTh"
        );
    }
}
