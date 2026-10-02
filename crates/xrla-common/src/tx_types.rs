//! Transaction type code -> name, as xrpld stores it in `Transactions.TransType`.
//!
//! GENERATED from rippled's `include/xrpl/protocol/detail/transactions.macro` (commit 5a5ad8673):
//! the third argument of each `TRANSACTION(tag, code, Name, ...)` is the string `TxFormats` uses
//! (`format->getName()` in `STTx::getMetaSQL`). Do not edit by hand; regenerate from the macro.
//!
//! An unknown code returns `None`. Callers must treat that as an error, not guess a name:
//! historic transaction types that no longer appear in the macro would otherwise be written
//! under a made-up name.

pub fn tx_type_name(code: u16) -> Option<&'static str> {
    Some(match code {
        0 => "Payment",
        1 => "EscrowCreate",
        2 => "EscrowFinish",
        3 => "AccountSet",
        4 => "EscrowCancel",
        5 => "SetRegularKey",
        7 => "OfferCreate",
        8 => "OfferCancel",
        10 => "TicketCreate",
        12 => "SignerListSet",
        13 => "PaymentChannelCreate",
        14 => "PaymentChannelFund",
        15 => "PaymentChannelClaim",
        16 => "CheckCreate",
        17 => "CheckCash",
        18 => "CheckCancel",
        19 => "DepositPreauth",
        20 => "TrustSet",
        21 => "AccountDelete",
        25 => "NFTokenMint",
        26 => "NFTokenBurn",
        27 => "NFTokenCreateOffer",
        28 => "NFTokenCancelOffer",
        29 => "NFTokenAcceptOffer",
        30 => "Clawback",
        31 => "AMMClawback",
        35 => "AMMCreate",
        36 => "AMMDeposit",
        37 => "AMMWithdraw",
        38 => "AMMVote",
        39 => "AMMBid",
        40 => "AMMDelete",
        41 => "XChainCreateClaimID",
        42 => "XChainCommit",
        43 => "XChainClaim",
        44 => "XChainAccountCreateCommit",
        45 => "XChainAddClaimAttestation",
        46 => "XChainAddAccountCreateAttestation",
        47 => "XChainModifyBridge",
        48 => "XChainCreateBridge",
        49 => "DIDSet",
        50 => "DIDDelete",
        51 => "OracleSet",
        52 => "OracleDelete",
        53 => "LedgerStateFix",
        54 => "MPTokenIssuanceCreate",
        55 => "MPTokenIssuanceDestroy",
        56 => "MPTokenIssuanceSet",
        57 => "MPTokenAuthorize",
        58 => "CredentialCreate",
        59 => "CredentialAccept",
        60 => "CredentialDelete",
        61 => "NFTokenModify",
        62 => "PermissionedDomainSet",
        63 => "PermissionedDomainDelete",
        64 => "DelegateSet",
        65 => "VaultCreate",
        66 => "VaultSet",
        67 => "VaultDelete",
        68 => "VaultDeposit",
        69 => "VaultWithdraw",
        70 => "VaultClawback",
        71 => "Batch",
        74 => "LoanBrokerSet",
        75 => "LoanBrokerDelete",
        76 => "LoanBrokerCoverDeposit",
        77 => "LoanBrokerCoverWithdraw",
        78 => "LoanBrokerCoverClawback",
        80 => "LoanSet",
        81 => "LoanDelete",
        82 => "LoanManage",
        84 => "LoanPay",
        85 => "ConfidentialMPTConvert",
        86 => "ConfidentialMPTMergeInbox",
        87 => "ConfidentialMPTConvertBack",
        88 => "ConfidentialMPTSend",
        89 => "ConfidentialMPTClawback",
        90 => "SponsorshipTransfer",
        91 => "SponsorshipSet",
        92 => "ConfidentialMPTMirrorUpdate",
        100 => "EnableAmendment",
        101 => "SetFee",
        102 => "UNLModify",
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_match_what_xrpld_stored_in_a_real_transaction_db() {
        // The type names seen in xrpld's own transaction.db for 733,218 real mainnet transactions.
        for (code, name) in [(0, "Payment"), (3, "AccountSet"), (7, "OfferCreate"), (8, "OfferCancel"), (20, "TrustSet")] {
            assert_eq!(tx_type_name(code), Some(name));
        }
        assert_eq!(tx_type_name(100), Some("EnableAmendment"));
        assert_eq!(tx_type_name(101), Some("SetFee"));
        assert_eq!(tx_type_name(102), Some("UNLModify"));
        assert_eq!(tx_type_name(60000), None);
    }
}
