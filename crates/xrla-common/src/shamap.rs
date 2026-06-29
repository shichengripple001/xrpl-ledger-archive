use thiserror::Error;

pub type Hash256 = [u8; 32];

pub const ZERO_HASH: Hash256 = [0u8; 32];

/// Wire type byte — stored as the LAST byte of every serialized node.
/// Source: include/xrpl/shamap/SHAMapTreeNode.h
pub mod wire_type {
    pub const TRANSACTION:          u8 = 0;
    pub const ACCOUNT_STATE:        u8 = 1;
    pub const INNER:                u8 = 2;
    pub const COMPRESSED_INNER:     u8 = 3;
    pub const TRANSACTION_WITH_META:u8 = 4;
}

#[derive(Debug, Clone, PartialEq)]
pub enum NodeType {
    Transaction,
    AccountState,
    Inner,
    CompressedInner,
    TransactionWithMeta,
}

impl NodeType {
    pub fn is_inner(&self) -> bool {
        matches!(self, NodeType::Inner | NodeType::CompressedInner)
    }

    pub fn is_leaf(&self) -> bool {
        !self.is_inner()
    }
}

impl TryFrom<u8> for NodeType {
    type Error = SHAMapError;
    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            wire_type::TRANSACTION           => Ok(NodeType::Transaction),
            wire_type::ACCOUNT_STATE         => Ok(NodeType::AccountState),
            wire_type::INNER                 => Ok(NodeType::Inner),
            wire_type::COMPRESSED_INNER      => Ok(NodeType::CompressedInner),
            wire_type::TRANSACTION_WITH_META => Ok(NodeType::TransactionWithMeta),
            _                                => Err(SHAMapError::UnknownNodeType(v)),
        }
    }
}

impl From<&NodeType> for u8 {
    fn from(t: &NodeType) -> u8 {
        match t {
            NodeType::Transaction           => wire_type::TRANSACTION,
            NodeType::AccountState          => wire_type::ACCOUNT_STATE,
            NodeType::Inner                 => wire_type::INNER,
            NodeType::CompressedInner       => wire_type::COMPRESSED_INNER,
            NodeType::TransactionWithMeta   => wire_type::TRANSACTION_WITH_META,
        }
    }
}

/// A single SHAMap node as stored in a chunk file.
/// `content` includes all bytes EXCEPT the trailing type byte.
#[derive(Debug, Clone)]
pub struct SHAMapNode {
    pub hash:      Hash256,
    pub node_type: NodeType,
    /// Raw content bytes, NOT including the trailing wire type byte.
    pub content:   Vec<u8>,
}

impl SHAMapNode {
    /// Reconstruct the original wire bytes (content + type byte at end).
    pub fn to_wire_bytes(&self) -> Vec<u8> {
        let mut v = self.content.clone();
        v.push(u8::from(&self.node_type));
        v
    }

    /// Parse a node from wire bytes (type byte is the last byte).
    pub fn from_wire_bytes(hash: Hash256, wire: &[u8]) -> Result<Self, SHAMapError> {
        if wire.is_empty() {
            return Err(SHAMapError::InvalidEncoding("empty wire bytes".into()));
        }
        let type_byte = wire[wire.len() - 1];
        let node_type = NodeType::try_from(type_byte)?;
        let content   = wire[..wire.len() - 1].to_vec();
        Ok(SHAMapNode { hash, node_type, content })
    }
}

/// Parsed inner node: up to 16 child hashes indexed by nibble 0..F.
#[derive(Debug, Clone)]
pub struct InnerNode {
    pub children: [Option<Hash256>; 16],
}

impl InnerNode {
    /// Parse a full inner node.
    ///
    /// Format: 16 × 32 bytes, back to back, in slot order 0..15.
    /// Zero hash means slot is empty.
    /// Source: SHAMapInnerNode::makeFullInner()
    pub fn from_full_bytes(content: &[u8]) -> Result<Self, SHAMapError> {
        if content.len() != 16 * 32 {
            return Err(SHAMapError::InvalidEncoding(
                format!("full inner: expected {} bytes, got {}", 16 * 32, content.len())
            ));
        }
        let mut children = [None; 16];
        for i in 0..16 {
            let offset = i * 32;
            let mut hash = [0u8; 32];
            hash.copy_from_slice(&content[offset..offset + 32]);
            if hash != ZERO_HASH {
                children[i] = Some(hash);
            }
        }
        Ok(InnerNode { children })
    }

    /// Parse a compressed inner node.
    ///
    /// Format: N × (32-byte hash + 1-byte branch position), where N < 12.
    /// Used when the node has fewer than 12 children.
    /// Source: SHAMapInnerNode::makeCompressedInner()
    pub fn from_compressed_bytes(content: &[u8]) -> Result<Self, SHAMapError> {
        const CHUNK: usize = 33; // 32 bytes hash + 1 byte position
        if content.len() % CHUNK != 0 || content.len() > CHUNK * 16 {
            return Err(SHAMapError::InvalidEncoding(
                format!("compressed inner: invalid length {}", content.len())
            ));
        }
        let mut children = [None; 16];
        let mut offset = 0;
        while offset < content.len() {
            let mut hash = [0u8; 32];
            hash.copy_from_slice(&content[offset..offset + 32]);
            let pos = content[offset + 32] as usize;
            if pos >= 16 {
                return Err(SHAMapError::InvalidEncoding(
                    format!("compressed inner: invalid position {pos}")
                ));
            }
            if hash != ZERO_HASH {
                children[pos] = Some(hash);
            }
            offset += CHUNK;
        }
        Ok(InnerNode { children })
    }

    /// Parse an inner node from a SHAMapNode (handles both full and compressed).
    pub fn from_node(node: &SHAMapNode) -> Result<Self, SHAMapError> {
        match node.node_type {
            NodeType::Inner           => Self::from_full_bytes(&node.content),
            NodeType::CompressedInner => Self::from_compressed_bytes(&node.content),
            _ => Err(SHAMapError::InvalidEncoding("not an inner node".into())),
        }
    }

    pub fn child_hashes(&self) -> impl Iterator<Item = &Hash256> {
        self.children.iter().filter_map(|c| c.as_ref())
    }
}

/// Delta between two consecutive ledger SHAMap states.
#[derive(Debug, Default)]
pub struct SHAMapDiff {
    /// Nodes present in new state but not old (added or modified).
    /// Sorted ascending by hash.
    pub added: Vec<SHAMapNode>,
    /// Hashes present in old state but not new (deleted).
    /// Sorted ascending by hash.
    pub deleted: Vec<Hash256>,
}

#[derive(Debug, Error)]
pub enum SHAMapError {
    #[error("unknown wire type: {0}")]
    UnknownNodeType(u8),
    #[error("invalid encoding: {0}")]
    InvalidEncoding(String),
    #[error("node not found in store: {0}")]
    NodeNotFound(String),
}
