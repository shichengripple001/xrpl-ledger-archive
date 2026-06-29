use thiserror::Error;

pub type Hash256 = [u8; 32];

#[derive(Debug, Clone, PartialEq)]
pub enum NodeType {
    Inner = 0,
    Leaf  = 1,
}

impl TryFrom<u8> for NodeType {
    type Error = SHAMapError;
    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            0 => Ok(NodeType::Inner),
            1 => Ok(NodeType::Leaf),
            _ => Err(SHAMapError::UnknownNodeType(v)),
        }
    }
}

/// A single SHAMap node as stored in a chunk file.
#[derive(Debug, Clone)]
pub struct SHAMapNode {
    pub hash:      Hash256,
    pub node_type: NodeType,
    pub content:   Vec<u8>,
}

/// Parsed inner node: up to 16 child hashes indexed by nibble 0..F.
#[derive(Debug, Clone)]
pub struct InnerNode {
    pub children: [Option<Hash256>; 16],
}

impl InnerNode {
    /// Parse an inner node from raw NuDB content bytes.
    ///
    /// rippled serializes inner nodes as a 2-byte bitmask followed by
    /// the hashes of present children in nibble order.
    pub fn from_bytes(content: &[u8]) -> Result<Self, SHAMapError> {
        if content.len() < 2 {
            return Err(SHAMapError::InvalidInnerNode);
        }
        let bitmask = u16::from_be_bytes([content[0], content[1]]);
        let mut children = [None; 16];
        let mut offset = 2usize;

        for i in 0..16 {
            if bitmask & (1 << i) != 0 {
                if offset + 32 > content.len() {
                    return Err(SHAMapError::InvalidInnerNode);
                }
                let mut hash = [0u8; 32];
                hash.copy_from_slice(&content[offset..offset + 32]);
                children[i] = Some(hash);
                offset += 32;
            }
        }
        Ok(InnerNode { children })
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
    #[error("unknown node type: {0}")]
    UnknownNodeType(u8),
    #[error("invalid inner node encoding")]
    InvalidInnerNode,
    #[error("node not found in store: {0}")]
    NodeNotFound(String),
}
