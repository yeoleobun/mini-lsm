use super::Block;
use crate::key::{KeySlice, KeyVec};
use bytes::BufMut;

pub fn common_prefix(first_key: &[u8], current_key: &[u8]) -> usize {
    let mut i = 0;
    while i < std::cmp::min(first_key.len(), current_key.len()) && first_key[i] == current_key[i] {
        i += 1;
    }
    i
}

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::with_capacity(block_size),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let n = self.data.len() + self.offsets.len() * 2 + key.len() + value.len() + 8;
        if !self.is_empty() && n > self.block_size {
            return false;
        }

        self.offsets.push(self.data.len() as u16);
        if self.first_key.is_empty() {
            self.first_key.set_from_slice(key);
            self.data.put_u16(key.len() as u16);
            self.data.extend_from_slice(key.into_inner());
        } else {
            let overlap_len =
                common_prefix(self.first_key.as_key_slice().into_inner(), key.into_inner());
            let rest_len = key.len() - overlap_len;
            self.data.put_u16(overlap_len as u16);
            self.data.put_u16(rest_len as u16);
            self.data
                .extend_from_slice(&key.into_inner()[overlap_len..]);
        }
        self.data.put_u16(value.len() as u16);
        self.data.put_slice(value);
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }
}
