#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use crate::key::{self, Key, KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        let mut first_key = KeyVec::new();
        if block.data.len() > 0 {
            let key_size = (block.data[0] as usize) << 8 | block.data[1] as usize;
            first_key.set_from_slice(KeySlice::from_slice(&block.data[2..key_size + 2]));
        }
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: first_key,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = BlockIterator::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut res = BlockIterator::new(block);
        res.seek_to_key(key);
        res
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key().is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.idx = 0;
        self.key = self.first_key.clone();
        if self.block.data.len() > 0 {
            let key_size = (self.block.data[0] as usize) << 8 | self.block.data[1] as usize;
            let value_size = (self.block.data[key_size + 2] as usize) << 8
                | self.block.data[key_size + 3] as usize;
            self.value_range = (key_size + 4, key_size + 4 + value_size);
        } else {
            self.value_range = (0, 0);
        }
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx += 1;
        if self.idx < self.block.offsets.len() {
            let key_offset = self.block.offsets[self.idx] as usize;
            let key_length = (self.block.data[key_offset] as usize) << 8
                | self.block.data[key_offset + 1] as usize;
            self.key.set_from_slice(KeySlice::from_slice(
                &self.block.data[key_offset + 2..key_offset + 2 + key_length],
            ));
            let value_offset = key_offset + 2 + key_length;
            let value_length = (self.block.data[value_offset] as usize) << 8
                | self.block.data[value_offset + 1] as usize;
            self.value_range = (value_offset + 2, value_offset + 2 + value_length);
        } else {
            self.key = KeyVec::new();
            self.value_range = (0, 0);
        }
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let n = self.block.offsets.len();
        let (mut i, mut j) = (0, n - 1);
        let mut cur = KeySlice::from_slice(&[]);
        let mut offset = 0;
        let mut length = 0;
        while i < j {
            let m = (i + j) / 2;
            offset = self.block.offsets[m] as usize;
            length = (self.block.data[offset] as usize) << 8 | self.block.data[offset + 1] as usize;
            cur = KeySlice::from_slice(&self.block.data[offset + 2..offset + 2 + length]);
            if cur >= key {
                j = m;
            } else {
                i = m + 1;
            }
        }

        if i < n {
            offset = self.block.offsets[i] as usize;
            length = (self.block.data[offset] as usize) << 8 | self.block.data[offset + 1] as usize;
            cur = KeySlice::from_slice(&self.block.data[offset + 2..offset + 2 + length]);
        }

        if cur.len() > 0 && cur >= key {
            self.idx = i;
            self.key.set_from_slice(cur);
            let value_offset = offset + 2 + length;
            let value_length = (self.block.data[value_offset] as usize) << 8
                | self.block.data[value_offset + 1] as usize;
            self.value_range = (value_offset + 2, value_offset + 2 + value_length);
        } else {
            self.idx = n;
            self.key.set_from_slice(KeySlice::from_slice(&[]));
            self.value_range = (0, 0);
        }
    }
}
