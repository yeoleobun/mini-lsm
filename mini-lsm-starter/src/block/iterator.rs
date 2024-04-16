#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use crate::key::{KeySlice, KeyVec};

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
        if !block.data.is_empty() {
            let key_size = (block.data[0] as usize) << 8 | block.data[1] as usize;
            first_key.set_from_slice(KeySlice::from_slice(&block.data[2..key_size + 2]));
        }
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut res = Self::new(block);
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
        if !self.block.data.is_empty() {
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
            let common_len = u16::from_be_bytes(
                self.block.data[key_offset..key_offset + 2]
                    .try_into()
                    .unwrap(),
            ) as usize;
            let rest_len = u16::from_be_bytes(
                self.block.data[key_offset + 2..key_offset + 4]
                    .try_into()
                    .unwrap(),
            ) as usize;
            let value_offset = key_offset + 4 + rest_len;
            let key_slice = [
                &self.first_key.as_key_slice().into_inner()[..common_len],
                &self.block.data[key_offset + 4..value_offset],
            ]
            .concat();
            self.key.set_from_slice(KeySlice::from_slice(&key_slice));
            let value_length = u16::from_be_bytes(
                self.block.data[value_offset..value_offset + 2]
                    .try_into()
                    .unwrap(),
            ) as usize;
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
        let (mut i, mut j) = (0, n);
        while i < j {
            let m = (i + j) / 2;
            let cur = if m == 0 {
                self.first_key.clone()
            } else {
                let offset = self.block.offsets[m] as usize;
                let common_len =
                    u16::from_be_bytes(self.block.data[offset..offset + 2].try_into().unwrap())
                        as usize;
                let rest_len =
                    u16::from_be_bytes(self.block.data[offset + 2..offset + 4].try_into().unwrap())
                        as usize;
                let v = [
                    &self.first_key.as_key_slice().into_inner()[..common_len],
                    &self.block.data[offset + 4..offset + 4 + rest_len],
                ]
                .concat();
                KeyVec::from_vec(v)
            };
            if cur.as_key_slice() >= key {
                j = m;
            } else {
                i = m + 1;
            }
        }

        if i < n {
            self.idx = i;
            if i == 0 {
                self.key.set_from_slice(self.first_key.as_key_slice());
                let value_offset = 2 + self.key.len();
                let value_length = u16::from_be_bytes(
                    self.block.data[value_offset..value_offset + 2]
                        .try_into()
                        .unwrap(),
                ) as usize;
                self.value_range = (value_offset + 2, value_offset + 2 + value_length);
            } else {
                let offset = self.block.offsets[i] as usize;
                let common_len =
                    u16::from_be_bytes(self.block.data[offset..offset + 2].try_into().unwrap())
                        as usize;
                let rest_len =
                    u16::from_be_bytes(self.block.data[offset + 2..offset + 4].try_into().unwrap())
                        as usize;
                let value_offset = offset + 4 + rest_len;
                let value_length = u16::from_be_bytes(
                    self.block.data[value_offset..value_offset + 2]
                        .try_into()
                        .unwrap(),
                ) as usize;
                self.value_range = (value_offset + 2, value_offset + 2 + value_length);
                let v = [
                    &self.first_key.as_key_slice().into_inner()[..common_len],
                    &self.block.data[offset + 4..value_offset],
                ]
                .concat();
                self.key.set_from_slice(KeySlice::from_slice(&v));
            }
        } else {
            self.idx = n;
            self.key.set_from_slice(KeySlice::from_slice(&[]));
            self.value_range = (0, 0);
        }
    }
}
