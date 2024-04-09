#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{ops::Bound, sync::Arc};

use anyhow::{Ok, Result};

use super::SsTable;
use crate::{
    block::{Block, BlockIterator},
    iterators::StorageIterator,
    key::KeySlice,
};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
    end_bound: Bound<Vec<u8>>,
}

impl SsTableIterator {
    pub fn create_with_bound(
        table: Arc<SsTable>,
        lower_bound: Bound<&[u8]>,
        upper_bound: Bound<&[u8]>,
    ) -> Result<Self> {
        let mut iter = match lower_bound {
            Bound::Included(bound) => {
                SsTableIterator::create_and_seek_to_key(table, KeySlice::from_slice(bound))?
            }
            Bound::Excluded(bound) => {
                let mut iter =
                    SsTableIterator::create_and_seek_to_key(table, KeySlice::from_slice(bound))?;
                let key = KeySlice::from_slice(bound);
                while iter.is_valid() && iter.key() == key {
                    iter.next()?
                }
                iter
            }
            Bound::Unbounded => SsTableIterator::create_and_seek_to_first(table)?,
        };
        iter.end_bound = upper_bound.map(|v| Vec::from(v));
        Ok(iter)
    }
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let block = if table.block_meta.is_empty() {
            Arc::new(Block::empty())
        } else {
            table.read_block_cached(0)?
        };
        let iter = BlockIterator::create_and_seek_to_first(block);
        Ok(Self {
            table,
            blk_iter: iter,
            blk_idx: 0,
            end_bound: Bound::Unbounded,
        })
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        if self.blk_idx == 0 {
            self.blk_iter.seek_to_first();
        } else {
            self.blk_iter =
                BlockIterator::create_and_seek_to_first(self.table.read_block_cached(0)?);
            self.blk_idx = 0;
        }
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let i = table.find_block_idx(key);
        let block = table.read_block_cached(i)?;
        let iter = BlockIterator::create_and_seek_to_key(block, key);
        Ok(Self {
            table,
            blk_iter: iter,
            blk_idx: i,
            end_bound: Bound::Unbounded,
        })
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let mut index = self.table.find_block_idx(key);
        if self.table.block_meta[index].last_key.as_key_slice() < key
            && index + 1 < self.table.block_meta.len()
        {
            index += 1;
        }
        self.blk_idx = index;
        let block = self.table.read_block_cached(index)?;
        self.blk_iter = BlockIterator::create_and_seek_to_key(block, key);
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        if !self.blk_iter.is_valid() {
            return false;
        }
        match self.end_bound.as_ref().map(|v| KeySlice::from_slice(&v)) {
            Bound::Included(bound) => self.key() <= bound,
            Bound::Excluded(bound) => self.key() < bound,
            Bound::Unbounded => true,
        }
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();
        if !self.blk_iter.is_valid() && self.blk_idx < self.table.block_meta.len() - 1 {
            self.blk_idx += 1;
            self.blk_iter = BlockIterator::create_and_seek_to_first(
                self.table.read_block_cached(self.blk_idx)?,
            );
        }
        Ok(())
    }
}
