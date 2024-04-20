use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};
use anyhow::Result;
use std::sync::Arc;

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        let current = if !sstables.is_empty() {
            let head = sstables[0].clone();
            Some(SsTableIterator::create_and_seek_to_first(head)?)
        } else {
            None
        };

        Ok(Self {
            current,
            next_sst_idx: 1,
            sstables,
        })
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        let mut i = 0;
        let mut j = sstables.len();
        while i < j {
            let mid = (i + j) / 2;
            if key > sstables[mid].last_key().as_key_slice() {
                i = mid + 1
            } else {
                j = mid
            }
        }

        let current = if i < sstables.len() {
            Some(SsTableIterator::create_and_seek_to_key(
                sstables[i].clone(),
                key,
            )?)
        } else {
            None
        };
        Ok(Self {
            current,
            next_sst_idx: std::cmp::min(i + 1, sstables.len()),
            sstables,
        })
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        let cur = self.current.as_ref().unwrap();
        cur.key()
    }

    fn value(&self) -> &[u8] {
        let cur = self.current.as_ref().unwrap();
        cur.value()
    }

    fn is_valid(&self) -> bool {
        if let Some(cur) = &self.current {
            cur.is_valid()
        } else {
            false
        }
    }

    fn next(&mut self) -> Result<()> {
        if let Some(cur) = &mut self.current {
            cur.next()?;
            if !cur.is_valid() {
                if self.next_sst_idx < self.sstables.len() {
                    let next = self.sstables[self.next_sst_idx].clone();
                    self.current = Some(SsTableIterator::create_and_seek_to_first(next)?);
                    self.next_sst_idx += 1;
                } else {
                    self.current = None;
                }
            }
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
