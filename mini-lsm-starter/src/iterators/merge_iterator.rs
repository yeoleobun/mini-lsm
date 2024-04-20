use std::cmp::{self};
use std::collections::BinaryHeap;

use anyhow::{Ok, Result};

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    #[allow(clippy::non_canonical_partial_ord_impl)]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.1.key().cmp(&other.1.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
        }
        .map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut heap = BinaryHeap::with_capacity(iters.len());
        for (i, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                heap.push(HeapWrapper(i, iter));
            }
        }

        let current = heap.pop();

        Self {
            iters: heap,
            current,
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn next(&mut self) -> Result<()> {
        if self.current.is_none() {
            return Ok(());
        }
        let mut cur = self.current.take().unwrap();
        let v = cur.1.key().to_key_vec();
        let pre_key = v.as_key_slice();
        cur.1.next()?;
        if cur.1.is_valid() {
            self.iters.push(cur);
        }

        while let Some(mut iter) = self.iters.pop() {
            if pre_key < iter.1.key() {
                self.current = Some(iter);
                break;
            }
            iter.1.next()?;
            if iter.1.is_valid() {
                self.iters.push(iter);
            }
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        let count = self
            .iters
            .iter()
            .map(|i| i.1.num_active_iterators())
            .sum::<usize>();

        if let Some(a) = self.current.as_ref() {
            if a.1.is_valid() {
                return count + 1;
            }
        }
        count
    }
}
