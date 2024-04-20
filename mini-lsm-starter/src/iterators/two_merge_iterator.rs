use super::StorageIterator;
use anyhow::{Ok, Result};

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    // Add fields as need
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        Ok(TwoMergeIterator { a, b })
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        if !self.b.is_valid() || self.a.is_valid() && self.a.key() <= self.b.key() {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        if !self.b.is_valid() || self.a.is_valid() && self.a.key() <= self.b.key() {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn is_valid(&self) -> bool {
        self.a.is_valid() || self.b.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        if !self.b.is_valid() || self.a.is_valid() && self.a.key() <= self.b.key() {
            while self.b.is_valid() && self.b.key() == self.a.key() {
                self.b.next()?;
            }
            self.a.next()
        } else {
            while self.a.is_valid() && self.a.key() == self.b.key() {
                self.a.next()?;
            }
            self.b.next()
        }
    }

    fn num_active_iterators(&self) -> usize {
        self.a.num_active_iterators() + self.b.num_active_iterators()
    }
}
