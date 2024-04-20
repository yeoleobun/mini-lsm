use std::path::Path;
use std::sync::Arc;

use anyhow::{Ok, Result};
use bytes::{BufMut, Bytes};

use super::{bloom::Bloom, BlockMeta, SsTable};
use crate::{
    block::BlockBuilder,
    key::{KeyBytes, KeySlice},
    lsm_storage::BlockCache,
    table::FileObject,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    key_hashes: Vec<u32>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            key_hashes: Vec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
        }
    }

    fn flush(&mut self) {
        self.meta.push(BlockMeta {
            first_key: KeyBytes::from_bytes(Bytes::copy_from_slice(&self.first_key)),
            last_key: KeyBytes::from_bytes(Bytes::copy_from_slice(&self.last_key)),
            offset: self.data.len(),
        });

        let old = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        self.data.put_slice(&old.build().encode());
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if !self.builder.add(key, value) {
            self.flush();
            let _ = self.builder.add(key, value);
            self.first_key.clear();
        }

        if self.first_key.is_empty() {
            self.first_key.extend_from_slice(key.into_inner());
        }

        self.last_key.clear();
        self.last_key.extend_from_slice(key.into_inner());
        self.key_hashes
            .push(farmhash::fingerprint32(key.into_inner()));
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len() + self.builder.size()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        if !self.builder.is_empty() {
            self.flush()
        }

        let block_meta_offset = self.data.len();
        BlockMeta::encode_block_meta(&self.meta, &mut self.data);
        self.data.put_u32(block_meta_offset as u32);

        let bits = Bloom::bloom_bits_per_key(self.key_hashes.len(), 0.01);
        let bloom = Bloom::build_from_key_hashes(&self.key_hashes, bits);
        let bloom_offset = self.data.len();
        bloom.encode(&mut self.data);
        self.data.put_u32(bloom_offset as u32);

        let file = FileObject::create(path.as_ref(), self.data)?;
        let first_key = self.meta[0].first_key.clone();
        let last_key = self.meta[self.meta.len() - 1].last_key.clone();
        Ok(SsTable {
            file,
            first_key,
            last_key,
            block_meta: self.meta,
            block_meta_offset,
            id,
            block_cache,
            bloom: Some(bloom),
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
