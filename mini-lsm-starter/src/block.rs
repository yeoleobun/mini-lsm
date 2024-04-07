#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

use core::slice;
use std::env::consts;

pub use builder::BlockBuilder;
use bytes::{BufMut, Bytes, BytesMut};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(self.data.len() + self.offsets.len() * 2 + 2);
        bytes.put_slice(&self.data);
        let p = self.offsets.as_ref() as *const [u16] as *const u8;
        let s = unsafe { slice::from_raw_parts(p, self.offsets.len() * 2) };
        bytes.put_slice(s);
        bytes.put_u16(self.offsets.len() as u16);
        bytes.freeze()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let n = (data[data.len() - 2] as usize) << 8 | data[data.len() - 1] as usize;
        let m = data.len() - n * 2 - 2;
        let kv = Vec::from(&data[..m]);
        let ptr = &data[m..data.len() - 2] as *const [u8] as *mut u16;
        let offsets = unsafe { Vec::from(slice::from_raw_parts(ptr, n)) };
        Self { data: kv, offsets }
    }
}
