use std::fs::File;
use std::os::unix::fs::FileExt;
use zerocopy::{AsBytes, FromBytes, FromZeroes};

use crate::{decode_fixed32, decode_varint32, decode_varint64, Result, Status};

pub struct Block<'a> {
    data: &'a [u8],
    restart_offset: usize,
}

impl<'a> Block<'a> {
    pub fn new(data: &'a [u8]) -> Option<Block<'a>> {
        let max_restarts_allowed = (data.len() - 4) / 4;
        if get_num_restarts(data) as usize > max_restarts_allowed {
            return None;
        }
        let restart_offset = data.len() - (1 + get_num_restarts(data)) as usize * 4;
        Some(Block {
            data,
            restart_offset,
        })
    }

    pub fn iter(self) -> impl Iterator<Item = (Vec<u8>, &'a [u8])> {
        BlockIterator::new(
            self.data,
            self.restart_offset as u32,
            get_num_restarts(self.data),
        )
        .unwrap()
    }
}

fn get_num_restarts(data: &[u8]) -> u32 {
    let size = data.len();
    assert!(size >= 4);

    decode_fixed32(&data[size - 4..])
}

// decode entry returns offset of key, shared, non_shared, value
fn decode_entry(data: &[u8]) -> Option<(usize, u32, u32, u32)> {
    if data.len() < 3 {
        return None;
    }
    let mut key_offset = 0;

    let mut shared = data[0] as u32;
    let mut non_shared = data[1] as u32;
    let mut value_length = data[2] as u32;
    if (shared | non_shared | value_length) < 0x80 {
        // fast path: all three values are encoded in one byte each
        key_offset = 3;
    } else {
        let mut bytes: usize;
        (shared, bytes) = decode_varint32(&data[key_offset..])?;
        key_offset += bytes;
        (non_shared, bytes) = decode_varint32(&data[key_offset..])?;
        key_offset += bytes;
        (value_length, bytes) = decode_varint32(&data[key_offset..])?;
        key_offset += bytes;
    }

    if data.len() - key_offset < non_shared as usize + value_length as usize {
        return None;
    }
    Some((key_offset, shared, non_shared, value_length))
}

pub struct BlockIterator<'a> {
    block: &'a [u8],
    restart_offset: usize,
    num_restarts: usize,

    current: usize,
    current_restart_index: usize,

    key: Vec<u8>,

    // value_offset is offset from the current offset
    value_offset: usize,
    value_size: usize,
}

impl<'a> BlockIterator<'a> {
    pub fn new(
        data: &'a [u8],
        restart_offset: u32,
        num_restarts: u32,
    ) -> Option<BlockIterator<'a>> {
        Some(BlockIterator {
            block: data,
            restart_offset: restart_offset as usize,
            num_restarts: num_restarts as usize,

            current: 0,
            current_restart_index: 0,

            key: Vec::new(),
            value_offset: 0,
            value_size: 0,
        })
    }

    fn next_entry_offset(&self) -> usize {
        self.current + self.value_offset + self.value_size
    }

    fn get_restart_point(&self, idx: usize) -> usize {
        assert!(idx < self.num_restarts);
        decode_fixed32(&self.block[self.restart_offset + idx * 4..]) as usize
    }
}

impl<'a> Iterator for BlockIterator<'a> {
    type Item = (Vec<u8>, &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        self.current = self.next_entry_offset();
        if self.current >= self.restart_offset {
            return None;
        }

        let (key_offset, shared, non_shared, value_length) =
            decode_entry(&self.block[self.current..self.restart_offset])?;

        self.key.resize(shared as usize, 0);
        self.key.extend_from_slice(
            &self.block[self.current + key_offset..self.current + key_offset + non_shared as usize],
        );
        self.value_offset = key_offset + non_shared as usize;
        self.value_size = value_length as usize;

        // Note: self.get_restart_point(self.current_restart_index + 1) < self.current
        while self.current_restart_index + 1 < self.num_restarts
            && self.get_restart_point(self.current_restart_index + 1) <= self.current
        {
            self.current_restart_index += 1;
        }

        Some((
            self.key.to_vec(),
            &self.block[self.current + self.value_offset
                ..self.current + self.value_offset + self.value_size],
        ))
    }
}

pub const BLOCK_HANDLE_MAX_ENCODED_LENGTH: usize = 10 + 10;

// type and crc32 size
const BLOCK_TRAILER_SIZE: usize = 5;

pub const FOOTER_ENCODED_LENGTH: usize = 2 * BLOCK_HANDLE_MAX_ENCODED_LENGTH + 8;

const TABLE_MAGIC_NUMBER: u64 = 0xdb4775248b80fb57;

// BlockHandle specifies a Block's location in a file.
// but it's not contain a type(1-byte) and checksum(4-bytes)
#[derive(Debug)]
pub struct BlockHandle {
    pub offset: u64,
    pub size: u64,
}

impl BlockHandle {
    pub fn decode_from(src: &[u8]) -> Result<(BlockHandle, usize)> {
        let (offset, offset_bytes) = decode_varint64(src).ok_or(Status::Corruption)?;
        let (size, size_bytes) = decode_varint64(&src[offset_bytes..]).ok_or(Status::Corruption)?;

        Ok((BlockHandle { offset, size }, offset_bytes + size_bytes))
    }
}

pub fn read_block<'a>(
    file: &File,
    handle: &BlockHandle,
    scratch: &'a mut Vec<u8>,
) -> Result<&'a [u8]> {
    scratch.resize(handle.size as usize + BLOCK_TRAILER_SIZE, 0);
    file.read_exact_at(scratch, handle.offset)?;

    // TODO: check crc verify

    match scratch[handle.size as usize] {
        // no compression
        0 => Ok(&scratch[..handle.size as usize]),
        // TODO: snappy
        1 => todo!("snappy compression"),
        _ => Err(Status::Corruption),
    }
}

#[derive(Debug, AsBytes, FromBytes, FromZeroes)]
#[repr(C)]
struct FooterDecoder {
    // 2 block handles in the rep
    // metaindex block handle (varint64 offset, varint64 size) = p[bytes]
    // index block handle (varint64 offset, varint64 size) = q[bytes]
    // padding = 40 - p - q[bytes]
    rep: [u8; 40],
    magic: u64,
}

#[derive(Debug)]
pub struct Footer {
    pub metaindex_handle: BlockHandle,
    pub index_handle: BlockHandle,
}

impl Footer {
    pub fn decode_from(src: &[u8]) -> Result<Footer> {
        let footer = FooterDecoder::read_from(src).ok_or(Status::Corruption)?;
        if footer.magic != TABLE_MAGIC_NUMBER {
            return Err(Status::Corruption);
        }

        let (metaindex_handle, offset) = BlockHandle::decode_from(&footer.rep[..])?;
        let (index_handle, _) = BlockHandle::decode_from(&footer.rep[offset..])?;

        Ok(Footer {
            metaindex_handle,
            index_handle,
        })
    }
}
