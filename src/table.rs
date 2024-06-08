use std::fs::File;
use std::os::unix::fs::FileExt;
use zerocopy::{AsBytes, FromBytes, FromZeroes};

use crate::{decode_varint64, Result, Status};

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
