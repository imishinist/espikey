use crate::{decode_length_prefixed_slice, decode_varint32, Result, Status};
use std::collections::HashSet;

const NUM_LEVELS: usize = 7;

#[derive(Debug)]
pub struct FileMetaData {
    pub number: usize,
    pub file_size: usize,
    // TODO: InternalKey
    pub smallest: Vec<u8>,
    pub largest: Vec<u8>,
}

#[derive(Debug)]
enum Tag {
    Comparator,
    LogNumber,
    NextFileNumber,
    LastSequence,
    CompactPointer,
    DeletedFile,
    NewFile,
    PrevLogNumber,
}

impl From<Tag> for u8 {
    fn from(value: Tag) -> Self {
        match value {
            Tag::Comparator => 1,
            Tag::LogNumber => 2,
            Tag::NextFileNumber => 3,
            Tag::LastSequence => 4,
            Tag::CompactPointer => 5,
            Tag::DeletedFile => 6,
            Tag::NewFile => 7,
            Tag::PrevLogNumber => 9,
        }
    }
}

impl From<Tag> for u32 {
    fn from(value: Tag) -> Self {
        <Tag as Into<u8>>::into(value) as u32
    }
}

impl TryFrom<u8> for Tag {
    type Error = Status;

    fn try_from(value: u8) -> std::result::Result<Self, Self::Error> {
        match value {
            1 => Ok(Tag::Comparator),
            2 => Ok(Tag::LogNumber),
            3 => Ok(Tag::NextFileNumber),
            4 => Ok(Tag::LastSequence),
            5 => Ok(Tag::CompactPointer),
            6 => Ok(Tag::DeletedFile),
            7 => Ok(Tag::NewFile),
            9 => Ok(Tag::PrevLogNumber),
            _ => Err(Status::Corruption),
        }
    }
}

impl TryFrom<u32> for Tag {
    type Error = Status;

    fn try_from(value: u32) -> std::result::Result<Self, Self::Error> {
        (value as u8).try_into()
    }
}

fn get_level(src: &[u8], pos: &mut usize) -> Result<usize> {
    let (level, num_bytes) = decode_varint32(&src[*pos..]);
    *pos += num_bytes;
    if level >= NUM_LEVELS as u32 {
        return Err(Status::Corruption);
    }
    Ok(level as usize)
}

#[derive(Debug)]
pub struct VersionEdit {
    pub comparator: Option<Vec<u8>>,
    pub log_number: Option<usize>,
    pub prev_log_number: Option<usize>,
    pub next_file_number: Option<usize>,
    pub last_sequence: Option<u64>,

    // TODO: InternalKey
    pub compact_pointers: Vec<(usize, Vec<u8>)>,
    pub deleted_files: HashSet<(usize, usize)>,
    pub new_files: Vec<(usize, FileMetaData)>,
}

impl VersionEdit {
    pub fn decode_from(src: &[u8]) -> Result<Self> {
        let mut pos = 0;

        let mut comparator = None;
        let mut log_number = None;
        let mut prev_log_number = None;
        let mut next_file_number = None;
        let mut last_sequence = None;
        let mut compact_pointers = Vec::new();
        let mut deleted_files = HashSet::new();
        let mut new_files = Vec::new();
        while pos < src.len() {
            let (tag, bytes) = decode_varint32(&src[pos..]);
            pos += bytes;

            // TODO: varint64
            match tag.try_into()? {
                Tag::Comparator => {
                    let (slice, bytes) = decode_length_prefixed_slice(&src[pos..]);
                    pos += bytes;
                    comparator = Some(slice.to_vec());
                }
                Tag::LogNumber => {
                    let (num, bytes) = decode_varint32(&src[pos..]);
                    pos += bytes;
                    log_number = Some(num as usize);
                }
                Tag::PrevLogNumber => {
                    let (num, bytes) = decode_varint32(&src[pos..]);
                    pos += bytes;
                    prev_log_number = Some(num as usize);
                }
                Tag::NextFileNumber => {
                    let (num, bytes) = decode_varint32(&src[pos..]);
                    pos += bytes;
                    next_file_number = Some(num as usize);
                }
                Tag::LastSequence => {
                    let (num, bytes) = decode_varint32(&src[pos..]);
                    pos += bytes;
                    last_sequence = Some(num as u64);
                }
                Tag::CompactPointer => {
                    let level = get_level(src, &mut pos)?;
                    let (slice, bytes) = decode_length_prefixed_slice(&src[pos..]);
                    pos += bytes;
                    compact_pointers.push((level, slice.to_vec()));
                }
                Tag::DeletedFile => {
                    let level = get_level(src, &mut pos)?;
                    let (num, bytes) = decode_varint32(&src[pos..]);
                    pos += bytes;
                    deleted_files.insert((level, num as usize));
                }
                Tag::NewFile => {
                    let level = get_level(src, &mut pos)?;
                    let (num, bytes) = decode_varint32(&src[pos..]);
                    pos += bytes;
                    let (file_size, bytes) = decode_varint32(&src[pos..]);
                    pos += bytes;
                    let (smallest, bytes) = decode_length_prefixed_slice(&src[pos..]);
                    pos += bytes;
                    let (largest, bytes) = decode_length_prefixed_slice(&src[pos..]);
                    pos += bytes;
                    new_files.push((
                        level,
                        FileMetaData {
                            number: num as usize,
                            file_size: file_size as usize,
                            smallest: smallest.to_vec(),
                            largest: largest.to_vec(),
                        },
                    ));
                }
            }
        }

        Ok(Self {
            comparator,
            log_number,
            prev_log_number,
            next_file_number,
            last_sequence,
            compact_pointers,
            deleted_files,
            new_files,
        })
    }
}
