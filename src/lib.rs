use std::fs::File;
use std::path::PathBuf;
use std::{collections::HashMap, io::Write};

use crate::write_batch::WriteBatch;
use itertools::Itertools;
use thiserror::Error;

pub mod log;
pub mod write_batch;

pub type Result<T> = anyhow::Result<T, Status>;

#[derive(Debug, Error)]
pub enum Status {
    #[error("Not found")]
    NotFound,
    #[error("Corruption")]
    Corruption,
    #[error("Not supported")]
    NotSupported,
    #[error("Invalid argument")]
    InvalidArgument,
    #[error("IO error")]
    IOError(#[from] std::io::Error),
}

impl PartialEq<Self> for Status {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Status::NotFound, Status::NotFound) => true,
            (Status::Corruption, Status::Corruption) => true,
            (Status::NotSupported, Status::NotSupported) => true,
            (Status::InvalidArgument, Status::InvalidArgument) => true,
            (Status::IOError(e1), Status::IOError(e2)) => {
                e1.kind() == e2.kind() && e1.to_string() == e2.to_string()
            }
            _ => false,
        }
    }
}

impl Eq for Status {}

#[derive(Debug, PartialEq, Eq)]
pub enum ValueType<'a> {
    Deletion(&'a [u8]),
    Value(&'a [u8], &'a [u8]),
}

impl<'a> ValueType<'a> {
    pub fn deletion(key: &'a [u8]) -> Self {
        ValueType::Deletion(key)
    }

    pub fn value(v1: &'a [u8], v2: &'a [u8]) -> Self {
        ValueType::Value(v1, v2)
    }
}

#[derive(Debug)]
pub struct DB {
    mem_table: MemTable,
    log_writer: log::Writer,

    sequence: u64,
    wb: WriteBatch,
}

impl DB {
    pub fn open(db_path: impl Into<PathBuf>) -> Result<Self> {
        let db_path = db_path.into();

        let log_file = File::create(db_path.join("espikey.wal"))?;
        let log_writer = log::Writer::new(log_file);
        Ok(DB {
            mem_table: MemTable::default(),
            log_writer,

            // TODO: from manifest
            sequence: 0,
            wb: WriteBatch::new(),
        })
    }

    pub fn get(&self, key: &[u8]) -> Result<Vec<u8>> {
        self.mem_table
            .get(key)
            .map(|v| v.to_vec())
            .ok_or(Status::NotFound)
    }

    pub fn put(&mut self, key: &[u8], value: &[u8], sync: bool) -> Result<()> {
        self.wb.clear();
        self.wb.put(key, value);
        self.write(sync)?;
        Ok(())
    }

    pub fn delete(&mut self, key: &[u8], sync: bool) -> Result<()> {
        self.wb.clear();
        self.wb.delete(key);
        self.write(sync)?;
        Ok(())
    }

    fn write(&mut self, sync: bool) -> Result<()> {
        let mut last_sequence = self.sequence;

        self.wb.set_sequence(last_sequence + 1);
        last_sequence += self.wb.get_count() as u64;
        self.log_writer.append(self.wb.get_contents())?;

        if sync {
            self.log_writer.sync()?;
        }
        self.wb.apply_to(&mut self.mem_table)?;

        self.sequence = last_sequence;
        Ok(())
    }
}

pub(crate) fn put_varint32(buf: &mut Vec<u8>, mut value: u32) -> usize {
    let mut cnt = 0;
    while {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        cnt += 1;

        value != 0
    } {}
    cnt
}

pub(crate) fn write_varint32<W: Write>(writer: &mut W, mut value: u32) -> std::io::Result<usize> {
    let mut cnt = 0;
    while {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        writer.write_all(&[byte])?;
        cnt += 1;

        value != 0
    } {}
    Ok(cnt)
}

pub(crate) fn decode_varint32(buf: &[u8]) -> (u32, usize) {
    let mut value = 0;
    let mut shift = 0;
    let mut offset = 0;

    while {
        let byte = buf[offset];
        value |= ((byte & 0x7f) as u32) << shift;
        shift += 7;
        offset += 1;

        byte & 0x80 != 0
    } {}
    (value, offset)
}

#[test]
fn test_put_varint32() {
    let mut buf = Vec::new();
    put_varint32(&mut buf, 127);
    assert_eq!(buf, vec![127]);
    buf.clear();

    let mut buf = Vec::new();
    put_varint32(&mut buf, 128);
    assert_eq!(buf, vec![0x80, 0x01]);
}

#[allow(dead_code)]
pub(crate) fn put_fixed32(buf: &mut Vec<u8>, value: u32) {
    buf.extend_from_slice(&value.to_le_bytes());
}

fn write_fixed32<W: Write>(writer: &mut W, value: u32) -> std::io::Result<()> {
    writer.write_all(&value.to_le_bytes())?;
    Ok(())
}

#[allow(dead_code)]
pub(crate) fn put_fixed64(buf: &mut Vec<u8>, value: u64) {
    buf.extend_from_slice(&value.to_le_bytes());
}

fn write_fixed64<W: Write>(writer: &mut W, value: u64) -> std::io::Result<()> {
    writer.write_all(&value.to_le_bytes())?;
    Ok(())
}

pub(crate) fn encode_fixed32(buf: &mut [u8], value: u32) {
    assert!(
        buf.len() >= 4,
        "buf length must be greater than or equal to 4"
    );
    buf.copy_from_slice(&value.to_le_bytes());
}

pub(crate) fn encode_fixed64(buf: &mut [u8], value: u64) {
    assert!(
        buf.len() >= 8,
        "buf length must be greater than or equal to 8"
    );
    buf.copy_from_slice(&value.to_le_bytes());
}

pub(crate) fn decode_fixed32(data: &[u8]) -> u32 {
    assert!(
        data.len() >= 4,
        "data length must be greater than or equal to 4"
    );
    u32::from_le_bytes([data[0], data[1], data[2], data[3]])
}

pub(crate) fn decode_fixed64(data: &[u8]) -> u64 {
    assert!(
        data.len() >= 8,
        "data length must be greater than or equal to 8"
    );
    u64::from_le_bytes([
        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
    ])
}

pub(crate) fn put_length_prefixed_slice(buf: &mut Vec<u8>, data: &[u8]) {
    put_varint32(buf, data.len() as u32);
    buf.extend_from_slice(data);
}

pub(crate) fn decode_length_prefixed_slice(data: &[u8]) -> (&[u8], usize) {
    let (length, offset) = decode_varint32(data);
    assert!(offset + length as usize <= data.len());

    let value = &data[offset..offset + length as usize];
    (value, offset + length as usize)
}

#[test]
fn test_length_prefixed_slice() {
    let mut buf = Vec::new();
    put_length_prefixed_slice(&mut buf, b"hello");
    assert_eq!(buf, vec![5, b'h', b'e', b'l', b'l', b'o']);

    let (value, offset) = decode_length_prefixed_slice(&buf);
    assert_eq!(value, b"hello");
    assert_eq!(offset, 6);
}

#[allow(dead_code)]
struct BlockBuilder {
    buf: Vec<u8>,
    restarts: Vec<u32>,
    last_key: Vec<u8>,

    counter: usize,
    block_restart_interval: usize,
}

#[allow(dead_code)]
impl BlockBuilder {
    fn new(block_restart_interval: usize) -> Self {
        BlockBuilder {
            buf: Vec::new(),
            restarts: vec![0],
            last_key: Vec::new(),
            counter: 0,
            block_restart_interval,
        }
    }

    fn add(&mut self, key: &[u8], value: &[u8]) {
        let mut shared = 0;
        if self.counter < self.block_restart_interval {
            let min_length = std::cmp::min(key.len(), self.last_key.len());
            while shared < min_length && key[shared] == self.last_key[shared] {
                shared += 1;
            }
        } else {
            self.restarts.push(self.buf.len() as u32);
            self.counter = 0;
        }
        let non_shared = key.len() - shared;

        put_varint32(&mut self.buf, shared as u32);
        put_varint32(&mut self.buf, non_shared as u32);
        put_varint32(&mut self.buf, value.len() as u32);

        self.buf.extend_from_slice(&key[shared..]);
        self.buf.extend_from_slice(value);

        self.last_key = key.to_vec();
        self.counter += 1;
    }

    fn finish(mut self) -> Vec<u8> {
        // Write restarts
        for restart in self.restarts.iter() {
            self.buf.extend_from_slice(&restart.to_le_bytes());
        }

        put_fixed32(&mut self.buf, self.restarts.len() as u32);
        self.buf
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ValueItem<T> {
    Deletion,
    Value(T),
}

impl<T> ValueItem<T> {
    fn map<U, F: FnOnce(&T) -> U>(&self, f: F) -> ValueItem<U> {
        match self {
            ValueItem::Deletion => ValueItem::Deletion,
            ValueItem::Value(v) => ValueItem::Value(f(v)),
        }
    }

    fn as_ref(&self) -> ValueItem<&T> {
        match self {
            ValueItem::Deletion => ValueItem::Deletion,
            ValueItem::Value(v) => ValueItem::Value(v),
        }
    }
}

#[derive(Debug, Default)]
pub struct MemTable {
    total_bytes: usize,
    entry_count: usize,
    items: HashMap<Vec<u8>, ValueItem<Vec<u8>>>,
}

impl MemTable {
    pub fn get<'a>(&'a self, key: &[u8]) -> Option<&'a [u8]> {
        self.items.get(key).and_then(|v| match v {
            ValueItem::Deletion => None,
            ValueItem::Value(v) => Some(v.as_slice()),
        })
    }

    pub fn set(&mut self, key: &[u8], value: &[u8]) {
        self.items
            .entry(key.to_vec())
            .and_modify(|v| {
                if let ValueItem::Value(v) = v {
                    self.total_bytes -= v.len();
                }
                self.total_bytes += value.len();
                *v = ValueItem::Value(value.to_vec());
            })
            .or_insert_with(|| {
                self.total_bytes += key.len() + value.len();
                self.entry_count += 1;
                ValueItem::Value(value.to_vec())
            });
    }

    pub fn delete(&mut self, key: &[u8]) {
        self.items
            .entry(key.to_vec())
            .and_modify(|v| {
                if let ValueItem::Value(v) = v {
                    self.total_bytes -= v.len();
                }
                *v = ValueItem::Deletion;
            })
            .or_insert_with(|| {
                self.total_bytes += key.len();
                self.entry_count += 1;
                ValueItem::Deletion
            });
    }

    pub fn iter(&self) -> impl Iterator<Item = (&[u8], ValueItem<&[u8]>)> {
        self.items
            .iter()
            .sorted_by(|(k1, _), (k2, _)| k1.cmp(k2))
            .map(|(k, v)| (k.as_slice(), v.as_ref().map(|v| v.as_slice())))
    }
}

pub fn serialize_to_sstable<W: Write>(writer: &mut W, memtable: MemTable) -> anyhow::Result<()> {
    write_fixed32(writer, memtable.entry_count as u32)?;
    for (k, v) in memtable.iter() {
        write_varint32(writer, k.len() as u32)?;
        writer.write_all(k)?;

        match v {
            ValueItem::Deletion => {
                let tag = 0u64;
                write_fixed64(writer, tag)?;
                write_fixed32(writer, 0)?;
            }
            ValueItem::Value(v) => {
                let tag = 1u64;
                write_fixed64(writer, tag)?;
                write_fixed32(writer, v.len() as u32)?;

                writer.write_all(v)?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! v {
        ($e:expr) => {
            $e.as_slice()
        };
    }

    #[test]
    fn test_memtable() {
        let mut memtable = MemTable::default();
        memtable.set(b"key1", b"value1");
        memtable.set(b"key2", b"value2");
        memtable.set(b"key0", b"value");
        memtable.set(b"key0", b"value0");
        memtable.delete(b"key1");

        assert_eq!(memtable.get(b"key0"), Some(v!(b"value0")));
        assert_eq!(memtable.get(b"key1"), None);
        assert_eq!(memtable.entry_count, 3);

        let iter = memtable.iter();
        let kvs = iter.collect::<Vec<_>>();
        assert_eq!(
            kvs,
            vec![
                (v!(b"key0"), ValueItem::Value(v!(b"value0"))),
                (v!(b"key1"), ValueItem::Deletion),
                (v!(b"key2"), ValueItem::Value(v!(b"value2"))),
            ]
        );
    }

    #[test]
    #[rustfmt::skip]
    fn test_serialize_memtable() {
        let mut memtable = MemTable::default();
        memtable.set(b"key1", b"value1");
        memtable.set(b"key2", b"value2");
        memtable.set(b"key0", b"value0");
        memtable.delete(b"key1");

        let mut buf = Vec::new();
        assert!(serialize_to_sstable(&mut buf, memtable).is_ok());

        assert_eq!(
            buf,
            vec![
                3, 0, 0, 0, // entry count
                4, b'k', b'e', b'y', b'0', 1, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, b'v', b'a', b'l', b'u', b'e', b'0',
                4, b'k', b'e', b'y', b'1', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                4, b'k', b'e', b'y', b'2', 1, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, b'v', b'a', b'l', b'u', b'e', b'2',
            ]
        )
    }

    #[test]
    fn test_block_builder() {
        let restart_interval = 2;
        let mut block_builder = BlockBuilder::new(restart_interval);
        block_builder.add(b"key1", b"value1");
        block_builder.add(b"key2", b"value2");
        block_builder.add(b"key0", b"value0");

        let block = block_builder.finish();
        let restart_offset = 1 /* varint32 bytes */ * 3 /* three field */ * restart_interval as u8
            + b"key1value1".len() as u8
            + b"2value2".len() as u8;

        #[rustfmt::skip]
        assert_eq!(
            block,
            vec![
                0, 4, 6, b'k', b'e', b'y', b'1', b'v', b'a', b'l', b'u', b'e', b'1',
                3, 1, 6, b'2', b'v', b'a', b'l', b'u', b'e', b'2',
                0, 4, 6, b'k', b'e', b'y', b'0', b'v', b'a', b'l', b'u', b'e', b'0',
                0, 0, 0, 0, restart_offset, 0, 0, 0, 2, 0, 0, 0
            ]
        );
    }
}
