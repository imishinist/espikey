use std::{collections::HashMap, io::Write};

use itertools::Itertools;

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

        // TODO: varint32 for shared, non_shared and value length
        self.buf.extend_from_slice(&(shared as u32).to_le_bytes());
        self.buf
            .extend_from_slice(&(non_shared as u32).to_le_bytes());
        self.buf
            .extend_from_slice(&(value.len() as u32).to_le_bytes());
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

        let len = self.restarts.len() as u32;
        self.buf.extend_from_slice(&len.to_le_bytes());

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
    writer.write_all(&(memtable.entry_count as u32).to_le_bytes())?;
    for (k, v) in memtable.iter() {
        // TODO: varint32 for key and value length
        writer.write_all(&(k.len() as u32).to_le_bytes())?;
        writer.write_all(k)?;

        match v {
            ValueItem::Deletion => {
                let tag = 0u64;
                writer.write_all(&tag.to_le_bytes())?;
                writer.write_all(&0u32.to_le_bytes())?;
            }
            ValueItem::Value(v) => {
                let tag = 1u64;
                writer.write_all(&tag.to_le_bytes())?;

                writer.write_all(&(v.len() as u32).to_le_bytes())?;
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
                4, 0, 0, 0, b'k', b'e', b'y', b'0', 1, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, b'v', b'a',
                b'l', b'u', b'e', b'0', 4, 0, 0, 0, b'k', b'e', b'y', b'1', 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 4, 0, 0, 0, b'k', b'e', b'y', b'2', 1, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0,
                b'v', b'a', b'l', b'u', b'e', b'2',
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
        let restart_offset = 4 /* fixed byte */ * 3 /* three field */ * restart_interval as u8
            + b"key1value1".len() as u8
            + b"2value2".len() as u8;

        #[rustfmt::skip]
        assert_eq!(
            block,
            vec![
                0, 0, 0, 0, 4, 0, 0, 0, 6, 0, 0, 0, b'k', b'e', b'y', b'1', b'v', b'a', b'l', b'u', b'e', b'1',
                3, 0, 0, 0, 1, 0, 0, 0, 6, 0, 0, 0, b'2', b'v', b'a', b'l', b'u', b'e', b'2',
                0, 0, 0, 0, 4, 0, 0, 0, 6, 0, 0, 0, b'k', b'e', b'y', b'0', b'v', b'a', b'l', b'u', b'e', b'0',
                0, 0, 0, 0, restart_offset, 0, 0, 0, 2, 0, 0, 0
            ]
        );
    }
}
