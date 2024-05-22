use std::{collections::HashMap, io::Write};

use itertools::Itertools;

#[derive(Debug)]
pub struct MemTable {
    total_bytes: usize,
    entry_count: usize,
    data: HashMap<Vec<u8>, Vec<u8>>,
}

impl MemTable {
    pub fn new() -> Self {
        MemTable {
            total_bytes: 0,
            entry_count: 0,
            data: HashMap::new(),
        }
    }

    pub fn get<'a>(&'a self, key: &[u8]) -> Option<&'a [u8]> {
        self.data.get(key).map(|v| v.as_slice())
    }

    pub fn set(&mut self, key: &[u8], value: &[u8]) {
        self.total_bytes += key.len() + value.len();
        self.entry_count += 1;
        self.data.insert(key.to_vec(), value.to_vec());
    }

    pub fn iter(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        self.data
            .iter()
            .sorted_by(|(k1, v1), (k2, v2)| k1.cmp(k2).then(v1.cmp(v2)))
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
    }
}

pub fn serialize_to_sstable<W: Write>(writer: &mut W, memtable: MemTable) -> anyhow::Result<()> {
    writer.write_all(&(memtable.entry_count as u32).to_le_bytes())?;
    for (k, v) in memtable.iter() {
        // TODO: varint32 for key and value length
        writer.write_all(&(k.len() as u32).to_le_bytes())?;
        writer.write_all(k)?;
        writer.write_all(&(v.len() as u32).to_le_bytes())?;
        writer.write_all(v)?;
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
        let mut memtable = MemTable::new();
        memtable.set(b"key1", b"value1");
        memtable.set(b"key2", b"value2");
        memtable.set(b"key0", b"value0");

        assert_eq!(memtable.get(b"key1"), Some(v!(b"value1")));

        let iter = memtable.iter();
        let kvs = iter.collect::<Vec<_>>();
        assert_eq!(
            kvs,
            vec![
                (v!(b"key0"), v!(b"value0")),
                (v!(b"key1"), v!(b"value1")),
                (v!(b"key2"), v!(b"value2")),
            ]
        );
    }

    #[test]
    fn test_serialize_memtable() {
        let mut memtable = MemTable::new();
        memtable.set(b"key1", b"value1");
        memtable.set(b"key2", b"value2");
        memtable.set(b"key0", b"value0");

        let mut buf = Vec::new();
        assert!(serialize_to_sstable(&mut buf, memtable).is_ok());

        assert_eq!(
            buf,
            vec![
                3, 0, 0, 0, // entry count
                4, 0, 0, 0, b'k', b'e', b'y', b'0', 6, 0, 0, 0, b'v', b'a', b'l', b'u', b'e', b'0',
                4, 0, 0, 0, b'k', b'e', b'y', b'1', 6, 0, 0, 0, b'v', b'a', b'l', b'u', b'e', b'1',
                4, 0, 0, 0, b'k', b'e', b'y', b'2', 6, 0, 0, 0, b'v', b'a', b'l', b'u', b'e', b'2',
            ]
        )
    }
}
