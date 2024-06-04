use crate::{
    decode_fixed32, decode_fixed64, encode_fixed32, encode_fixed64, put_length_prefixed_slice,
    MemTable, Status, ValueType,
};
use crate::{decode_length_prefixed_slice, Result};

enum ValueTypeCode {
    Deletion = 0,
    Value = 1,
}

const WRITE_BATCH_HEADER_SIZE: usize = 12;

#[derive(Debug)]
pub struct WriteBatch {
    rep: Vec<u8>,
}

impl Default for WriteBatch {
    fn default() -> Self {
        Self::new()
    }
}

impl WriteBatch {
    pub fn new() -> Self {
        Self {
            rep: vec![0; WRITE_BATCH_HEADER_SIZE],
        }
    }

    pub fn clear(&mut self) {
        self.rep.clear();
        self.rep.resize(WRITE_BATCH_HEADER_SIZE, 0);
    }

    pub fn set_sequence(&mut self, sequence: u64) {
        encode_fixed64(&mut self.rep[0..8], sequence);
    }

    pub fn set_count(&mut self, count: u32) {
        encode_fixed32(&mut self.rep[8..WRITE_BATCH_HEADER_SIZE], count);
    }

    pub fn get_sequence(&self) -> u64 {
        decode_fixed64(&self.rep[0..8])
    }

    pub fn get_count(&self) -> u32 {
        decode_fixed32(&self.rep[8..WRITE_BATCH_HEADER_SIZE])
    }

    pub fn get_contents(&self) -> &[u8] {
        &self.rep
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        let count = self.get_count();
        self.set_count(count + 1);

        self.rep.push(ValueTypeCode::Value as u8);
        put_length_prefixed_slice(&mut self.rep, key);
        put_length_prefixed_slice(&mut self.rep, value);
    }

    pub fn delete(&mut self, key: &[u8]) {
        let count = self.get_count();
        self.set_count(count + 1);

        self.rep.push(ValueTypeCode::Deletion as u8);
        put_length_prefixed_slice(&mut self.rep, key);
    }

    pub fn apply_to(&self, mem_table: &mut MemTable) -> Result<()> {
        let iter = self.iter();
        for result in iter {
            let value = result?;
            match value {
                ValueType::Value(key, value) => mem_table.set(key, value),
                ValueType::Deletion(key) => mem_table.delete(key),
            }
        }
        Ok(())
    }

    pub fn iter(&self) -> impl Iterator<Item = Result<ValueType<'_>>> {
        WriteBatchIter::new(self)
    }

    pub fn from(rep: impl Into<Vec<u8>>) -> Result<Self> {
        let rep = rep.into();
        if rep.len() < WRITE_BATCH_HEADER_SIZE {
            return Err(Status::Corruption);
        }
        Ok(Self { rep: rep.to_vec() })
    }
}

pub(crate) struct WriteBatchIter<'a> {
    wb: &'a WriteBatch,
    offset: usize,
}

impl<'a> WriteBatchIter<'a> {
    pub fn new(wb: &'a WriteBatch) -> Self {
        assert!(wb.rep.len() >= WRITE_BATCH_HEADER_SIZE);
        Self {
            wb,
            offset: WRITE_BATCH_HEADER_SIZE,
        }
    }
}

impl<'a> Iterator for WriteBatchIter<'a> {
    type Item = Result<ValueType<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.wb.rep.len() {
            return None;
        }

        let value_type_bytes = self.wb.rep[self.offset];
        self.offset += 1;
        let value_type = match value_type_bytes {
            0 => ValueTypeCode::Deletion,
            1 => ValueTypeCode::Value,
            _ => return Some(Err(Status::Corruption)),
        };

        let (key, bytes) = decode_length_prefixed_slice(&self.wb.rep[self.offset..]);
        self.offset += bytes;

        match value_type {
            ValueTypeCode::Deletion => Some(Ok(ValueType::deletion(key))),
            ValueTypeCode::Value => {
                let (value, bytes) = decode_length_prefixed_slice(&self.wb.rep[self.offset..]);
                self.offset += bytes;
                Some(Ok(ValueType::value(key, value)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::write_batch::{ValueTypeCode, WriteBatch, WRITE_BATCH_HEADER_SIZE};
    use crate::ValueType;

    #[test]
    fn test_write_batch() {
        let mut batch = WriteBatch::new();

        batch.put(b"key1", b"value1");
        batch.put(b"key2", b"value2");
        batch.delete(b"key1");
        batch.put(b"key3", b"value3");
        batch.delete(b"key0");
        batch.set_sequence(1);

        assert_eq!(batch.get_sequence(), 1);
        assert_eq!(batch.get_count(), 5);

        #[rustfmt::skip]
        assert_eq!(
            batch.rep[WRITE_BATCH_HEADER_SIZE..],
            vec![
                ValueTypeCode::Value as u8, 4, b'k', b'e', b'y', b'1', 6, b'v', b'a', b'l', b'u', b'e', b'1',
                ValueTypeCode::Value as u8, 4, b'k', b'e', b'y', b'2', 6, b'v', b'a', b'l', b'u', b'e', b'2',
                ValueTypeCode::Deletion as u8, 4, b'k', b'e', b'y', b'1',
                ValueTypeCode::Value as u8, 4, b'k', b'e', b'y', b'3', 6, b'v', b'a', b'l', b'u', b'e', b'3',
                ValueTypeCode::Deletion as u8, 4, b'k', b'e', b'y', b'0',
            ]
        );

        let mut batch = WriteBatch::new();
        let long_key = "a".repeat(127).into_bytes();
        let long_value = "b".repeat(128).into_bytes();
        let long_del_key = "c".repeat(128).into_bytes();
        batch.put(&long_key, &long_value);
        batch.delete(&long_del_key);

        let mut offset = 12;
        // type(put)
        assert_eq!(batch.rep[offset], ValueTypeCode::Value as u8);
        offset += 1;

        // key
        assert_eq!(batch.rep[offset], 0x7f);
        offset += 1;
        assert_eq!(batch.rep[offset..offset + long_key.len()], long_key);
        offset += long_key.len();

        // value
        assert_eq!(batch.rep[offset..offset + 2], vec![0x80, 0x01]);
        offset += 2;
        assert_eq!(batch.rep[offset..offset + long_value.len()], long_value);
        offset += long_value.len();

        // type(delete)
        assert_eq!(batch.rep[offset], ValueTypeCode::Deletion as u8);
        offset += 1;
        // key
        assert_eq!(batch.rep[offset..offset + 2], vec![0x80, 0x01]);
        offset += 2;
        assert_eq!(batch.rep[offset..], long_del_key);
        offset += long_del_key.len();

        assert_eq!(offset, batch.rep.len());
    }

    #[test]
    fn test_write_batch_iter() {
        let mut batch = WriteBatch::new();
        batch.put(b"key1", b"value1");
        batch.put(b"key2", b"value2");
        batch.delete(b"key1");
        batch.put(b"key3", b"value3");
        batch.delete(b"key0");

        let mut iter = batch.iter();
        assert_eq!(iter.next(), Some(Ok(ValueType::value(b"key1", b"value1"))));
        assert_eq!(iter.next(), Some(Ok(ValueType::value(b"key2", b"value2"))));
        assert_eq!(iter.next(), Some(Ok(ValueType::deletion(b"key1"))));
        assert_eq!(iter.next(), Some(Ok(ValueType::value(b"key3", b"value3"))));
        assert_eq!(iter.next(), Some(Ok(ValueType::deletion(b"key0"))));
        assert_eq!(iter.next(), None);
    }
}
