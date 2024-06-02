use crate::{
    decode_fixed32, decode_fixed64, encode_fixed32, encode_fixed64, put_length_prefixed_slice,
};

enum ValueType {
    Deletion = 0,
    Value = 1,
}

#[derive(Debug)]
pub struct WriteBatch {
    rep: Vec<u8>,
}

impl WriteBatch {
    pub fn new() -> Self {
        Self {
            rep: vec![0; 8 + 4],
        }
    }

    pub fn set_sequence(&mut self, sequence: u64) {
        encode_fixed64(&mut self.rep[0..8], sequence);
    }

    pub fn set_count(&mut self, count: u32) {
        encode_fixed32(&mut self.rep[8..12], count);
    }

    #[allow(dead_code)]
    pub fn get_sequence(&self) -> u64 {
        decode_fixed64(&self.rep[0..8])
    }

    pub fn get_count(&self) -> u32 {
        decode_fixed32(&self.rep[8..12])
    }

    pub fn get_contents(&self) -> &[u8] {
        &self.rep
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        let count = self.get_count();
        self.set_count(count + 1);

        self.rep.push(ValueType::Value as u8);
        put_length_prefixed_slice(&mut self.rep, key);
        put_length_prefixed_slice(&mut self.rep, value);
    }

    pub fn delete(&mut self, key: &[u8]) {
        let count = self.get_count();
        self.set_count(count + 1);

        self.rep.push(ValueType::Deletion as u8);
        put_length_prefixed_slice(&mut self.rep, key);
    }
}

#[cfg(test)]
mod tests {
    use crate::write_batch::{ValueType, WriteBatch};

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
            batch.rep[12..],
            vec![
                ValueType::Value as u8, 4, b'k', b'e', b'y', b'1', 6, b'v', b'a', b'l', b'u', b'e', b'1',
                ValueType::Value as u8, 4, b'k', b'e', b'y', b'2', 6, b'v', b'a', b'l', b'u', b'e', b'2',
                ValueType::Deletion as u8, 4, b'k', b'e', b'y', b'1',
                ValueType::Value as u8, 4, b'k', b'e', b'y', b'3', 6, b'v', b'a', b'l', b'u', b'e', b'3',
                ValueType::Deletion as u8, 4, b'k', b'e', b'y', b'0',
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
        assert_eq!(batch.rep[offset], ValueType::Value as u8);
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
        assert_eq!(batch.rep[offset], ValueType::Deletion as u8);
        offset += 1;
        // key
        assert_eq!(batch.rep[offset..offset + 2], vec![0x80, 0x01]);
        offset += 2;
        assert_eq!(batch.rep[offset..], long_del_key);
        offset += long_del_key.len();

        assert_eq!(offset, batch.rep.len());
    }
}
