use std::collections::HashMap;

#[derive(Debug)]
pub struct InMemoryStorage {
    data: HashMap<Vec<u8>, Vec<u8>>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        InMemoryStorage {
            data: HashMap::new(),
        }
    }

    pub fn get<'a>(&'a self, key: &[u8]) -> Option<&'a [u8]> {
        self.data.get(key).map(|v| v.as_slice())
    }

    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.data.insert(key, value);
    }
}
