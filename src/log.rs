use std::fs::File;
use std::io::{Read, Write};

use zerocopy::{AsBytes, FromBytes, FromZeroes, Unaligned};

use crate::{Result, Status};

const BLOCK_SIZE: usize = 32768;
const HEADER_SIZE: usize = 7;

#[derive(Debug)]
#[repr(u8)]
pub(crate) enum RecordType {
    #[allow(dead_code)]
    Zero = 0,
    Full = 1,
    First = 2,
    Middle = 3,
    Last = 4,
}

#[derive(Debug, Unaligned, AsBytes, FromBytes, FromZeroes)]
#[repr(C, packed)]
struct WalHeader {
    checksum: u32,
    length: u16,
    record_type: u8,
}

#[derive(Debug)]
pub(crate) struct Writer {
    file: File,
    block_offset: usize,
}

impl Writer {
    pub(crate) fn new(file: File) -> Writer {
        Writer {
            file,
            block_offset: 0,
        }
    }

    pub(crate) fn sync(&mut self) -> Result<()> {
        self.file.sync_all()?;
        Ok(())
    }

    pub(crate) fn append(&mut self, message: &[u8]) -> Result<()> {
        let mut remains_message_bytes = message.len();
        let mut offset = 0;
        let mut begin = true;

        while {
            let space = BLOCK_SIZE - self.block_offset;
            if space < HEADER_SIZE {
                if space > 0 {
                    self.file.write_all(&[0; HEADER_SIZE][..space])?;
                }
                self.block_offset = 0;
            }

            let available = BLOCK_SIZE - self.block_offset - HEADER_SIZE;
            let fragment_length = std::cmp::min(available, remains_message_bytes);
            let end = fragment_length == remains_message_bytes;

            let record_type = match (begin, end) {
                (true, true) => RecordType::Full,
                (true, false) => RecordType::First,
                (false, true) => RecordType::Last,
                (false, false) => RecordType::Middle,
            };
            // write
            self.write(record_type, &message[offset..offset + fragment_length])?;

            offset += fragment_length;
            remains_message_bytes -= fragment_length;
            begin = false;
            remains_message_bytes > 0
        } {}

        Ok(())
    }

    fn write(&mut self, record_type: RecordType, message: &[u8]) -> Result<()> {
        assert!(message.len() <= 0xffff);
        assert!(self.block_offset + HEADER_SIZE + message.len() <= BLOCK_SIZE);

        let length = message.len();
        let wal_header = WalHeader {
            checksum: crc32fast::hash(message),
            length: length as u16,
            record_type: record_type as u8,
        };

        self.file.write_all(wal_header.as_bytes())?;
        self.file.write_all(message)?;
        self.file.flush()?;

        self.block_offset += HEADER_SIZE + length;
        Ok(())
    }
}

pub struct Reader {
    file: File,

    buffer_offset: usize,
    buffer_length: usize,
    buffer: [u8; BLOCK_SIZE],

    eof: bool,
}

impl Reader {
    pub fn new(file: File) -> Reader {
        Reader {
            file,
            buffer_offset: 0,
            buffer_length: 0,
            buffer: [0; BLOCK_SIZE],
            eof: false,
        }
    }

    pub fn read(&mut self) -> Result<Option<Vec<u8>>> {
        let mut in_fragmented_record = false;

        let mut fragment = Vec::new();
        loop {
            let (record_type, message) = match self.read_physical()? {
                Some(v) => v,
                // EOF
                None => return Ok(None),
            };

            match record_type {
                RecordType::Full => {
                    if in_fragmented_record {
                        return Err(Status::Corruption);
                    }
                    return Ok(Some(message.to_vec()));
                }
                RecordType::First => {
                    if in_fragmented_record {
                        return Err(Status::Corruption);
                    }
                    in_fragmented_record = true;
                    fragment.extend(message);
                }
                RecordType::Middle => {
                    if !in_fragmented_record {
                        return Err(Status::Corruption);
                    }
                    fragment.extend(message);
                }
                RecordType::Last => {
                    if !in_fragmented_record {
                        return Err(Status::Corruption);
                    }
                    fragment.extend(message);
                    return Ok(Some(fragment));
                }
                _ => return Err(Status::Corruption),
            }
        }
    }

    fn read_physical(&mut self) -> Result<Option<(RecordType, &[u8])>> {
        loop {
            if self.buffer_length - self.buffer_offset < HEADER_SIZE {
                if self.eof {
                    return Ok(None);
                }
                self.buffer_offset = 0;
                let nreads = match self.file.read(&mut self.buffer) {
                    Ok(n) if n < BLOCK_SIZE => {
                        self.eof = true;
                        n
                    }
                    Ok(n) => n,
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(e) => return Err(e.into()),
                };
                self.buffer_length = nreads;
                continue;
            }
            let header = WalHeader::read_from(
                &self.buffer[self.buffer_offset..self.buffer_offset + HEADER_SIZE],
            )
            .unwrap();
            let length = header.length as usize;

            if length + HEADER_SIZE > self.buffer_length - self.buffer_offset {
                self.buffer_offset = 0;
                self.buffer_length = 0;
                if !self.eof {
                    return Err(Status::Corruption);
                }
                return Ok(None);
            }
            if header.record_type == RecordType::Zero as u8 && length == 0 {
                self.buffer_offset = 0;
                self.buffer_length = 0;
                return Err(Status::Corruption);
            }

            // TODO: check checksum

            let record_offset = self.buffer_offset + HEADER_SIZE;
            self.buffer_offset += HEADER_SIZE + length;

            let record_type = match header.record_type {
                x if x == RecordType::Full as u8 => RecordType::Full,
                x if x == RecordType::First as u8 => RecordType::First,
                x if x == RecordType::Middle as u8 => RecordType::Middle,
                x if x == RecordType::Last as u8 => RecordType::Last,
                _ => return Err(Status::Corruption),
            };

            return Ok(Some((
                record_type,
                &self.buffer[record_offset..record_offset + length],
            )));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;

    #[test]
    fn test_writer() {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open("/tmp/test_writer.log")
            .unwrap();
        let mut writer = Writer::new(file);

        let a = [1; 1000];
        let b = [2; 97270];
        let c = [3; 8000];

        writer.append(&a).unwrap();
        writer.append(&b).unwrap();
        writer.append(&c).unwrap();
        writer.sync().unwrap();
        drop(writer);

        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open("/tmp/test_writer.log")
            .unwrap();

        let file_size = file.metadata().unwrap().len();
        assert_eq!(file_size / BLOCK_SIZE as u64, 3);
    }

    #[test]
    fn test_reader() {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open("/tmp/test_reader.log")
            .unwrap();
        let mut writer = Writer::new(file);

        writer.append(b"hello world").unwrap();
        drop(writer);

        let file = OpenOptions::new()
            .read(true)
            .open("/tmp/test_reader.log")
            .unwrap();

        let mut reader = Reader::new(file);
        assert_eq!(reader.read(), Ok(Some(b"hello world".to_vec())));
    }

    #[test]
    fn test_wal() {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open("/tmp/test_wal.log")
            .unwrap();
        let table = [1, HEADER_SIZE - 1, BLOCK_SIZE - 1, BLOCK_SIZE * 2 - 1];

        let mut writer = Writer::new(file);
        for size in table {
            for d in [0, 1, 2] {
                let i = size + d;
                let value = i % 0xff;
                let message = vec![value as u8; i];
                assert_eq!(writer.append(&message), Ok(()));
            }
        }
        assert_eq!(writer.sync(), Ok(()));
        drop(writer);

        let file = OpenOptions::new()
            .read(true)
            .open("/tmp/test_wal.log")
            .unwrap();
        let mut reader = Reader::new(file);

        for size in table {
            for d in [0, 1, 2] {
                let i = size + d;
                let value = i % 0xff;
                let message = vec![value as u8; i];
                assert_eq!(reader.read(), Ok(Some(message)));
            }
        }
        assert_eq!(reader.read(), Ok(None));
    }
}
