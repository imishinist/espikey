use std::fs::File;
use std::io::Write;

const BLOCK_SIZE: usize = 32768;
const HEADER_SIZE: usize = 7;

#[derive(Debug)]
#[repr(u8)]
pub(crate) enum RecordType {
    Zero = 0,
    Full = 1,
    First = 2,
    Middle = 3,
    Last = 4,
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

    pub(crate) fn append(&mut self, message: &[u8]) -> anyhow::Result<()> {
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

    fn write(&mut self, record_type: RecordType, message: &[u8]) -> anyhow::Result<()> {
        assert!(message.len() <= 0xffff);
        assert!(self.block_offset + HEADER_SIZE + message.len() <= BLOCK_SIZE);

        let length = message.len();

        let mut header = [0; HEADER_SIZE];
        header[4..6].copy_from_slice(&(message.len() as u16).to_le_bytes());
        header[6] = record_type as u8;

        // TODO: checksum (crc32c)

        self.file.write_all(&header)?;
        self.file.write_all(message)?;
        self.file.flush()?;

        self.block_offset += HEADER_SIZE + length;
        Ok(())
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
        drop(writer);

        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open("/tmp/test_writer.log")
            .unwrap();

        let file_size = file.metadata().unwrap().len();
        assert_eq!(file_size / BLOCK_SIZE as u64, 3);
    }
}
