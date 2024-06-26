use std::fs::OpenOptions;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;

use clap::Parser;
use itertools::Itertools;

use espikey::table::{Block, BlockHandle, Footer, FOOTER_ENCODED_LENGTH};
use espikey::version_edit::VersionEdit;
use espikey::write_batch::{ValueTypeCode, WriteBatch};
use espikey::{log, InternalKey};

#[derive(Debug, Clone, Copy)]
enum Mode {
    Manifest,
    Table,
    Wal,
}

impl Mode {
    // estimate mode from file name
    fn from_file_name(file_name: &str) -> Self {
        if file_name.ends_with(".log") {
            Mode::Wal
        } else if file_name.ends_with(".ldb") {
            Mode::Table
        } else {
            Mode::Manifest
        }
    }
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about=None)]
#[clap(propagate_version = true)]
struct EspikeyTool {
    file: PathBuf,

    #[clap(short, long, default_value_t = false)]
    raw: bool,
}

fn encode_bytes_to_hex(data: &[u8]) -> String {
    data.iter().map(|byte| format!("{:02x}", byte)).join(" ")
}

fn show_human_readable(prefix: &str, data: &[u8]) {
    print!("{}[{}]=>", prefix, encode_bytes_to_hex(data));
    print!("\"");
    for byte in data {
        if byte.is_ascii_alphanumeric() {
            print!("{}", *byte as char);
        } else {
            print!(".");
        }
    }
    print!("\"");
    println!();
}

#[allow(dead_code)]
fn show_wal_record(data: &[u8]) {
    // show like hexdump with offset
    for chunk in data.chunks(16) {
        print!(
            "\t{:08x}\t",
            chunk.as_ptr() as usize - data.as_ptr() as usize
        );
        print!("{} ", encode_bytes_to_hex(chunk));
        println!();
    }
}

fn show_internal_key(prefix: &str, ikey: &InternalKey) {
    let user_key = ikey.user_key();
    let seq = ikey.sequence();
    let ty = match ikey.value_type_code() {
        Ok(ValueTypeCode::Deletion) => "deletion",
        Ok(ValueTypeCode::Value) => "value",
        _ => "unknown",
    };
    println!(
        "{}[{}]=>('{}' @ {} : {})",
        prefix,
        encode_bytes_to_hex(ikey.get_contents()),
        encode_bytes_to_hex(user_key),
        seq,
        ty
    );
}

fn show_block_handle(prefix: &str, block_handle: &BlockHandle) {
    let mut content = Vec::new();
    block_handle.encode_to(&mut content);
    println!(
        "{}[{}]=>block_handle {{ .offset: {}, .size: {}}}",
        prefix,
        encode_bytes_to_hex(&content),
        block_handle.offset,
        block_handle.size
    );
}

fn show_version_edit(prefix: &str, ve: &VersionEdit) {
    if let Some(comparator) = &ve.comparator {
        print!("{}comparator:          ", prefix);
        show_human_readable("", comparator);
    }
    if let Some(log_number) = ve.log_number {
        println!("{}log_number:          {}", prefix, log_number);
    }
    if let Some(prev_log_number) = ve.prev_log_number {
        println!("{}prev_log_number:     {}", prefix, prev_log_number);
    }
    if let Some(next_file_number) = ve.next_file_number {
        println!("{}next_file_number:    {}", prefix, next_file_number);
    }
    if let Some(last_sequence) = ve.last_sequence {
        println!("{}last_sequence:       {}", prefix, last_sequence);
    }
    for (level, key) in &ve.compact_pointers {
        print!("{}compact_pointer[{}]:  ", prefix, level);
        show_internal_key("", key);
    }
    for (level, file) in &ve.deleted_files {
        println!("{}deleted_file[{}]:    {}", prefix, level, file);
    }
    for (level, file) in &ve.new_files {
        println!("{}new_file[level={}]:", prefix, level);
        println!("{}    number:    {}", prefix, file.number);
        println!("{}    file_size: {}", prefix, file.file_size);
        show_internal_key(&format!("{}    smallest: ", prefix), &file.smallest);
        show_internal_key(&format!("{}    largest:  ", prefix), &file.largest);
    }
}

fn main() -> anyhow::Result<()> {
    let args = EspikeyTool::parse();

    let file = OpenOptions::new().read(true).open(&args.file)?;
    match Mode::from_file_name(args.file.to_str().unwrap()) {
        Mode::Table => {
            println!("sstable");
            let mut buf = [0; FOOTER_ENCODED_LENGTH];
            let file_size = file.metadata()?.len();
            file.read_at(&mut buf, file_size - FOOTER_ENCODED_LENGTH as u64)?;

            let footer = Footer::decode_from(&buf)?;

            let mut scratch = Vec::new();
            let meta_index_block =
                espikey::table::read_block(&file, &footer.metaindex_handle, &mut scratch)?;

            let mut scratch = Vec::new();
            let index_block =
                espikey::table::read_block(&file, &footer.index_handle, &mut scratch)?;

            println!("data block(accessed by index): ");
            let block = Block::new(index_block).unwrap();
            for (i, (_, value)) in block.iter().enumerate() {
                let mut scratch = Vec::new();
                let (block_handle, _) = BlockHandle::decode_from(value)?;
                let block = espikey::table::read_block(&file, &block_handle, &mut scratch)?;
                let block = Block::new(block).unwrap();

                println!("=== block#{} (offset={}, size={}) ===", i, block_handle.offset, block_handle.size);
                for (key, value) in block.iter() {
                    let ikey = InternalKey::decode_from(&key);
                    show_internal_key("        key:   ", &ikey);
                    show_human_readable("        value: ", value);
                }
                println!();
            }

            println!("meta index block: ");
            let block = Block::new(meta_index_block).unwrap();
            for (key, value) in block.iter() {
                show_human_readable("    key:   ", &key);
                show_human_readable("    value: ", value);
            }

            println!("index block: ");
            let block = Block::new(index_block).unwrap();
            for (key, value) in block.iter() {
                let ikey = InternalKey::decode_from(&key);
                let (block_handle, _) = BlockHandle::decode_from(value)?;
                show_internal_key("    key(index): ", &ikey);
                show_block_handle("    value:      ", &block_handle);
            }

            println!("footer: ");
            println!(
                "    meta index handle: offset={}\tsize={}",
                footer.metaindex_handle.offset, footer.metaindex_handle.size
            );
            println!(
                "    index handle:      offset={}\tsize={}",
                footer.index_handle.offset, footer.index_handle.size
            );
        }
        Mode::Manifest => {
            println!("manifest(versino-edit)");
            let mut reader = log::Reader::new(file);
            while let Some(entry) = reader.read()? {
                println!("length: {}", entry.len());
                let ve = VersionEdit::decode_from(&entry)?;
                show_version_edit("\t", &ve);
            }
        }
        Mode::Wal => {
            println!("wal");
            let mut reader = log::Reader::new(file);
            while let Some(entry) = reader.read()? {
                let wb = WriteBatch::from(entry)?;
                println!("sequence: {}, count: {}", wb.get_sequence(), wb.get_count());
                for item in wb.iter() {
                    match item {
                        Ok(item) => match item {
                            espikey::ValueType::Deletion(key) => {
                                println!("delete");
                                print!("\tkey: ");
                                show_human_readable("\t", key);
                            }
                            espikey::ValueType::Value(key, value) => {
                                println!("put");
                                print!("\tkey: ");
                                show_human_readable("\t", key);
                                print!("\tvalue: ");
                                show_human_readable("\t", value);
                            }
                        },
                        Err(e) => {
                            println!("\tError: {:?}", e);
                        }
                    }
                }
            }
        }
    }
    Ok(())
}
