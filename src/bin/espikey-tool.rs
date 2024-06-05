use std::fs::OpenOptions;
use std::path::PathBuf;

use clap::{Parser, ValueEnum};
use itertools::Itertools;

use espikey::version_edit::VersionEdit;
use espikey::write_batch::{ValueTypeCode, WriteBatch};
use espikey::{log, InternalKey};

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Mode {
    Raw,
    WriteBatch,
    VersionEdit,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about=None)]
#[clap(propagate_version = true)]
struct EspikeyTool {
    wal_file: PathBuf,

    #[clap(short, long, default_value = "write-batch")]
    mode: Mode,
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
        "{} [{}]=>('{}' @ {} : {})",
        prefix,
        encode_bytes_to_hex(ikey.get_contents()),
        encode_bytes_to_hex(user_key),
        seq,
        ty
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
        print!("{}compact_pointer[{}]: ", prefix, level);
        show_internal_key("", key);
    }
    for (level, file) in &ve.deleted_files {
        println!("{}deleted_file[{}]:    {}", prefix, level, file);
    }
    for (level, file) in &ve.new_files {
        println!("{}new_file[{}]:", prefix, level);
        println!("{}    number:    {}", prefix, file.number);
        println!("{}    file_size: {}", prefix, file.file_size);
        show_internal_key(&format!("{}    smallest: ", prefix), &file.smallest);
        show_internal_key(&format!("{}    largest:  ", prefix), &file.largest);
    }
}

fn main() -> anyhow::Result<()> {
    let args = EspikeyTool::parse();

    let wal_file = OpenOptions::new().read(true).open(&args.wal_file)?;

    let mut reader = log::Reader::new(wal_file);
    match args.mode {
        Mode::Raw => {
            println!("raw WAL records");
        }
        Mode::WriteBatch => {
            println!("WAL write batches");
        }
        Mode::VersionEdit => {
            println!("WAL version edit");
        }
    }
    while let Some(entry) = reader.read()? {
        match args.mode {
            Mode::Raw => {
                println!("length: {}", entry.len());
                show_wal_record(&entry);
                continue;
            }
            Mode::WriteBatch => {
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
            Mode::VersionEdit => {
                println!("length: {}", entry.len());
                let ve = VersionEdit::decode_from(&entry)?;
                show_version_edit("\t", &ve);
            }
        }
    }
    Ok(())
}
