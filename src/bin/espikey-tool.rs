use std::fs::OpenOptions;
use std::path::PathBuf;

use clap::Parser;
use itertools::Itertools;

use espikey::log;
use espikey::write_batch::WriteBatch;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about=None)]
#[clap(propagate_version = true)]
struct EspikeyTool {
    wal_file: PathBuf,

    #[clap(short, long)]
    raw: bool,
}

fn encode_bytes_to_hex(data: &[u8]) -> String {
    data.iter().map(|byte| format!("{:02x}", byte)).join(" ")
}

fn show_human_readable(prefix: &str, data: &[u8]) {
    print!("{}[{}]: ", prefix, encode_bytes_to_hex(data));
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

fn main() -> anyhow::Result<()> {
    let args = EspikeyTool::parse();

    let wal_file = OpenOptions::new().read(true).open(&args.wal_file)?;

    let mut reader = log::Reader::new(wal_file);
    if args.raw {
        println!("raw WAL records");
    } else {
        println!("WAL write batches");
    }
    while let Some(entry) = reader.read()? {
        if args.raw {
            println!("length: {}", entry.len());
            show_wal_record(&entry);
            continue;
        } else {
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
    Ok(())
}
