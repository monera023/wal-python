use anyhow::{Result, Context};
use serde_json::json;
use wal_rust::WriteAheadLog;
use wal_rust::constants::OperationType;

fn main() -> Result<()> {
    println!("Hello, world for KV Store!");

    let wal_path = String::from("wal.log");

    let mut wal = WriteAheadLog::new(wal_path)
        .context("Failed to initialize Write-Ahead Log")?;

    wal.write_log_entry("transaction_id".to_string(),
     OperationType::INSERT,
      "key1".to_string(),
       None, Some(json!({"name": "Anil", "age": 25})))
        .context("Failed to write log entry")?;

    wal.write_log_entry("transaction_id".to_string(), OperationType::UPDATE, "key1".to_string(), Some(json!({"name": "Anil", "age": 25})), Some(json!({"name": "Anil Kumar", "age": 25})))
        .context("Failed to write log entry")?;

    let entries = wal.read_log_entries()
        .context("Failed to read log entries")?;

    for entry in entries {
        println!("{:?}", entry);
    }

    Ok(())
    
    
}
