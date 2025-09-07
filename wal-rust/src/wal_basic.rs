use std::fs::{File, OpenOptions};
use std::path::Path;
use anyhow::{Result, Context};
use serde_json::Value;
use std::io::{BufRead, BufReader, Write};

use crate::constants::{OperationType, LogEntry};


pub struct WriteAheadLog {
    log_file_path: String,
    sequence_counter: u64,
}

impl WriteAheadLog {
    pub fn new(log_file_path: String) -> Result<Self> {
        let mut wal = WriteAheadLog {
            log_file_path: log_file_path.clone(),
            sequence_counter: 0,
        };
        wal.ensure_log_file_exists()?;
        wal.sequence_counter = 0;
        Ok(wal)
    }

    pub fn ensure_log_file_exists(&self) -> Result<()> {
        if !Path::new(&self.log_file_path).exists() {
            File::create(&self.log_file_path).context("Failed to create log file")?;
        }
        Ok(())
    }

    pub fn write_log_entry(
        &mut self,
        transaction_id: String,
        operation_type: OperationType,
        key: String,
        old_value: Option<Value>,
        new_value: Option<Value>,
    ) -> Result<u64> {
        self.sequence_counter += 1;
        
        let log_entry = LogEntry::new(
            self.sequence_counter,
            transaction_id,
            operation_type,
            key,
            old_value,
            new_value
        );

        // Serialize log entry to JSON
        let entry_json = serde_json::to_string(&log_entry).context("Failed to serialize log entry")?;

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_file_path)
            .context("Failed to open log file")?;

        writeln!(file, "{}", entry_json)
            .context("Failed to write log entry to file")?;

        file.sync_all()
        .context("Failed to sync log file")?;
        
        Ok(self.sequence_counter)

    }

    pub fn read_log_entries(&self) -> Result<Vec<LogEntry>> {
        let mut entries = Vec::new();

        if !Path::new(&self.log_file_path).exists() {
            return Ok(entries); // Return empty if file doesn't exist
        }

        let file = File::open(&self.log_file_path)
            .context("Failed to open log file for reading")?;

        let reader = BufReader::new(file);

        for line in reader.lines() {
            let line = line.context("Failed to read line from log file")?;

            let line = line.trim();
            if line.is_empty() {
                continue; // Skip empty lines
            }
            match serde_json::from_str::<LogEntry>(line) {
                Ok(entry) => entries.push(entry),
                Err(e) => eprintln!("Failed to parse log entry: {}. Error: {}", line, e),
            }

        }

        Ok(entries)
    }
    
}