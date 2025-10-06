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
    pub fn new(log_file_path: &str) -> Result<Self> {
        let mut wal = WriteAheadLog {
            log_file_path: log_file_path.to_string(), // Creates a new String and just copies the value. The original &str remains valid. No change to it.
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

#[cfg(test)]
mod tests {

    use std::sync::Arc;
    use std::thread;
    use std::fs::OpenOptions;

    use tempfile::TempDir;

    use super::*; // Import everything from the outer scope
    // use std::fs;
    use serde_json::json;

    fn setup_wal() -> (TempDir, String, WriteAheadLog) {
        let temp_dir = TempDir::new().unwrap();
        let log_path = temp_dir.path().join("wal.log").to_string_lossy().to_string();
        let wal = WriteAheadLog::new(&log_path).unwrap(); // Ensure log_path is passed as &str
        (temp_dir, log_path, wal)
    }

    #[test]
    fn test_create_new_wal() {
        let (_temp_dir, log_path, wal) = setup_wal();
        println!("Log file path: {}", wal.log_file_path);
        println!("Log WAL PATH: {}", log_path);

        assert_eq!(wal.sequence_counter, 0);
        assert!(Path::new(&log_path).exists())
    }

    #[test]
    fn test_basic_log_writing() {
        let (_temp_dir, _log_path, mut wal) = setup_wal();
        let seq1 = wal.write_log_entry(
            "txn1".to_string(),
            OperationType::INSERT,
            "key1".to_string(),
            None,
            Some(json!({"name": "Ayush", "age": 30}))
        ).unwrap();

        let seq2 = wal.write_log_entry(
            "txn1".to_string(),
            OperationType::UPDATE,
            "key1".to_string(),
            Some(json!({"name": "Ayush", "age": 30})),
            Some(json!({"name": "Ayush Rai", "age": 31}))
        ).unwrap();

        assert_eq!(seq1, 1);
        assert_eq!(seq2, 2);

        let entries = wal.read_log_entries().unwrap();
        assert_eq!(entries.len(), 2);

        let first_entry = &entries[0];
        assert_eq!(first_entry.sequence_number, 1);
        assert_eq!(first_entry.transaction_id, "txn1");
        assert_eq!(first_entry.operation_type, OperationType::INSERT);
        assert_eq!(first_entry.key, "key1");
        assert_eq!(first_entry.old_value, None);
        assert_eq!(first_entry.new_value, Some(json!({"name": "Ayush", "age": 30})));

        assert!(first_entry.sequence_number < entries[1].sequence_number, "Sequence numbers should be increasing");

    }

    #[test]
    fn test_persistance_across_instances() {
        let (_temp_dir, log_path, mut wal) = setup_wal();
        wal.write_log_entry(
            "txn1".to_string(),
            OperationType::INSERT,
            "key1".to_string(),
            None,
            Some(json!({"name": "Shikhar"}))
        ).unwrap();

        wal.write_log_entry(
            "txn2".to_string(),
            OperationType::INSERT,
            "key2".to_string(),
            None,
            Some(json!({"name": "Rohit"}))
        ).unwrap();


        let new_wal = WriteAheadLog::new(&log_path).unwrap();

        let entries = new_wal.read_log_entries().unwrap();
        assert_eq!(entries.len(), 2);

    }

    #[test]
    fn test_empty_log_file() {
        let (_temp_dir, _log_path, wal) = setup_wal();
        let entries = wal.read_log_entries().unwrap();
        assert_eq!(entries.len(), 0);
    }

    #[test]
    fn test_corrupted_log_entry() {
        let (_temp_dir, log_path, mut wal) = setup_wal();

        wal.write_log_entry(
            "txn1".to_string(),
            OperationType::INSERT,
            "key1".to_string(),
            None,
            Some(json!({"name": "Shekhar"}))
        ).unwrap();

        // Manually corrupt log file
        {
            let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(&log_path)
            .unwrap();
            
            writeln!(file, "invalid entry").unwrap();
        }

        wal.write_log_entry(
            "txn2".to_string(),
            OperationType::UPDATE,
            "key1".to_string(),
            Some(json!({"name": "Shekhar"})),
            Some(json!({"name": "Shekhar Tipre"}))
        ).unwrap();

        let entries = wal.read_log_entries().unwrap();
        assert_eq!(entries.len(), 2); // Only valid entries should be counted

    }

    #[test]
    fn test_concurrent_writes() {
        let (_temp_dir, _log_path, wal) = setup_wal();
        let num_threads = 10;
        let entries_per_thread = 5;
        let wal_arc = Arc::new(wal);

        let mut handles = vec![];

        for thread_id in 0..num_threads {
            let wal_clone = Arc::clone(&wal_arc);
            let handle = thread::spawn(move || {

                let wal_ptr = Arc::as_ptr(&wal_clone) as *mut WriteAheadLog;

                for _i in 0..entries_per_thread {
                    unsafe {
                        (*wal_ptr).write_log_entry(
                            format!("txn_thread_{}", thread_id),
                            OperationType::INSERT,
                            format!("key_thread_{}", thread_id),
                            None,
                            Some(json!({"thread_id": thread_id, "entry": 1}))
                        ).unwrap();
                    }
                }
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        println!("All threads completed.");
        let entries = wal_arc.read_log_entries().unwrap();
        if entries.len() != num_threads * entries_per_thread {
            println!("Expected {} entries, found {}", num_threads * entries_per_thread, entries.len());
        }

        use std::collections::HashSet;

        let sequence_numbers: HashSet<_> = entries.iter()
            .map(|entry| entry.sequence_number)
            .collect();

        if sequence_numbers.len() != entries.len() {
            println!("Duplicate sequence numbers found!");
        }
    }
}