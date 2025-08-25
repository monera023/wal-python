from dataclasses import dataclass
from enum import Enum
import json
import os
import time
from typing import Any, Optional

class OperationType(Enum):
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


@dataclass
class LogEntry:
    """
    Significance of each field:
    - sequence_number: To ensure order and helps to replay events in the correct sequence.
    - transaction_id: To group multiple operations under a single transaction.
    - old_value: Needed for UNDO operations.
    - new_value: Needed for REDO operations.
    - timestamp: For auditing and tracking when the operation occurred.
    """
    sequence_number: int
    transaction_id: str
    operation_type: OperationType
    key: str
    old_value: Optional[Any]
    new_value: Optional[Any]
    timestamp: float

class WriteAheadLog:
    def __init__(self, log_file_path: str):
        self.log_file_path = log_file_path
        self.sequence_counter = 0
        self._ensure_log_file_exists()
    
    def _ensure_log_file_exists(self):
        try:
            with open(self.log_file_path, 'x'):
                pass
        except FileExistsError:
            pass
    
    def write_log_entry(self, transaction_id: str, operation_type: OperationType, key: str, old_value: Optional[Any], new_value: Optional[Any]) -> int:
        self.sequence_counter += 1
        log_entry = LogEntry(
            sequence_number=self.sequence_counter,
            transaction_id=transaction_id,
            operation_type=operation_type,
            key=key,
            old_value=old_value,
            new_value=new_value,
            timestamp=time.time()
        )
        # Convert to json string for storage
        entry_json = json.dumps({
            'sequence_number': log_entry.sequence_number,
            'transaction_id': log_entry.transaction_id,
            'operation_type': log_entry.operation_type.value,
            'key': log_entry.key,
            'old_value': log_entry.old_value,
            'new_value': log_entry.new_value,
            'timestamp': log_entry.timestamp
        })

        with open(self.log_file_path, 'a') as f:
            f.write(entry_json + '\n')
            f.flush()   # Ensure it's written to disk
            os.fsync(f.fileno())  # Force OS to write to disk
        
        return log_entry.sequence_number
    
    def read_all_entries(self):
        entries = []
        with open(self.log_file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                try:
                    entry_data = json.loads(line)
                    entry = LogEntry(**entry_data)
                    entries.append(entry)
                except (json.JSONDecodeError, ValueError, KeyError) as e:
                    print(f"Warning: Skipping malformed log entry: {line}. Error: {e}")
        
        return entries


if __name__ == "__main__":
    wal = WriteAheadLog("wal.log")
    seq1 = wal.write_log_entry("txn1", OperationType.INSERT, "user:123", None, {"name": "Alice"})
    seq2 = wal.write_log_entry("txn1", OperationType.UPDATE, "user:123", {"name": "Alice"}, {"name": "Alice Smith"})

    # read entries
    entries = wal.read_all_entries()
    print(f"Found {len(entries)} log entries:")
    for entry in entries:
        print(entry)
