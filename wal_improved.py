
import json
import os
import threading
import time
from typing import Any, Dict, Optional
import fcntl

from constants import LogEntry, OperationType
from data_store import SimpleDataStore


class ImprovedWriteAheadLog:
    def __init__(self, log_file_path: str):
        self.log_file_path = log_file_path
        self.lock = threading.RLock()
        self._ensure_log_file_exists()
        self.sequence_counter = self._get_last_sequence_number()
    

    def _ensure_log_file_exists(self):
        try:
            with open(self.log_file_path, 'x'):
                pass
        except FileExistsError:
            pass
    
    def _get_last_sequence_number(self) -> int:
        max_seq = 0
        try:
            self.read_all_entries()
            max_seq = max(entry.sequence_number for entry in self.read_all_entries())
        except Exception as e:
            print(f"Error reading log file for last sequence number: {e}")
        return max_seq
    
    def write_log_entry(self, transaction_id: str, operation_type: OperationType, key: str, old_value: Optional[Any], new_value: Optional[Any]) -> int:
        """Thread safe log entry writing.
        We consider 2 types of locks:
        - Reentrant Lock: For same thread re-entrance.
        - File Locking: Process level locking
        """

        with self.lock:
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
                try:
                    # Acquire an exclusive lock on the file
                    fcntl.flock(f.fileno(), fcntl.LOCK_EX) # type: ignore
                    f.write(entry_json + '\n')
                    f.flush()   # Ensure it's written to disk
                    os.fsync(f.fileno())  # Force OS to write to disk
                finally:
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN) # type: ignore
        return log_entry.sequence_number
    
    def read_all_entries(self):
        entries = []
        if not os.path.exists(self.log_file_path):
            return entries
        
        with open(self.log_file_path, 'r') as f:
            try:
                # Read lock over file
                fcntl.flock(f.fileno(), fcntl.LOCK_SH) # type: ignore
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
            finally:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN) # type: ignore
    
        return entries
    
    def recover_datastore(self, datastore: SimpleDataStore) -> Dict[str, Any]:
        """Recovery logic:
        - Replay all log entries in order
        - Log entries that cannot be parsed are skipped with a warning
        - Keep track of transactions -- Check proper handling of incomplete transactions
        """

        entries = self.read_all_entries()

        recovery_stats = {
            'total_entries': len(entries),
            'operations_applied': 0,
            'errors': []
        }

        # Sort entries by sequence number for correct order
        entries.sort(key=lambda x: x.sequence_number)

        for entry in entries:
            try:
                if entry.operation_type == OperationType.INSERT.value:
                    datastore.put(entry.key, entry.new_value)
                elif entry.operation_type == OperationType.UPDATE.value:
                    datastore.put(entry.key, entry.new_value)
                elif entry.operation_type == OperationType.DELETE.value:
                    datastore.delete(entry.key)
                
                recovery_stats['operations_applied'] += 1
            except Exception as e:
                error_msg = f"Error applying log entry {entry.sequence_number}: {e}"
                recovery_stats['errors'].append(error_msg)
                print(error_msg)
        
        return recovery_stats
