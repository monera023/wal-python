
import time
from constants import OperationType
from wal_basic import WriteAheadLog
import unittest
import tempfile
import os
import threading

class TestWriteAheadLog(unittest.TestCase):

    def setUp(self):
        """Set up a fresh WAL instance and log file for each test."""
        self.temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.log')
        self.temp_file.close()  # Close the file so WAL can open it
        self.log_path = self.temp_file.name
        self.wal = WriteAheadLog(self.log_path)
    
    def tearDown(self):
        """Clean up the temporary log file after each test."""
        if os.path.exists(self.log_path):
            os.unlink(self.log_path)
    
    def test_basic_log_writing(self):

        seq1 = self.wal.write_log_entry("tx1", OperationType.INSERT, "key1", None, "value1")
        seq2 = self.wal.write_log_entry("txn1", OperationType.UPDATE, "key1", "value1", "value2")
        seq3 = self.wal.write_log_entry("txn1", OperationType.DELETE, "key2", "old_value", None)

        self.assertEqual(seq1, 1)
        self.assertEqual(seq2, 2)
        self.assertEqual(seq3, 3)

        # Read all entries and verify
        entries = self.wal.read_all_entries()
        self.assertEqual(len(entries), 3)

        entry_1 = entries[0]
        self.assertEqual(entry_1.sequence_number, 1)
        self.assertEqual(entry_1.transaction_id, "tx1")
        self.assertEqual(entry_1.operation_type, OperationType.INSERT.value)
        self.assertEqual(entry_1.key, "key1")
        self.assertIsNone(entry_1.old_value, None)
        self.assertEqual(entry_1.new_value, "value1")

        self.assertTrue(entries[0].sequence_number < entries[1].sequence_number < entries[2].sequence_number)
    

    def test_persistence_across_instances(self):

        # Write entries with the first instances
        self.wal.write_log_entry("tx1", OperationType.INSERT, "key1", None, {"name": "Alice"})
        self.wal.write_log_entry("txn1", OperationType.UPDATE, "key1", {"name": "Alice"}, {"name": "Alice Smith"})

        new_wal = WriteAheadLog(self.log_path)
        entries = new_wal.read_all_entries()
        
        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0].key, "key1")
        self.assertEqual(entries[1].new_value, {"name": "Alice Smith"})

        seq3 = new_wal.write_log_entry("txn2", OperationType.INSERT, "key2", None, {"name": "Bob"})
        new_entries = new_wal.read_all_entries()
        self.assertEqual(len(new_entries), 3)
    

    def test_empty_log_file(self):
        entries = self.wal.read_all_entries()
        self.assertEqual(len(entries), 0)
    

    def test_corrupted_log_entry(self):
        # Write a valid entry
        self.wal.write_log_entry("txn1", OperationType.INSERT, "key1", None, {"name": "Anil"})

        with open(self.log_path, 'a') as f:
            f.write(f"This is a corrupted log entry\n")
            f.flush()
            os.fsync(f.fileno())


        # Write another valid entry
        self.wal.write_log_entry("txn2", OperationType.UPDATE, "key1", {"name": "Anil"}, {"name": "Anil Kumar"})

        entries = self.wal.read_all_entries()
        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0].key, "key1")
        self.assertEqual(entries[1].key, "key1")
    

    def test_concurrent_writes(self):
        num_threads = 10
        entries_per_thread = 100

        def write_entries(thread_id):
            for i in range(entries_per_thread):
                # print(f"Writing entries from thread {thread_id}")
                self.wal.write_log_entry(f"txn_{thread_id}", OperationType.INSERT, f"key_{thread_id}_{i}", None, f"value_{thread_id}_{i}")
            
        threads = []
        for t_id in range(num_threads):
            thread = threading.Thread(target=write_entries, args=(t_id,))
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
        
        print(f"All threads have finished writing.")
        # Checking results
        entries = self.wal.read_all_entries()
        expected_count = num_threads * entries_per_thread
        print(f"Expected {expected_count} entries, found {len(entries)} entries.")

        self.assertEqual(len(entries), expected_count)

        # Check for unique sequence numbers
        sequence_numbers = set(entry.sequence_number for entry in entries)
        if len(sequence_numbers) != len(sequence_numbers):
            print("WARNING: Found duplicate sequence numbers - race condition detected!")
            
    
    def test_different_data_types(self):
        test_cases = [
            ('string_val', "ssss"),
            ('int_val', 1233),
            ('float_val', 123.456),
            ('list_val', [1, 2, 3, "four"]),
            ('dict_val', {"key": "value", "num": 42}),
            ('none_val', None),
            ('nested_val', {"list": [1, {"inner_key": "inner_value"}]})
        ]

        for key, value in test_cases:
            self.wal.write_log_entry("test_txn", OperationType.INSERT, key, None, value)

        # Read back and verify
        entries = self.wal.read_all_entries()
        self.assertEqual(len(entries), len(test_cases))

        for entry, (expected_key, expected_value) in zip(entries, test_cases):
            self.assertEqual(entry.key, expected_key)
            self.assertEqual(entry.new_value, expected_value)
    
    def test_large_entires(self):
        large_value = {"data": "x" * 10000, "numbers": list(range(1000))}

        start_time = time.time()
        self.wal.write_log_entry("large_txn", OperationType.INSERT, "large_key", None, large_value)
        end_time = time.time()

        print(f"Time taken to write large entry: {(end_time - start_time) * 1000} milliseconds")

        # Read entry
        read_start_time = time.time()
        entries = self.wal.read_all_entries()
        read_end_time = time.time()

        print(f"Time taken to read entry: {(read_end_time - read_start_time) * 1000} milliseconds")
        self.assertEqual(len(entries), 1)
    

