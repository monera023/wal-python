
from wal_basic import WriteAheadLog, OperationType, LogEntry
import unittest
import tempfile
import os

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
        