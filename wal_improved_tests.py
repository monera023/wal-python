
import random
import tempfile
import unittest
import os
import threading
import time

from transactional_data_store import TransactionalDataStore
from wal_improved import ImprovedWriteAheadLog


class ImproveWalTests(unittest.TestCase):

    def setUp(self):
        self.temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.log')
        self.temp_file.close()  # Close the file so WAL can open it
        self.log_path = self.temp_file.name
        self.wal = ImprovedWriteAheadLog(self.log_path)
        self.store = TransactionalDataStore(self.wal)
    
    def tearDown(self):
        if os.path.exists(self.log_path):
            os.unlink(self.log_path)
    
    def test_concurrent_transactions_consistency(self):
        """
        Writes and updates from multiple threads and ensures log integrity.
        """
        num_threads = 5
        operations_per_thread = 20

        def worker_thread(thread_id):
            for i in range(operations_per_thread):
                key = f"key-{thread_id}-{i}"
                value = {"thread": thread_id, "operation": i, "timestamp": time.time()}
                self.store.put(key, value)

                if i > 0 and random.random() < 0.3:
                    old_key = f"key-{thread_id}-{i-1}"
                    new_value = {"thread": thread_id, "operation": i-1, "timestamp": time.time()}
                    self.store.put(old_key, new_value)
        
        threads = []
        start_time = time.time()

        for thread_id in range(num_threads):
            thread = threading.Thread(target=worker_thread, args=(thread_id,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to finish
        for thread in threads:
            thread.join()
        
        end_time = time.time()
        print(f"All threads completed in {end_time - start_time:.2f} seconds")

        original_size = self.store.size()
        print(f"Datastore size before recovery: {original_size}")

        new_wal = ImprovedWriteAheadLog(self.log_path)
        recovered_store = TransactionalDataStore(new_wal)
        stats = recovered_store.recover_from_log()

        print(f"Recovery stats: {stats}")
        recovered_size = recovered_store.size()
        self.assertEqual(recovered_size, original_size, f"Recovered size {recovered_size} doesn't match original size {original_size}")

        for thread in range(min(3, num_threads)):
            for i in range(min(5, operations_per_thread)):
                key = f"key-{thread_id}-{i}"
                original_value = self.store.get(key)
                recovered_value = recovered_store.get(key)
                self.assertEqual(original_value, recovered_value, f"Mismatch for key {key}")
    

    def test_recovery_after_partial_writes(self):

        self.store.put("user:1", {"name": "Rohit", "age": 28})
        self.store.put("user:2", {"name": "Rohan", "age": 32})

        # Manually added corrupted entry
        with open(self.log_path, 'a') as f:
            f.write('{"sequence_number":33, "transaction_id": "incomplete"' + '\n')  # Incomplete JSON
            f.flush()

        self.store.put("user:3", {"name": "Suresh", "age": 40})

        # Now do recovery
        new_wal = ImprovedWriteAheadLog(self.log_path)
        recovered_store = TransactionalDataStore(new_wal)
        stats = recovered_store.recover_from_log()

        print(f"Recovery stats after partial writes: {stats}")
        self.assertEqual(recovered_store.size(), 3)

        self.assertIsNotNone(recovered_store.get("user:1"))
        self.assertIsNotNone(recovered_store.get("user:2"))
        self.assertIsNotNone(recovered_store.get("user:3"))
    