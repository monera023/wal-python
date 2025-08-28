from constants import OperationType
from data_store import SimpleDataStore
from wal_improved import ImprovedWriteAheadLog
from typing import Dict, Any, Optional
import threading
import time
import os


class TransactionalDataStore:
    """
    A datastore that integrates with WAL for durability.
    Before write operations, it logs the operation to WAL.
    """

    def __init__(self, wal: ImprovedWriteAheadLog):
        self.wal = wal
        self.datastore = SimpleDataStore()
        self.transaction_counter = 0
        self.lock = threading.Lock()
    
    def _generate_transaction_id(self) -> str:
        with self.lock:
            self.transaction_counter += 1
            return f"txn_{self.transaction_counter}_{int(time.time())}"
    
    def put(self, key: str, value: Any) -> None:
        """
        Order of operations:
        1. Read current value
        2. Write to WAL (intention)
        3. Write to datastore (in-memory)

        Importance of order: If the system crashes after step 2 but before step 3,
        the WAL can be replayed to ensure the operation is not lost.
        """
        transaction_id = self._generate_transaction_id()
        # Step 1 read current value
        old_value = self.datastore.get(key)

        # Step 2 Write intention to WAL
        operation_type = OperationType.INSERT if old_value is None else OperationType.UPDATE
        self.wal.write_log_entry(transaction_id, operation_type, key, old_value, value)

        # Step 3 Write to in-memory datastore
        self.datastore.put(key, value)
    
    def get(self, key: str) -> Any:
        return self.datastore.get(key)
    
    def delete(self, key: str) -> bool:
        """
        Order of operations:
        1. Read current value
        2. Write to WAL (intention)
        3. Delete from datastore (in-memory)

        """
        transaction_id = self._generate_transaction_id()
        current_value = self.datastore.get(key)
        
        if current_value is None:
            return False  # Key doesn't exist
        
        self.wal.write_log_entry(transaction_id, OperationType.DELETE, key, current_value, None)

        self.datastore.delete(key)

        return True
    
    def size(self) -> int:
        return self.datastore.size()
    
    def recover_from_log(self) -> Dict[str, Any]:
        """Recover the in-memory datastore by replaying the WAL."""
        
        self.datastore = SimpleDataStore()  # Reset the datastore

        stats = self.wal.recover_datastore(self.datastore)

        return stats

if __name__ == "__main__":
    print(f"WAL recovery demo")
    print("-"*50)

    wal = ImprovedWriteAheadLog("demo.log")
    txn_data_store = TransactionalDataStore(wal)

    print(f"1. Initial datastore size: {txn_data_store.size()}")
    print(f"2. Adding some entries")

    txn_data_store.put("user:1", {"name": "Ashok", "age": 30})
    txn_data_store.put("user:2", {"name": "Surekha", "age": 25})
    txn_data_store.put("user:3", {"name": "Ravi", "age": 35})
    
    txn_data_store.put("user:1", {"name": "Ashok Kumar", "age": 30})  # Update
    txn_data_store.delete("user:2")  # Delete

    print(f"3. Datastore size after adds/updates/deletes: {txn_data_store.size()}")
    print(f"user:1 -> {txn_data_store.get('user:1')}")
    print(f"user:3 -> {txn_data_store.get('user:3')}")

    print(f"4. Simulating crash and recovery...")

    new_wal = ImprovedWriteAheadLog("demo.log")
    # recovered_datastore = TransactionalDataStore(new_wal)

    stats = txn_data_store.recover_from_log()

    print(f"Recovery stats: {stats}")
    print(f"5. Datastore size after recovery: {txn_data_store.size()}")
    print(f"user:1 -> {txn_data_store.get('user:1')}")
    print(f"user:2 -> {txn_data_store.get('user:2')}")
    print(f"user:3 -> {txn_data_store.get('user:3')}")

    if os.path.exists("demo.log"):
        os.remove("demo.log")