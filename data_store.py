

from typing import Dict, Any, Optional
import threading


class SimpleDataStore:
    """A simple in-memory key-value data store."""

    def __init__(self):
        self.store: Dict[str, Any] = {}
        self.lock = threading.Lock()
    
    def get(self, key: str) -> Any:
        with self.lock:
            return self.store.get(key)
    
    def put(self, key: str, value: Any) -> Optional[Any]:
        """Returns the old value if key existed, else None."""
        with self.lock:
            old_value = self.store.get(key)
            self.store[key] = value
            return old_value
    
    def delete(self, key: str) -> Optional[Any]:
        """Returns the old value if key existed, else None."""
        with self.lock:
            return self.store.pop(key, None)
    
    def size(self) -> int:
        with self.lock:
            return len(self.store)
