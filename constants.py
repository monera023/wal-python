from dataclasses import dataclass
from enum import Enum
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