use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub enum OperationType {
    INSERT,
    UPDATE,
    DELETE,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub sequence_number: u64,
    pub transaction_id: String,
    pub operation_type: OperationType,
    pub key: String,
    pub old_value: Option<Value>, // TODO: Change to generic type
    pub new_value: Option<Value>, // TODO: Change to generic type
    pub timestamp: f64,
}

impl LogEntry {
    pub fn new(
        sequence_number: u64,
        transaction_id: String,
        operation_type: OperationType,
        key: String,
        old_value: Option<Value>,
        new_value: Option<Value>,
    ) -> Self {
        LogEntry {
            sequence_number,
            transaction_id,
            operation_type,
            key,
            old_value,
            new_value,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs_f64(),
        }
    }
}