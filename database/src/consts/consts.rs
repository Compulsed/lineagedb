use std::fmt;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

// New Type Pattern -- https://doc.rust-lang.org/rust-by-example/generics/new_types.html
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, PartialOrd)]
pub struct TransactionId(pub usize);

impl TransactionId {
    pub fn to_number(&self) -> usize {
        self.0
    }

    // TODO: There is a bug here, we should be able to start at 0
    pub fn new_first_transaction() -> TransactionId {
        TransactionId(1)
    }

    pub fn increment(&self) -> TransactionId {
        TransactionId(self.0 + 1)
    }
}

impl From<i32> for TransactionId {
    fn from(transaction_id: i32) -> TransactionId {
        TransactionId(transaction_id.try_into().unwrap())
    }
}

impl fmt::Display for TransactionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, PartialOrd)]
pub struct VersionId(pub usize);

impl VersionId {
    pub fn increment(&self) -> VersionId {
        VersionId(self.0 + 1)
    }

    pub fn to_number(self) -> usize {
        self.0
    }

    pub fn new_first_version() -> VersionId {
        VersionId(1)
    }
}

impl fmt::Display for VersionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Error, Debug)]
pub enum VersionIdVersionError {
    #[error("VersionId must be greater than 0, got {0}")]
    NegativeOrZero(i32),
    #[error("VersionId must be less than 65536, got {0}")]
    TooLarge(i32),
}

impl TryFrom<i32> for VersionId {
    type Error = VersionIdVersionError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        if value <= 0 {
            return Err(VersionIdVersionError::NegativeOrZero(value));
        }

        // TODO: Define the max a u16
        if value > u16::MAX as i32 {
            return Err(VersionIdVersionError::TooLarge(value));
        }

        Ok(VersionId(
            value
                .try_into()
                .expect("Validation should have caught this"),
        ))
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct EntityId(pub String);

impl EntityId {
    // TODO:
    //  - This is likely inefficient, we should implement a ref type
    pub fn to_string(&self) -> String {
        self.0.clone()
    }

    pub fn new() -> EntityId {
        EntityId(Uuid::new_v4().to_string())
    }
}

impl fmt::Display for EntityId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
