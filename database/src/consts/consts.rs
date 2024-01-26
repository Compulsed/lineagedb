use serde::{Deserialize, Serialize};
use uuid::Uuid;

// Types
pub type ErrorString = String;

// New Type Pattern -- https://doc.rust-lang.org/rust-by-example/generics/new_types.html
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, PartialOrd)]
pub struct TransactionId(pub usize);

impl TransactionId {
    pub fn to_number(self) -> usize {
        self.0
    }

    pub fn increment(&self) -> TransactionId {
        TransactionId(self.0 + 1)
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
}

pub enum VersionIdVersionError {
    NegativeOrZero(i32),
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

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct EntityId(pub String);

impl EntityId {
    // TODO: This is likely inefficient, we should implement a ref type
    pub fn to_string(&self) -> String {
        self.0.clone()
    }

    pub fn new() -> EntityId {
        EntityId(Uuid::new_v4().to_string())
    }
}

// Values
pub const START_AT_INDEX: VersionId = VersionId(1);
