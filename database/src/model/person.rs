use serde::{Deserialize, Serialize};

use crate::consts::consts::EntityId;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Person {
    pub id: EntityId,
    pub full_name: String,
    pub email: Option<String>,
}

impl Person {
    pub fn new(full_name: String, email: Option<String>) -> Self {
        Person {
            id: EntityId(uuid::Uuid::new_v4().to_string()),
            full_name,
            email,
        }
    }

    pub fn new_test() -> Self {
        Person {
            id: EntityId("1".to_string()),
            full_name: "Full Name".to_string(),
            email: Some("Email".to_string()),
        }
    }
}
