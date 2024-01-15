use serde::{Deserialize, Serialize};

use crate::consts::consts::EntityId;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Person {
    pub id: EntityId,
    pub full_name: String,
    pub email: Option<String>,
}
