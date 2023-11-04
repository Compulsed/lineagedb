use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Person {
    pub id: String,
    pub full_name: String,
    pub email: Option<String>,
}
