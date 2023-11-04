use crate::row::row::UpdatePersonData;

use super::person::Person;

#[derive(Clone, Debug)]
pub enum Action {
    Add(Person),
    Update(String, UpdatePersonData),
    Remove(String),
    Get(String),
    GetVersion(String, usize),
    List(usize),
}

impl Action {
    pub fn is_mutation(&self) -> bool {
        match self {
            Action::Add(_) | Action::Remove(_) | Action::Update(_, _) => true,
            Action::List(_) | Action::Get(_) | Action::GetVersion(_, _) => false,
        }
    }
}
