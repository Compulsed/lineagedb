use serde::{Deserialize, Serialize};

use crate::{
    consts::consts::{EntityId, VersionId},
    database::table::row::{PersonVersion, UpdatePersonData},
};

use super::person::Person;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Action {
    Add(Person),
    Update(EntityId, UpdatePersonData),
    Remove(EntityId),
    Get(EntityId),
    GetVersion(EntityId, VersionId),
    /// Returns a list of Person
    List,
    /// Returns list of PersonVersion (version id, worldstate, tx_id, etc)
    ListLatestVersions,
}

impl Action {
    pub fn is_mutation(&self) -> bool {
        match self {
            Action::Add(_) | Action::Remove(_) | Action::Update(_, _) => true,
            Action::List
            | Action::ListLatestVersions
            | Action::Get(_)
            | Action::GetVersion(_, _) => false,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum ActionResult {
    SuccessStatus(String),
    ErrorStatus(String),
    Single(Person),
    GetSingle(Option<Person>),
    List(Vec<Person>),
    ListVersion(Vec<PersonVersion>),
}

impl ActionResult {
    #[allow(dead_code)]
    pub fn error_status(self) -> String {
        if let ActionResult::ErrorStatus(s) = self {
            s
        } else {
            panic!("Action result is not of type Status")
        }
    }

    pub fn single(self) -> Person {
        if let ActionResult::Single(p) = self {
            p
        } else {
            panic!("Action result is not of type Single")
        }
    }

    pub fn get_single(self) -> Option<Person> {
        if let ActionResult::GetSingle(p) = self {
            p
        } else {
            panic!("Action result is not of type GetSingle")
        }
    }

    pub fn list(self) -> Vec<Person> {
        if let ActionResult::List(l) = self {
            l
        } else {
            panic!("Action result is not of type List")
        }
    }

    #[allow(dead_code)]
    pub fn list_version(self) -> Vec<PersonVersion> {
        if let ActionResult::ListVersion(p) = self {
            p
        } else {
            panic!("Action result is not of type ListVersion")
        }
    }

    #[allow(dead_code)]
    pub fn success_status(self) -> String {
        if let ActionResult::SuccessStatus(s) = self {
            s
        } else {
            panic!("Action result is not of type ListVersion")
        }
    }
}
