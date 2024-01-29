use serde::{Deserialize, Serialize};

use crate::{
    consts::consts::{EntityId, VersionId},
    database::table::{
        filter::QueryPersonData,
        row::{PersonVersion, UpdatePersonData},
    },
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
    List(Option<QueryPersonData>),
    /// Returns list of PersonVersion (version id, worldstate, tx_id, etc)
    ListLatestVersions,
}

impl Action {
    pub fn is_mutation(&self) -> bool {
        match self {
            Action::Add(_) | Action::Remove(_) | Action::Update(_, _) => true,
            Action::List(_)
            | Action::ListLatestVersions
            | Action::Get(_)
            | Action::GetVersion(_, _) => false,
        }
    }
}

// TODO: Is there a better way to type this? Like if we know we are going to get a SuccessStatus, we should be able to unwrap it
//  Note: the solution could be similiar to how we make the send_request method accept specific action types, and thus, return their corresponding response.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum ActionResult {
    /// Used for database status messages
    SuccessStatus(String),
    Single(Person),
    GetSingle(Option<Person>),
    List(Vec<Person>),
    ListVersion(Vec<PersonVersion>),
}

impl ActionResult {
    // TODO: Consider removing these methods and localizing them in the request_manager
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
            panic!("Success status code")
        }
    }
}
