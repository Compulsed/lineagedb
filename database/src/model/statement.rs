use serde::{Deserialize, Serialize};

use crate::{
    consts::consts::{EntityId, VersionId},
    database::table::{
        query::QueryPersonData,
        row::{PersonVersion, UpdatePersonData},
    },
};

use super::person::Person;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Statement {
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

impl Statement {
    pub fn is_query(&self) -> bool {
        !self.is_mutation()
    }

    pub fn is_mutation(&self) -> bool {
        match self {
            Statement::Add(_) | Statement::Remove(_) | Statement::Update(_, _) => true,
            Statement::List(_)
            | Statement::ListLatestVersions
            | Statement::Get(_)
            | Statement::GetVersion(_, _) => false,
        }
    }
}

// TODO: Is there a better way to type this? Like if we know we are going to get a SuccessStatus, we should be able to unwrap it
//  Note: the solution could be similiar to how we make the send_request method accept specific statement types, and thus, return their corresponding response.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum StatementResult {
    /// Used for database status messages
    SuccessStatus(String),
    Single(Person),
    GetSingle(Option<Person>),
    List(Vec<Person>),
    ListVersion(Vec<PersonVersion>),
}

impl StatementResult {
    // TODO: Consider removing these methods and localizing them in the request_manager
    pub fn single(self) -> Person {
        if let StatementResult::Single(p) = self {
            p
        } else {
            panic!("Statement result is not of type Single")
        }
    }

    pub fn get_single(self) -> Option<Person> {
        if let StatementResult::GetSingle(p) = self {
            p
        } else {
            panic!("Statement result is not of type GetSingle")
        }
    }

    pub fn list(self) -> Vec<Person> {
        if let StatementResult::List(l) = self {
            l
        } else {
            panic!("Statement result is not of type List")
        }
    }

    #[allow(dead_code)]
    pub fn list_version(self) -> Vec<PersonVersion> {
        if let StatementResult::ListVersion(p) = self {
            p
        } else {
            panic!("Statement result is not of type ListVersion")
        }
    }

    #[allow(dead_code)]
    pub fn success_status(self) -> String {
        if let StatementResult::SuccessStatus(s) = self {
            s
        } else {
            panic!("Success status code")
        }
    }
}
