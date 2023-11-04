use serde::{Deserialize, Serialize};

use crate::{
    consts::consts::{ErrorString, START_AT_INDEX},
    model::person::Person,
};

#[derive(Debug)]
pub struct ApplyUpdateResult {
    pub previous: Person,
}

#[derive(Debug)]
pub struct ApplyDeleteResult {
    pub previous: Person,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct UpdatePersonData {
    pub full_name: UpdateAction,
    pub email: UpdateAction,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum UpdateAction {
    Set(String),
    Unset,
    NoChanges,
}

#[derive(Clone, Debug, PartialEq)]
pub enum PersonVersionState {
    State(Person),
    Delete,
}

#[derive(Clone, Debug)]
pub struct PersonVersion {
    pub state: PersonVersionState,
    pub version: usize,
    pub transaction_id: usize,
}

impl PersonVersion {
    pub fn get_person(&self) -> Option<Person> {
        match &self.state {
            PersonVersionState::State(person) => Some(person.clone()),
            PersonVersionState::Delete => None,
        }
    }
}

#[derive(Debug)]
pub struct PersonRow {
    /// Earliest versions are at beginning, latest version is last
    pub versions: Vec<PersonVersion>,
}

impl PersonRow {
    pub fn new(person: Person, transaction_id: usize) -> Self {
        PersonRow {
            versions: vec![PersonVersion {
                state: PersonVersionState::State(person),
                version: START_AT_INDEX,
                transaction_id,
            }],
        }
    }

    /// Handles the case of adding an item back after it was deleted
    pub fn apply_add(&mut self, person: Person, transaction_id: usize) -> Result<(), ErrorString> {
        let current_version = self.current_version();

        // Verify
        if &current_version.state != &PersonVersionState::Delete {
            return Err("Cannot add an item when it already exists".to_string());
        }

        // Apply
        self.apply_new_version(
            &current_version,
            PersonVersionState::State(person),
            transaction_id,
        );

        Ok(())
    }

    pub fn apply_update(
        &mut self,
        update: UpdatePersonData,
        transaction_id: usize,
    ) -> Result<ApplyUpdateResult, ErrorString> {
        let previous_version = self.current_version();

        // Verify
        if &previous_version.state == &PersonVersionState::Delete {
            return Err("Cannot update a deleted record".to_string());
        }

        let previous_person = match previous_version.state.clone() {
            PersonVersionState::Delete => return Err("Cannot update a deleted record".to_string()),
            PersonVersionState::State(s) => s,
        };

        let mut current_person = previous_person.clone();

        match &update.full_name {
            UpdateAction::Set(full_name) => current_person.full_name = full_name.clone(),
            UpdateAction::Unset => return Err("Full name cannot be set to null".to_string()),
            UpdateAction::NoChanges => {}
        }

        match &update.email {
            UpdateAction::Set(email) => current_person.email = Some(email.clone()),
            UpdateAction::Unset => current_person.email = None,
            UpdateAction::NoChanges => {}
        }

        // Apply
        self.apply_new_version(
            &previous_version,
            PersonVersionState::State(current_person),
            transaction_id,
        );

        Ok(ApplyUpdateResult {
            previous: previous_person,
        })
    }

    pub fn apply_delete(
        &mut self,
        transaction_id: usize,
    ) -> Result<ApplyDeleteResult, ErrorString> {
        let current_version = self.current_version();

        // Verify
        let previous_person = match current_version.clone().state {
            PersonVersionState::State(s) => s,
            PersonVersionState::Delete => {
                return Err("Cannot delete an already deleted record".to_string());
            }
        };

        // Apply
        self.apply_new_version(&current_version, PersonVersionState::Delete, transaction_id);

        Ok(ApplyDeleteResult {
            previous: previous_person,
        })
    }

    fn apply_new_version(
        &mut self,
        current_version: &PersonVersion,
        new_state: PersonVersionState,
        transaction_id: usize,
    ) {
        self.versions.push(PersonVersion {
            state: new_state,
            version: current_version.version + 1,
            transaction_id,
        });
    }

    pub fn current_version(&self) -> PersonVersion {
        self.versions
            .last()
            .expect("should not be possible to create a person data without any versions")
            .clone()
    }

    pub fn current_state(&self) -> Option<Person> {
        self.current_version().get_person()
    }

    pub fn at_version(&self, version_id: usize) -> Option<Person> {
        match self.versions.get(version_id) {
            Some(version) => version.get_person(),
            None => None,
        }
    }

    pub fn at_transaction_id(&self, transaction_id: usize) -> Option<Person> {
        // Can optimize this with a binary search
        for version in self.versions.iter().rev() {
            // May contain newer uncommited versions, we want to find the closest committed version
            if version.transaction_id <= transaction_id {
                return version.get_person();
            }
        }

        None
    }

    pub fn version_at_transaction_id(&self, transaction_id: usize) -> Option<PersonVersion> {
        // Can optimize this with a binary search
        for version in self.versions.iter().rev() {
            // May contain newer uncommited versions, we want to find the closest committed version
            if version.transaction_id <= transaction_id {
                return Some(version.clone());
            }
        }

        None
    }
}
