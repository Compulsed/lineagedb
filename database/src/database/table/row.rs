use serde::{Deserialize, Serialize};

use crate::{
    consts::consts::{EntityId, TransactionId, VersionId},
    model::person::Person,
};

use super::table::ApplyErrors;

#[derive(Debug)]
pub struct ApplyUpdateResult {
    pub previous: Person,
    pub current: Person,
}

#[derive(Debug)]
pub struct ApplyDeleteResult {
    pub previous: Person,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct UpdatePersonData {
    pub full_name: UpdateStatement,
    pub email: UpdateStatement,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum UpdateStatement {
    Set(String),
    Unset,
    NoChanges,
}

/// Used to clean up the table if there are no versions left
// I think it is better to have a non-optional version, and then all other versions captured in a vector
pub enum DropRow {
    VersionExist,
    NoVersionsExist,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum PersonVersionState {
    State(Person),
    Delete,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct PersonVersion {
    pub id: EntityId,
    pub state: PersonVersionState,
    pub version: VersionId, // Version Ids are re-indexed back to 1 on a restore
    pub transaction_id: TransactionId,
}

impl PersonVersion {
    pub fn get_person(&self) -> Option<Person> {
        match &self.state {
            PersonVersionState::State(person) => Some(person.clone()),
            PersonVersionState::Delete => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct PersonRow {
    /// Earliest versions are at beginning, latest version is last
    versions: Vec<PersonVersion>,
}

impl PersonRow {
    pub fn new(person: Person, transaction_id: TransactionId) -> Self {
        PersonRow {
            versions: vec![PersonVersion {
                id: person.id.clone(),
                state: PersonVersionState::State(person),
                version: VersionId::new_first_version(),
                transaction_id,
            }],
        }
    }

    /// Used when restoring from a snapshot
    pub fn from_restore(version: PersonVersion) -> Self {
        PersonRow {
            versions: vec![version],
        }
    }

    /// Handles the case of adding an item back after it was deleted
    pub fn apply_add(
        &mut self,
        person: Person,
        transaction_id: TransactionId,
    ) -> Result<(), ApplyErrors> {
        let current_version = self.current_version().clone();

        // Prevents adding an item that already exists
        if &current_version.state != &PersonVersionState::Delete {
            return Err(ApplyErrors::CannotCreateWhenAlreadyExists(
                person.id.clone(),
            ));
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
        id: &EntityId,
        update: UpdatePersonData,
        transaction_id: TransactionId,
    ) -> Result<ApplyUpdateResult, ApplyErrors> {
        let previous_version = self.current_version().clone();

        // Verify
        let previous_person = match previous_version.state.clone() {
            PersonVersionState::Delete => {
                return Err(ApplyErrors::CannotUpdateDoesNotExist(id.clone()))
            }
            PersonVersionState::State(s) => s,
        };

        let mut current_person = previous_person.clone();

        match &update.full_name {
            UpdateStatement::Set(full_name) => current_person.full_name = full_name.clone(),
            UpdateStatement::Unset => {
                return Err(ApplyErrors::NotNullConstraintViolation(
                    "Full Name".to_string(),
                ))
            }
            UpdateStatement::NoChanges => {}
        }

        match &update.email {
            UpdateStatement::Set(email) => current_person.email = Some(email.clone()),
            UpdateStatement::Unset => current_person.email = None,
            UpdateStatement::NoChanges => {}
        }

        // Apply
        self.apply_new_version(
            &previous_version,
            PersonVersionState::State(current_person.clone()),
            transaction_id,
        );

        Ok(ApplyUpdateResult {
            previous: previous_person,
            current: current_person,
        })
    }

    pub fn apply_delete(
        &mut self,
        id: &EntityId,
        transaction_id: TransactionId,
    ) -> Result<ApplyDeleteResult, ApplyErrors> {
        let current_version = self.current_version().clone();

        // Verify
        let previous_person = match current_version.clone().state {
            PersonVersionState::State(s) => s,
            PersonVersionState::Delete => {
                return Err(ApplyErrors::CannotDeleteDoesNotExist(id.clone()));
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
        transaction_id: TransactionId,
    ) {
        self.versions.push(PersonVersion {
            id: current_version.id.clone(),
            state: new_state,
            version: current_version.version.increment(),
            transaction_id,
        });
    }

    pub fn current_version(&self) -> &PersonVersion {
        // A row is always created with a version AND the row should be dropped if there are no versions (see: rollback_version)
        self.versions
            .last()
            .expect("Will always exist a current version, if not there is a bug")
    }

    pub fn current_state(&self) -> Option<Person> {
        self.current_version().get_person().clone()
    }

    /// Drop row means that we have rolled back to the point where there are no versions. We must clean up the row OR we
    ///    we will create bugs where we think a row exists when it does not
    pub fn rollback_version(&mut self) -> (PersonVersion, DropRow) {
        let version = self
            .versions
            .pop()
            .expect("should not be possible to rollback a person data without any versions");

        let drop_row = match self.versions.len() {
            0 => DropRow::NoVersionsExist,
            _ => DropRow::VersionExist,
        };

        return (version, drop_row);
    }

    pub fn person_at_version(&self, version_id: VersionId) -> Option<Person> {
        self.at_version(version_id)
            .and_then(|version| version.get_person())
    }

    pub fn at_version(&self, version_id: VersionId) -> Option<PersonVersion> {
        // Versions are 1 indexed, subtract 1 to get the correct vector index
        match self.versions.get(version_id.to_number() - 1) {
            Some(version) => Some(version.clone()),
            None => None,
        }
    }

    pub fn version_count(&self) -> usize {
        self.versions.len()
    }

    pub fn at_transaction_id(&self, transaction_id: &TransactionId) -> Option<Person> {
        // TODO: Can optimize this with a binary search
        for version in self.versions.iter().rev() {
            // May contain newer uncommited versions, we want to find the closest committed version
            if &version.transaction_id <= transaction_id {
                return version.get_person();
            }
        }

        None
    }

    pub fn version_at_transaction_id(
        &self,
        transaction_id: &TransactionId,
    ) -> Option<PersonVersion> {
        // Can optimize this with a binary search
        for version in self.versions.iter().rev() {
            // May contain newer uncommited versions, we want to find the closest committed version
            if &version.transaction_id <= transaction_id {
                return Some(version.clone());
            }
        }

        None
    }
}
