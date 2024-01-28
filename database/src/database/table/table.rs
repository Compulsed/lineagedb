use core::panic;
use std::collections::HashMap;
use thiserror::Error;

use crate::{
    consts::consts::{EntityId, TransactionId, VersionId},
    model::{
        action::{Action, ActionResult},
        person::Person,
    },
};

use super::row::{
    ApplyDeleteResult, ApplyUpdateResult, PersonRow, PersonVersion, PersonVersionState,
    UpdateAction,
};

#[derive(Error, Debug)]
pub enum ApplyErrors {
    // CRUD - GET
    #[error("Not found, record does not exist: {0}")]
    CannotGetDoesNotExist(EntityId),

    #[error("Not found, record does not exist at version: {0}:{1}")]
    CannotGetAtVersionDoesNotExist(EntityId, VersionId),

    // CRUD - CREATE
    #[error("Cannot create, record already exists: {0}")]
    CannotCreateWhenAlreadyExists(EntityId),

    // CRUD - UPDATE
    #[error("Cannot Update, record does not exist: {0}")]
    CannotUpdateDoesNotExist(EntityId),

    // CRUD - DELETE
    #[error("Cannot delete, record does not exist: {0}")]
    CannotDeleteDoesNotExist(EntityId),

    // Constraints
    #[error("Cannot add row as a person already exists with this email: {0}")]
    UniqueConstraintViolation(String),

    #[error("Cannot set field to null: {0}")]
    NotNullConstraintViolation(String),
}

type RowPrimaryKey = String;

pub struct PersonTable {
    pub person_rows: HashMap<String, PersonRow>,
    pub unique_email_index: HashMap<String, String>,
}

impl PersonTable {
    pub fn new() -> Self {
        Self {
            person_rows: HashMap::<RowPrimaryKey, PersonRow>::new(),
            unique_email_index: HashMap::<String, RowPrimaryKey>::new(),
        }
    }

    // Each mutation action can be broken up into 3 steps
    //  - Verifying validity / constraints (uniqueness)
    //  - Applying action
    //  - Clean up
    pub fn apply(
        &mut self,
        action: Action,
        transaction_id: TransactionId,
    ) -> Result<ActionResult, ApplyErrors> {
        let action_result = match action {
            Action::Add(person) => {
                let id = person.id.clone();
                let person_to_persist = person.clone();

                if let Some(email) = &person.email {
                    // Check if a person with an email already exists
                    if self.unique_email_index.contains_key(email) {
                        return Err(ApplyErrors::UniqueConstraintViolation(email.clone()));
                    }
                }

                // We need to handle the case where someone can add an item back after it has been deleted
                //  if it has been deleted there will already be a row.
                match self.person_rows.get_mut(&id.to_string()) {
                    Some(existing_person_row) => {
                        existing_person_row.apply_add(person_to_persist, transaction_id)?;
                    }
                    None => {
                        self.person_rows.insert(
                            id.to_string(),
                            PersonRow::new(person_to_persist, transaction_id),
                        );
                    }
                }

                // Persist the email so it cannot be added again
                if let Some(email) = &person.email {
                    self.unique_email_index
                        .insert(email.clone(), person.id.to_string());
                }

                ActionResult::Single(person)
            }
            Action::Update(id, update_person) => {
                let person_row = self
                    .person_rows
                    .get_mut(&id.to_string())
                    .ok_or(ApplyErrors::CannotUpdateDoesNotExist(id.clone()))?;

                if let UpdateAction::Set(email_to_update) = &update_person.email {
                    let mut skip_check = false;

                    // Edge case: If we are updating the email to the same value, we don't need to check the uniqueness constraint
                    if let Some(previous_state) = person_row.versions.last() {
                        if let PersonVersionState::State(previous_person) = &previous_state.state {
                            if let Some(previous_email) = &previous_person.email {
                                if previous_email == email_to_update {
                                    skip_check = true;
                                }
                            }
                        }
                    }

                    if skip_check == false && self.unique_email_index.contains_key(email_to_update)
                    {
                        return Err(ApplyErrors::UniqueConstraintViolation(
                            email_to_update.clone(),
                        ));
                    }
                }

                let person_update_to_persist = update_person.clone();

                let ApplyUpdateResult { current, previous } =
                    person_row.apply_update(&id, person_update_to_persist, transaction_id)?;

                // Persist / remove email from index
                match (&update_person.email, &previous.email) {
                    (UpdateAction::Set(email), _) => {
                        self.unique_email_index
                            .insert(email.clone(), id.to_string());
                    }
                    (UpdateAction::Unset, Some(email)) => {
                        self.unique_email_index.remove(email);
                    }
                    _ => {}
                }

                ActionResult::Single(current)
            }
            Action::Remove(id) => {
                let person_row = self
                    .person_rows
                    .get_mut(&id.to_string())
                    .ok_or(ApplyErrors::CannotDeleteDoesNotExist(id.clone()))?;

                let ApplyDeleteResult { previous } =
                    person_row.apply_delete(&id, transaction_id)?;

                if let Some(email) = &previous.email {
                    self.unique_email_index.remove(email);
                }

                ActionResult::Single(previous)
            }
            Action::Get(id) => {
                let person = match &self.person_rows.get(&id.to_string()) {
                    Some(person_data) => person_data.current_state(),
                    None => return Err(ApplyErrors::CannotGetDoesNotExist(id)),
                };

                ActionResult::GetSingle(person)
            }
            Action::GetVersion(id, version) => {
                let person = match &self.person_rows.get(&id.to_string()) {
                    Some(person_data) => person_data.at_version(version),
                    None => return Err(ApplyErrors::CannotGetAtVersionDoesNotExist(id, version)),
                };

                ActionResult::GetSingle(person)
            }
            Action::List => {
                let people_at_transaction_id: Vec<Person> = self
                    .person_rows
                    .iter()
                    .filter_map(|(_, value)| value.at_transaction_id(&transaction_id))
                    .collect();

                ActionResult::List(people_at_transaction_id)
            }
            Action::ListLatestVersions => {
                let people_at_transaction_id: Vec<PersonVersion> = self
                    .person_rows
                    .iter()
                    .filter_map(|(_, value)| value.version_at_transaction_id(&transaction_id))
                    .collect();

                ActionResult::ListVersion(people_at_transaction_id)
            }
        };

        Ok(action_result)
    }

    pub fn apply_rollback(&mut self, action: Action) {
        match action {
            Action::Add(person) => {
                self.remove_mutation(person.id);
            }
            Action::Update(id, _) => {
                self.remove_mutation(id);
            }
            Action::Remove(id) => {
                self.remove_mutation(id);
            }
            Action::Get(_)
            | Action::GetVersion(_, _)
            | Action::List
            | Action::ListLatestVersions => {}
        }
    }

    fn remove_mutation(&mut self, id: EntityId) {
        let person_row = self
            .person_rows
            .get_mut(&id.to_string())
            .expect("should exist because there is a rollback");

        // Remove the version that was applied
        let person_version_to_remove = person_row
            .versions
            .pop()
            .expect("should exist because there is a rollback");

        match person_version_to_remove.state {
            PersonVersionState::State(person) => {
                if let Some(email) = person.email {
                    self.unique_email_index.remove(&email);
                }

                if person_row.versions.is_empty() {
                    self.person_rows.remove(&id.to_string());
                }
            }
            PersonVersionState::Delete => {
                let current_person = person_row
                    .versions
                    .last()
                    .expect("should exist because there is a rollback");

                if let PersonVersionState::State(person) = &current_person.state {
                    if let Some(email) = &person.email {
                        self.unique_email_index
                            .insert(email.clone(), id.to_string());
                    }
                } else {
                    panic!("delete should always be followed by a state");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod uniqueness_constraint {
        use crate::database::table::row::UpdatePersonData;

        use super::*;

        #[test]
        fn adding_item_with_same_email_as_existing_item_fails() {
            // Given a table with an that has a unique email
            let mut table = PersonTable::new();

            let person = Person::new("1".to_string(), Some("email".to_string()));
            let action = Action::Add(person);

            table
                .apply(action, TransactionId(1))
                .expect("should not throw an error because there is no data");

            // When we add an item with the same email
            let person = Person::new("2".to_string(), Some("email".to_string()));
            let action = Action::Add(person);

            let result = table
                .apply(action, TransactionId(2))
                .err()
                .expect("should error");

            assert!(matches!(result, ApplyErrors::UniqueConstraintViolation(_)));
        }

        #[test]
        fn adding_item_with_same_email_as_existing_item_after_deleting_existing_item_succeeds() {
            // Given an empty table
            let mut table = PersonTable::new();

            // When we add an item
            let person = Person::new("1".to_string(), Some("email".to_string()));
            let action = Action::Add(person.clone());
            table.apply(action, TransactionId(1)).unwrap();

            // And we delete the item
            let action = Action::Remove(person.id.clone());
            table.apply(action, TransactionId(2)).unwrap();

            // Then we can add another item with the same email
            let person = Person::new("2".to_string(), Some("email".to_string()));
            let action = Action::Add(person.clone());
            table.apply(action, TransactionId(3)).unwrap();
        }

        #[test]
        fn updating_item_value_to_itself_does_not_break_uniqueness_constraint() {
            // Given a table with an that has a unique email
            let mut table = PersonTable::new();

            let person = Person::new("1".to_string(), Some("email".to_string()));
            let add_action = Action::Add(person.clone());

            table
                .apply(add_action, TransactionId(1))
                .expect("should not throw an error because there is no table data");

            // When we update the item with the same email
            let update_action = Action::Update(
                person.id.clone(),
                UpdatePersonData {
                    full_name: UpdateAction::NoChanges,
                    email: UpdateAction::Set(person.email.clone().unwrap()),
                },
            );

            let result = table
                .apply(update_action, TransactionId(2))
                .expect("should not throw an error because the email is the same");

            assert_eq!(result, ActionResult::Single(person));
        }
    }
}
