use std::collections::HashMap;

use crate::{
    consts::consts::{ErrorString, TransactionId},
    model::{
        action::{Action, ActionResult},
        person::Person,
    },
};

use super::row::{ApplyDeleteResult, ApplyUpdateResult, PersonRow, PersonVersion, UpdateAction};

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
    ) -> Result<ActionResult, ErrorString> {
        let action_result = match action {
            Action::Add(person) => {
                let id = person.id.clone();
                let person_to_persist = person.clone();

                if let Some(email) = &person.email {
                    // Check if a person with an email already exists
                    if self.unique_email_index.contains_key(email) {
                        return Err(format!(
                            "Cannot add row as a person already exists with this email: {}",
                            email
                        ));
                    }
                }

                // We need to handle the case where someone can add an item back after it has been deleted
                //  if it has been deleted there will already be a row
                match self.person_rows.get_mut(&id) {
                    Some(existing_person_row) => {
                        existing_person_row.apply_add(person_to_persist, transaction_id)?;
                    }
                    None => {
                        self.person_rows
                            .insert(id, PersonRow::new(person_to_persist, transaction_id));
                    }
                }

                // Persist the email so it cannot be added again
                if let Some(email) = &person.email {
                    self.unique_email_index
                        .insert(email.clone(), person.id.clone());
                }

                ActionResult::Single(person)
            }
            Action::Update(id, update_person) => {
                let person_update_to_persist = update_person.clone();

                let person_row = self
                    .person_rows
                    .get_mut(&id)
                    .ok_or(format!("Cannot update record [id: {}], does not exist", id))?;

                if let UpdateAction::Set(email_to_update) = &update_person.email {
                    if self.unique_email_index.contains_key(email_to_update) {
                        return Err(format!(
                            "Cannot update row as person already exists with this email: {}",
                            email_to_update
                        ));
                    }
                }

                let ApplyUpdateResult { current, previous } =
                    person_row.apply_update(person_update_to_persist.clone(), transaction_id)?;

                // Persist / remove email from index
                match (&update_person.email, &previous.email) {
                    (UpdateAction::Set(email), _) => {
                        self.unique_email_index.insert(email.clone(), id);
                    }
                    (UpdateAction::Unset, Some(email)) => {
                        self.unique_email_index.remove(email);
                    }
                    _ => {}
                }

                ActionResult::Single(current)
            }
            Action::Remove(id) => {
                let person_row = self.person_rows.get_mut(&id).ok_or(format!(
                    "Cannot delete a record [id: {}], does not exist",
                    id
                ))?;

                let ApplyDeleteResult { previous } = person_row.apply_delete(transaction_id)?;

                if let Some(email) = &previous.email {
                    self.unique_email_index.remove(email);
                }

                ActionResult::Single(previous)
            }
            Action::Get(id) => {
                let person = match &self.person_rows.get(&id) {
                    Some(person_data) => person_data.current_state(),
                    None => return Err(format!("No record at [id: {}]", id)),
                };

                ActionResult::GetSingle(person)
            }
            Action::GetVersion(id, version) => {
                let person = match &self.person_rows.get(&id) {
                    Some(person_data) => person_data.at_version(version),
                    None => return Err(format!("No record at [id: {}, version: {}]", id, version)),
                };

                ActionResult::GetSingle(person)
            }
            Action::List => {
                let people_at_transaction_id: Vec<Person> = self
                    .person_rows
                    .iter()
                    .filter_map(|(_, value)| {
                        return value.at_transaction_id(transaction_id);
                    })
                    .collect();

                ActionResult::List(people_at_transaction_id)
            }
            Action::ListLatestVersions => {
                let people_at_transaction_id: Vec<PersonVersion> = self
                    .person_rows
                    .iter()
                    .filter_map(|(_, value)| {
                        return value.version_at_transaction_id(transaction_id);
                    })
                    .collect();

                ActionResult::ListVersion(people_at_transaction_id)
            }
        };

        return Ok(action_result);
    }
}
