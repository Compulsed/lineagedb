use std::collections::HashMap;

use crate::{
    consts::consts::ErrorString,
    model::{action::Action, person::Person},
    row::row::{ApplyDeleteResult, ApplyUpdateResult, PersonRow, UpdateAction},
};

type RowPrimaryKey = String;

pub struct PersonTable {
    pub person_rows: HashMap<String, PersonRow>,
    pub unique_email_index: HashMap<String, String>,
}

impl PersonTable {
    pub fn new() -> Self {
        Self {
            person_rows: HashMap::<RowPrimaryKey, PersonRow>::new(),
            // TODO: Turn this into a class
            unique_email_index: HashMap::<String, RowPrimaryKey>::new(),
        }
    }

    pub fn apply(&mut self, action: Action, transaction_id: usize) -> Result<(), ErrorString> {
        match action {
            Action::Add(person) => {
                let id = person.id.clone();
                let person_to_persist = person.clone();

                // Check if a person with an email already exists
                if let Some(email) = &person.email {
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
                if let Some(email) = person.email {
                    self.unique_email_index.insert(email, person.id);
                }
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

                let ApplyUpdateResult { previous } =
                    person_row.apply_update(person_update_to_persist, transaction_id)?;

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
            }
            Action::Remove(id) => {
                let person_row = self.person_rows.get_mut(&id).ok_or(format!(
                    "Cannot delete a record [id: {}], does not exist",
                    id
                ))?;

                let ApplyDeleteResult { previous } = person_row.apply_delete(transaction_id)?;

                if let Some(email) = previous.email {
                    self.unique_email_index.remove(&email);
                }
            }
            Action::Get(id) => match &self.person_rows.get(&id) {
                Some(person_data) => println!("Get Result: {:#?}", person_data.current_state()),
                None => return Err(format!("No record at [id: {}]", id)),
            },
            Action::GetVersion(id, version) => match &self.person_rows.get(&id) {
                Some(person_data) => println!("Get Result: {:#?}", person_data.at_version(version)),
                None => return Err(format!("No record at [id: {}, version: {}]", id, version)),
            },
            Action::List(transaction_id) => {
                let people_at_transaction_id: Vec<Person> = self
                    .person_rows
                    .iter()
                    .filter_map(|(_, value)| {
                        return value.at_transaction_id(transaction_id);
                    })
                    .collect();

                println!("List Results: {:#?}", people_at_transaction_id)
            }
        }

        return Ok(());
    }
}
