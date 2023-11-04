use std::{
    collections::HashMap,
    sync::mpsc::{self, Receiver, Sender},
    thread::{self, current},
    time,
};

const START_AT_INDEX: usize = 1;

#[derive(Clone, Debug)]
struct UpdatePersonData {
    full_name: UpdateAction,
    email: UpdateAction,
}

#[derive(Clone, Debug)]
enum Action {
    Add(Person),
    Update(String, UpdatePersonData),
    Remove(String),
    Get(String),
    GetVersion(String, usize),
    List(usize),
}

impl Action {
    fn is_mutation(&self) -> bool {
        match self {
            Action::Add(_) | Action::Remove(_) | Action::Update(_, _) => true,
            Action::List(_) | Action::Get(_) | Action::GetVersion(_, _) => false,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct Person {
    id: String,
    full_name: String,
    email: Option<String>,
}

#[derive(Clone, Debug)]
enum UpdateAction {
    Set(String),
    Unset,
    NoChanges,
}

#[derive(Debug)]
struct Transaction {
    transaction_id: usize,
    action: Action,
}

#[derive(Clone, Debug, PartialEq)]
enum PersonVersionState {
    State(Person),
    Delete,
}

#[derive(Clone, Debug)]
struct PersonVersion {
    state: PersonVersionState,
    version: usize,
    transaction_id: usize,
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
struct PersonRow {
    /// Earliest versions are at beginning, latest version is last
    versions: Vec<PersonVersion>,
}

impl PersonRow {
    fn new(person: Person, transaction_id: usize) -> Self {
        PersonRow {
            versions: vec![PersonVersion {
                state: PersonVersionState::State(person),
                version: START_AT_INDEX,
                transaction_id,
            }],
        }
    }

    /// Handles the case of adding an item back after it was deleted
    fn apply_add(&mut self, person: Person, transaction_id: usize) -> Result<(), ErrorString> {
        let current_version = self.current_version();

        if &current_version.state != &PersonVersionState::Delete {
            return Err("Cannot add an item when it already exists".to_string());
        }

        self.apply_new_version(
            &current_version,
            PersonVersionState::State(person),
            transaction_id,
        );

        Ok(())
    }

    fn apply_update(
        &mut self,
        update: UpdatePersonData,
        transaction_id: usize,
    ) -> Result<ApplyUpdateResult, ErrorString> {
        let previous_version = self.current_version();

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

        self.apply_new_version(
            &previous_version,
            PersonVersionState::State(current_person),
            transaction_id,
        );

        Ok(ApplyUpdateResult { previous: previous_person })
    }

    fn apply_delete(&mut self, transaction_id: usize) -> Result<ApplyDeleteResult, ErrorString> {
        let current_version = self.current_version();

        let previous_person = match current_version.clone().state {
            PersonVersionState::State(s) => s,
            PersonVersionState::Delete => {
                return Err("Cannot delete an already deleted record".to_string());
            },
        };

        self.apply_new_version(&current_version, PersonVersionState::Delete, transaction_id);

        Ok(ApplyDeleteResult { previous: previous_person })
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

    fn current_version(&self) -> PersonVersion {
        self.versions
            .last()
            .expect("should not be possible to create a person data without any versions")
            .clone()
    }

    fn current_state(&self) -> Option<Person> {
        self.current_version().get_person()
    }

    fn at_version(&self, version_id: usize) -> Option<Person> {
        match self.versions.get(version_id) {
            Some(version) => version.get_person(),
            None => None,
        }
    }

    fn at_transaction_id(&self, transaction_id: usize) -> Option<Person> {
        // Can optimize this with a binary search
        for version in self.versions.iter().rev() {
            // May contain newer uncommited versions, we want to find the closest committed version
            if version.transaction_id <= transaction_id {
                return version.get_person();
            }
        }

        None
    }
}


type ErrorString = String;

#[derive(Debug)]
struct ApplyUpdateResult {
    previous: Person,
}

#[derive(Debug)]
struct ApplyDeleteResult {
    previous: Person,
}

type RowPrimaryKey = String;

struct PersonTable {
    person_rows: HashMap<String, PersonRow>,
    unique_email_index: HashMap<String, String>,
}

impl PersonTable {
    fn new() -> Self {
        Self {
            person_rows: HashMap::<RowPrimaryKey, PersonRow>::new(),
            // TODO: Turn this into a class 
            unique_email_index: HashMap::<String, RowPrimaryKey>::new(),
        }
    }

    fn apply(&mut self, action: Action, transaction_id: usize) -> Result<(), ErrorString> {
        match action {
            Action::Add(person) => {
                let id = person.id.clone();
                let person_to_persist = person.clone();

                // Check if a person with an email already exists
                if let Some(email) = &person.email {
                    if self.unique_email_index.contains_key(email) {
                        return Err(format!("Cannot add row as a person already exists with this email: {}", email))
                    }
                }

                // We need to handle the case where someone can add an item back after it has been deleted
                //  if it has been deleted there will already be a row
                match self.person_rows.get_mut(&id) {
                    Some(existing_person_row) => {
                        existing_person_row.apply_add(person_to_persist, transaction_id)?;
                    }
                    None => {
                        self.person_rows.insert(id, PersonRow::new(person_to_persist, transaction_id));
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
                        return Err(format!("Cannot update row as person already exists with this email: {}", email_to_update))
                    }
                }

                let ApplyUpdateResult { previous } = person_row.apply_update(person_update_to_persist, transaction_id)?;

                // Persist / remove email from index
                match (&update_person.email, &previous.email) {
                    (UpdateAction::Set(email), _) => {
                        self.unique_email_index.insert(email.clone(), id);
                    },
                    (UpdateAction::Unset, Some(email)) => {
                        self.unique_email_index.remove(email);
                    },
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

fn process_action(
    person_table: &mut PersonTable,
    processed_transactions: &mut Vec<Transaction>,
    user_action: Action,
) -> Result<(), ErrorString> {
    let mut transaction_id = processed_transactions
        .last()
        .and_then(|t| Some(t.transaction_id))
        .unwrap_or(START_AT_INDEX);

    if user_action.is_mutation() {
        transaction_id = transaction_id + 1;

        let new_transaction = Transaction {
            transaction_id: transaction_id,
            action: user_action.clone(),
        };

        processed_transactions.push(new_transaction)
    }

    person_table.apply(user_action, transaction_id)?;

    Ok(())
}

fn main() {
    static NTHREADS: i32 = 3;

    let (tx, rx): (Sender<Action>, Receiver<Action>) = mpsc::channel();

    for thread_id in 0..NTHREADS {
        let thread_tx = tx.clone();

        thread::spawn(move || {
            let record_id = format!("[Thread {}]", thread_id.to_string());

            let add_transaction = Action::Add(Person {
                id: record_id.clone(),
                full_name: format!("[Count 0] Dale Salter"),
                email: Some(format!("dalejsalter-{}@outlook.com", thread_id)),
            });

            thread_tx.send(add_transaction).unwrap();

            let mut counter = 0;

            loop {
                counter = counter + 1;

                let transaction = Action::Update(
                    record_id.clone(),
                    UpdatePersonData {
                        full_name: UpdateAction::Set(format!("[Count {}] Dale Salter", counter)),
                        email: UpdateAction::NoChanges,
                    },
                );

                thread_tx.send(transaction).unwrap();

                thread::sleep(time::Duration::from_millis(5000));
            }
        });
    }

    let mut person_table = PersonTable::new();
    let mut processed_transactions: Vec<Transaction> = vec![];

    loop {
        let action = rx.recv().unwrap();
        let response = process_action(
            &mut person_table,
            &mut processed_transactions,
            action.clone(),
        );

        if let Err(err) = response {
            println!("Error applying transaction: {}", err);
            println!("Action: {:?}", action)
        }

        process_action(
            &mut person_table,
            &mut processed_transactions,
            Action::List(1000),
        )
        .unwrap();
    }
}
