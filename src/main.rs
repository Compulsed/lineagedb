use std::{
    collections::HashMap,
    sync::mpsc::{self, Receiver, Sender},
    thread::{self},
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

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
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
    versions: Vec<PersonVersion>,
}

impl PersonRow {
    fn apply_add(person: Person, transaction_id: usize) -> Self {
        PersonRow {
            versions: vec![PersonVersion {
                version: START_AT_INDEX,
                state: PersonVersionState::State(person),
                transaction_id,
            }],
        }
    }

    fn apply_update(
        &mut self,
        update: UpdatePersonData,
        transaction_id: usize,
    ) -> Result<(), ErrorString> {
        let current_version = self.current_version();

        match &current_version.state {
            PersonVersionState::State(current_person) => {
                let mut new_person = current_person.clone();

                match &update.full_name {
                    UpdateAction::Set(full_name) => new_person.full_name = full_name.clone(),
                    UpdateAction::Unset => {
                        return Err("Full name cannot be set to null".to_string())
                    }
                    UpdateAction::NoChanges => {}
                }

                match &update.email {
                    UpdateAction::Set(email) => new_person.email = Some(email.clone()),
                    UpdateAction::Unset => new_person.email = None,
                    UpdateAction::NoChanges => {}
                }

                self.apply_new_version(
                    current_version,
                    PersonVersionState::State(new_person),
                    transaction_id,
                )
            }
            PersonVersionState::Delete => return Err("Cannot update a deleted record".to_string()),
        };

        Ok(())
    }

    fn apply_delete(&mut self, transaction_id: usize) -> Result<(), ErrorString> {
        let current_version = self.current_version();

        match current_version.state {
            PersonVersionState::State(_) => {
                self.apply_new_version(current_version, PersonVersionState::Delete, transaction_id)
            }
            PersonVersionState::Delete => {
                return Err("Cannot delete an already deleted record".to_string())
            }
        }

        Ok(())
    }

    fn apply_new_version(
        &mut self,
        current_version: PersonVersion,
        new_state: PersonVersionState,
        transaction_id: usize,
    ) {
        self.versions.push(PersonVersion {
            state: new_state,
            version: current_version.version + 1,
            transaction_id,
        })
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

struct PersonTable {
    user_data: HashMap<String, PersonRow>,
}

impl PersonTable {
    fn new() -> Self {
        Self {
            user_data: HashMap::<String, PersonRow>::new(),
        }
    }

    fn apply(&mut self, action: Action, transaction_id: usize) -> Result<(), ErrorString> {
        match action {
            Action::Add(person) => {
                let id = person.id.clone();

                if self.user_data.get(&id).is_some() {
                    return Err(format!("Duplicate record [id: {}], already exists", id));
                }

                self.user_data
                    .insert(id, PersonRow::apply_add(person, transaction_id));
            }
            Action::Update(id, update_person) => {
                let person_data = self.user_data.get_mut(&id);

                match person_data {
                    Some(person_data) => {
                        person_data.apply_update(update_person, transaction_id)?;
                    }
                    None => {
                        return Err(format!("Cannot update record [id: {}], does not exist", id))
                    }
                }
            }
            Action::Remove(id) => {
                let person_data = self.user_data.get_mut(&id);

                match person_data {
                    Some(person_data) => {
                        person_data.apply_delete(transaction_id)?;
                    }
                    None => {
                        return Err(format!(
                            "Cannot delete a record [id: {}], does not exist",
                            id
                        ))
                    }
                }
            }
            Action::Get(id) => match &self.user_data.get(&id) {
                Some(person_data) => println!("Get Result: {:#?}", person_data.current_state()),
                None => return Err(format!("No record at [id: {}]", id)),
            },
            Action::GetVersion(id, version) => match &self.user_data.get(&id) {
                Some(person_data) => println!("Get Result: {:#?}", person_data.at_version(version)),
                None => return Err(format!("No record at [id: {}, version: {}]", id, version)),
            },
            Action::List(transaction_id) => {
                let people_at_transaction_id: Vec<Person> = self
                    .user_data
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

    for id in 0..NTHREADS {
        let thread_tx = tx.clone();

        thread::spawn(move || {
            let id = format!("[Thread {}]", id.to_string());

            let add_transaction = Action::Add(Person {
                id: id.clone(),
                full_name: format!("[Count 0] Dale Salter"),
                email: Some("dalejsalter@outlook.com".to_string()),
            });

            thread_tx.send(add_transaction).unwrap();

            let mut counter = 0;

            loop {
                counter = counter + 1;

                let transaction = Action::Update(
                    id.clone(),
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
