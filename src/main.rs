use std::{
    collections::HashMap,
    sync::mpsc::{self, Receiver, Sender},
    thread, time,
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
struct PersonData {
    versions: Vec<PersonVersion>,
}

impl PersonData {
    fn apply_add(person: Person, transaction_id: usize) -> Self {
        PersonData {
            versions: vec![PersonVersion {
                version: START_AT_INDEX,
                state: PersonVersionState::State(person),
                transaction_id,
            }],
        }
    }

    fn apply_update(
        &mut self,
        update: &UpdatePersonData,
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

fn process_action_mutation(
    user_data: &mut HashMap<String, PersonData>,
    transaction: &Transaction,
) -> Result<(), ErrorString> {
    match &transaction.action {
        Action::Add(person) => {
            let id = person.id.clone();

            if user_data.get(&id).is_some() {
                return Err(format!("Duplicate record [id: {}], already exists", id));
            }

            user_data.insert(
                id,
                PersonData::apply_add(person.to_owned(), transaction.transaction_id),
            );
        }
        Action::Update(id, update_person) => {
            let person_data = user_data.get_mut(id);

            match person_data {
                Some(person_data) => {
                    person_data.apply_update(update_person, transaction.transaction_id)?;
                }
                None => return Err(format!("Cannot update record [id: {}], does not exist", id)),
            }
        }
        Action::Remove(id) => {
            let person_data = user_data.get_mut(id);

            match person_data {
                Some(person_data) => {
                    person_data.apply_delete(transaction.transaction_id)?;
                }
                None => {
                    return Err(format!(
                        "Cannot delete a record [id: {}], does not exist",
                        id
                    ))
                }
            }
        }
        Action::Get(_) | Action::GetVersion(_, _) | Action::List(_) => {
            panic!("Should only contain mutation actions")
        }
    }

    Ok(())
}

fn process_action_query(
    user_data: &mut HashMap<String, PersonData>,
    user_action: &Action,
) -> Result<(), ErrorString> {
    match user_action {
        Action::Get(id) => match user_data.get(id) {
            Some(person_data) => println!("Get Result: {:#?}", person_data.current_state()),
            None => return Err(format!("No record at [id: {}]", id)),
        },
        Action::GetVersion(id, version) => match user_data.get(id) {
            Some(person_data) => println!("Get Result: {:#?}", person_data.at_version(*version)),
            None => return Err(format!("No record at [id: {}, version: {}]", id, version)),
        },
        // TODO: List by transaction Id
        Action::List(transaction_id) => {
            let people_at_transaction_id: Vec<Person> = user_data
                .into_iter()
                .filter_map(|(_, value)| {
                    return value.at_transaction_id(*transaction_id);
                })
                .collect();

            println!("List Results: {:#?}", people_at_transaction_id)
        }
        Action::Add(_) | Action::Update(_, _) | Action::Remove(_) => {
            panic!("Should only contain mutation actions")
        }
    }

    Ok(())
}

fn process_action(
    user_data: &mut HashMap<String, PersonData>,
    processed_transactions: &mut Vec<Transaction>,
    user_action: &Action,
) -> Result<(), ErrorString> {
    match user_action.is_mutation() {
        true => {
            let new_transaction_id = processed_transactions
                .last()
                .and_then(|t| Some(t.transaction_id + 1))
                .unwrap_or(START_AT_INDEX);

            let new_transaction = Transaction {
                transaction_id: new_transaction_id,
                action: user_action.clone(),
            };

            // Apply to world state
            process_action_mutation(user_data, &new_transaction)?;

            // Push to history (is this the same as persisting to file?)
            processed_transactions.push(new_transaction)
        }
        false => process_action_query(user_data, user_action)?,
    }

    Ok(())
}

static NTHREADS: i32 = 3;

/*
    1. Start with multiple threads which can read / write data, accept information via channels
*/
fn main() {
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

                let transaction = Action::Update(id.clone(), UpdatePersonData {
                    full_name: UpdateAction::Set(format!("[Count {}] Dale Salter", counter)),
                    email: UpdateAction::NoChanges,
                });

                thread_tx.send(transaction).unwrap();

                thread::sleep(time::Duration::from_millis(10000));
            }
        });
    }

    let mut user_data = HashMap::<String, PersonData>::new();
    let mut processed_transactions: Vec<Transaction> = vec![];

    loop {
        let action = rx.recv().unwrap();

        let response = process_action(&mut user_data, &mut processed_transactions, &action);

        if let Err(err) = response {
            println!("Error applying transaction: {}", err);
            println!("Action: {:?}", action)
        }

        process_action(&mut user_data, &mut processed_transactions, &Action::List(1000)).unwrap();
    }
}
