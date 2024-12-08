use serde::{Deserialize, Serialize};

use crate::{consts::consts::TransactionId, model::person::Person};

use super::table::PersonTable;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum QueryMatch {
    Value(String),
    Null,
    NotNull,
    Any,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct QueryPersonData {
    pub full_name: QueryMatch,
    pub email: QueryMatch,
}

#[tracing::instrument(skip(table))]
pub fn query(table: &PersonTable, transaction_id: &TransactionId) -> Vec<Person> {
    return table
        .person_rows
        .iter()
        .filter_map(|v| v.value().read().unwrap().at_transaction_id(&transaction_id))
        .collect();
}

#[tracing::instrument(skip(people))]
pub fn filter(people: Vec<Person>, query: QueryPersonData) -> Vec<Person> {
    let filtered_people = people
        .into_iter()
        .filter(|person| {
            match &query.full_name {
                QueryMatch::Value(full_name) => {
                    if &person.full_name != full_name {
                        return false;
                    }
                }
                QueryMatch::Any => {}
                // Fullname is not nullable, this check is static
                QueryMatch::NotNull => {}
                QueryMatch::Null => return false,
            }

            match &query.email {
                QueryMatch::Value(email) => match &person.email {
                    Some(person_email) => {
                        if person_email != email {
                            return false;
                        }
                    }
                    None => return false,
                },
                QueryMatch::Null => {
                    if person.email.is_some() {
                        return false;
                    }
                }
                QueryMatch::NotNull => {
                    if person.email.is_none() {
                        return false;
                    }
                }
                QueryMatch::Any => {}
            }

            return true;
        })
        .collect();

    return filtered_people;
}
