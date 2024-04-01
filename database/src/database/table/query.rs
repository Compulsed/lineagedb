use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::{
    consts::consts::{EntityId, TransactionId},
    model::person::Person,
};

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

enum SearchMethod {
    Index(HashSet<EntityId>),
    RequiresFullScan,
    NoResults,
}

fn determine_search_method(
    table: &PersonTable,
    query_person_data: &QueryPersonData,
) -> SearchMethod {
    // match &query_person_data.full_name {
    //     QueryMatch::Value(v) => match table.full_name_index.get_from_index(&Some(v.clone())) {
    //         Some(set) => SearchMethod::Index(set.clone()),
    //         None => SearchMethod::NoResults,
    //     },
    //     QueryMatch::Null => match table.full_name_index.get_from_index(&None) {
    //         Some(set) => SearchMethod::Index(set.clone()),
    //         None => SearchMethod::NoResults,
    //     },
    //     // TODO: We could use the index here, but it would require a full scan of the index
    //     QueryMatch::NotNull => SearchMethod::RequiresFullScan,
    //     QueryMatch::Any => SearchMethod::RequiresFullScan,
    // }

    SearchMethod::RequiresFullScan
}

pub fn query(
    table: &PersonTable,
    transaction_id: &TransactionId,
    query_person_data: &Option<QueryPersonData>,
    use_index: bool,
) -> Vec<Person> {
    let search_method = match (query_person_data, use_index) {
        (Some(q), true) => determine_search_method(table, q),
        _ => SearchMethod::RequiresFullScan,
    };

    let people: Vec<Person> = match search_method {
        SearchMethod::Index(set) => set
            .into_iter()
            .filter_map(|id| table.person_rows.get(&id))
            .filter_map(|value| {
                value
                    .value()
                    .read()
                    .unwrap()
                    .at_transaction_id(&transaction_id)
            })
            .collect(),
        SearchMethod::RequiresFullScan => table
            .person_rows
            .iter()
            .filter_map(|v| v.value().read().unwrap().at_transaction_id(&transaction_id))
            .collect(),
        SearchMethod::NoResults => {
            return vec![];
        }
    };

    return people;
}

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
