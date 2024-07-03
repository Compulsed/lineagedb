use core::panic;
use crossbeam_skiplist::SkipMap;
use std::sync::RwLock;
use thiserror::Error;

use crate::{
    consts::consts::{EntityId, TransactionId, VersionId},
    database::orchestrator::DatabasePauseEvent,
    model::{
        person::Person,
        statement::{Statement, StatementResult},
    },
};

use super::{
    query::{filter, query},
    row::{
        ApplyDeleteResult, ApplyUpdateResult, DropRow, PersonRow, PersonVersion,
        PersonVersionState, UpdateStatement,
    },
};

// These are examples of 'logical' errors -- https://youtu.be/5blTGTwKZPI?si=tonGUDRXr9p9tTYu&t=685
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

    #[error("Cannot set field to null: {0}")]
    NotNullConstraintViolation(String),
}

pub struct PersonTable {
    pub person_rows: SkipMap<EntityId, RwLock<PersonRow>>,
}

impl PersonTable {
    pub fn new() -> Self {
        Self {
            person_rows: SkipMap::<EntityId, RwLock<PersonRow>>::new(),
        }
    }

    pub fn reset(&self, _: &DatabasePauseEvent) {
        for row in &self.person_rows {
            row.remove();
        }
    }

    pub fn restore_table(&self, version_snapshots: Vec<PersonVersion>) {
        for version_snapshot in version_snapshots {
            let id = version_snapshot.id.clone();

            let person_row = PersonRow::from_restore(version_snapshot);

            self.person_rows.insert(id, RwLock::new(person_row));
        }
    }

    pub fn query_statement(
        &self,
        statement: Statement,
        transaction_id: &TransactionId,
    ) -> Result<StatementResult, ApplyErrors> {
        let action_result = match statement {
            Statement::Get(id) => {
                let person = match &self.person_rows.get(&id) {
                    Some(person_data) => person_data.value().read().unwrap().current_state(),
                    None => return Err(ApplyErrors::CannotGetDoesNotExist(id)),
                };

                StatementResult::GetSingle(person)
            }
            Statement::GetVersion(id, version) => {
                let person = match &self.person_rows.get(&id) {
                    Some(person_data) => person_data
                        .value()
                        .read()
                        .unwrap()
                        .person_at_version(version),

                    None => return Err(ApplyErrors::CannotGetAtVersionDoesNotExist(id, version)),
                };

                StatementResult::GetSingle(person)
            }
            Statement::List(query_person_data) => {
                let mut people = query(&self, &transaction_id);

                sort_list(&mut people);

                if let Some(q) = query_person_data {
                    people = filter(people, q)
                }

                StatementResult::List(people)
            }
            Statement::ListLatestVersions => {
                let people_at_transaction_id: Vec<PersonVersion> = self
                    .person_rows
                    .iter()
                    .filter_map(|value| {
                        value
                            .value()
                            .read()
                            .unwrap()
                            .version_at_transaction_id(&transaction_id)
                    })
                    .collect();

                StatementResult::ListVersion(people_at_transaction_id)
            }
            Statement::Add(_) | Statement::Update(_, _) | Statement::Remove(_) => {
                panic!("Should not be a mutation statement")
            }
        };

        return Ok(action_result);
    }

    // Each mutation statement can be broken up into 3 steps
    //  - Verifying validity
    //  - Applying statement
    //  - Clean up
    pub fn apply(
        &self,
        statement: Statement,
        transaction_id: TransactionId,
    ) -> Result<StatementResult, ApplyErrors> {
        let action_result = match statement {
            Statement::Add(person) => {
                let id = person.id.clone();
                let person_to_persist = person.clone();

                // We need to handle the case where someone can add an item back after it has been deleted
                //  if it has been deleted there will already be a row.
                match self.person_rows.get(&id) {
                    Some(existing_person_row) => {
                        existing_person_row
                            .value()
                            .write()
                            .unwrap()
                            .apply_add(person_to_persist, transaction_id)?;
                    }
                    None => {
                        self.person_rows.insert(
                            id.clone(),
                            RwLock::new(PersonRow::new(person_to_persist, transaction_id)),
                        );
                    }
                }

                StatementResult::Single(person)
            }
            Statement::Update(id, update_person) => {
                let person_row = self
                    .person_rows
                    .get(&id)
                    .ok_or(ApplyErrors::CannotUpdateDoesNotExist(id.clone()))?;

                let person_update_to_persist = update_person.clone();

                let ApplyUpdateResult {
                    current,
                    previous: _,
                } = person_row.value().write().unwrap().apply_update(
                    &id,
                    person_update_to_persist,
                    transaction_id,
                )?;

                StatementResult::Single(current)
            }
            Statement::Remove(id) => {
                let person_row = self
                    .person_rows
                    .get(&id)
                    .ok_or(ApplyErrors::CannotDeleteDoesNotExist(id.clone()))?;

                let ApplyDeleteResult { previous } = person_row
                    .value()
                    .write()
                    .unwrap()
                    .apply_delete(&id, transaction_id)?;

                StatementResult::Single(previous)
            }
            s @ Statement::Get(_)
            | s @ Statement::GetVersion(_, _)
            | s @ Statement::List(_)
            | s @ Statement::ListLatestVersions => {
                return self.query_statement(s, &transaction_id);
            }
        };

        Ok(action_result)
    }

    pub fn apply_rollback(&self, statement: Statement) {
        match statement {
            Statement::Add(person) => {
                self.remove_mutation(person.id);
            }
            Statement::Update(id, _) => {
                self.remove_mutation(id);
            }
            Statement::Remove(id) => {
                self.remove_mutation(id);
            }
            Statement::Get(_)
            | Statement::GetVersion(_, _)
            | Statement::List(_)
            | Statement::ListLatestVersions => {}
        }
    }

    // TODO: Is there a way to centralize the logic for removing constraints? We could run into a situation
    //  where we update the logic here OR the row logic and it could get out of sync. This will likely be important
    //  for indexing as well.
    fn remove_mutation(&self, id: EntityId) {
        let person_row = self
            .person_rows
            .get(&id)
            .expect("should exist because there is a rollback");

        // Remove the version that was applied
        let (person_version_to_remove, drop_row) =
            person_row.value().write().unwrap().rollback_version();

        if matches!(person_version_to_remove.state, PersonVersionState::State(_)) {
            // Note: This should only happen when we rollback an add
            if let DropRow::NoVersionsExist = drop_row {
                self.person_rows.remove(&id);
            }
        }
    }

    #[cfg(test)]
    pub fn get_version_row_test(&self, id: &EntityId) -> PersonRow {
        // At the moment this is only available to tests as a convenience method
        use std::ops::Deref;

        let person_row_value = self.person_rows.get(&id).expect("should have a row");

        let person_row = person_row_value.value().read().unwrap();

        person_row.deref().clone()
    }
}

fn sort_list(people: &mut Vec<Person>) {
    people.sort_by(|a, b| a.id.cmp(&b.id));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::table::row::UpdatePersonData;

    // TODO:
    //  - There should be a better way of comparing lists of a default sort (sort_list)
    //  - Should test listing given a transaction id
    //  - Consider moving this into the filter module
    mod list_filtering {
        use super::*;

        /// Simplest possible cases -- does not need to handle any problems with versioning
        mod add {
            use crate::database::table::query::{QueryMatch, QueryPersonData};

            use super::*;

            #[test]
            fn list_all_items() {
                // Given there is a table with two different people
                let seed_data = vec![
                    Person::new("1".to_string(), Some("1".to_string())),
                    Person::new("2".to_string(), Some("2".to_string())),
                ];

                let seed_actions = seed_data
                    .iter()
                    .map(|person| Statement::Add(person.clone()))
                    .collect();

                // When query the table
                let list_action = Statement::List(None);

                // Then we should get all items back
                list_test(seed_actions, list_action, seed_data);
            }

            #[test]
            fn filter_value() {
                // Given there is a table with two different people
                let seed_data = vec![
                    Person::new("1".to_string(), Some("1".to_string())),
                    Person::new("2".to_string(), Some("2".to_string())),
                ];

                let seed_actions = seed_data
                    .iter()
                    .map(|person| Statement::Add(person.clone()))
                    .collect();

                // When query the table
                let list_action = Statement::List(Some(QueryPersonData {
                    full_name: QueryMatch::Value("1".to_string()),
                    email: QueryMatch::Any,
                }));

                // Then we should only get the rows with "1"
                list_test(seed_actions, list_action, vec![seed_data[0].clone()]);
            }

            #[test]
            fn filter_multiple_values() {
                // Given there is a table with two different people
                let seed_data = vec![
                    Person::new("1".to_string(), Some("1".to_string())),
                    Person::new("2".to_string(), Some("2".to_string())),
                ];

                let seed_actions = seed_data
                    .iter()
                    .map(|person| Statement::Add(person.clone()))
                    .collect();

                // When query the table
                let list_action = Statement::List(Some(QueryPersonData {
                    full_name: QueryMatch::Value("2".to_string()),
                    email: QueryMatch::Value("2".to_string()),
                }));

                // Then we should only get back the rows with "2" for email and full name
                list_test(seed_actions, list_action, vec![seed_data[1].clone()]);
            }

            #[test]
            fn filter_for_null() {
                // Given there is a table with two different people
                let seed_data = vec![
                    Person::new("1".to_string(), Some("1".to_string())),
                    Person::new("2".to_string(), None),
                ];

                let seed_actions = seed_data
                    .iter()
                    .map(|person| Statement::Add(person.clone()))
                    .collect();

                // When query the table
                let list_action = Statement::List(Some(QueryPersonData {
                    full_name: QueryMatch::Any,
                    email: QueryMatch::Null,
                }));

                // Then we should only get back the rows with null
                list_test(seed_actions, list_action, vec![seed_data[1].clone()]);
            }

            #[test]
            fn filter_no_matches() {
                // Given there is a table with two different people
                let seed_data = vec![
                    Person::new("1".to_string(), None),
                    Person::new("2".to_string(), None),
                ];

                let seed_actions = seed_data
                    .iter()
                    .map(|person| Statement::Add(person.clone()))
                    .collect();

                // When query the table
                let list_action = Statement::List(Some(QueryPersonData {
                    full_name: QueryMatch::Any,
                    email: QueryMatch::Value("1".to_string()),
                }));

                // Then we should only get items that have an email of "1", which there are none
                list_test(seed_actions, list_action, vec![]);
            }

            #[test]
            fn filter_not_null() {
                // Given there is a table with two different people
                let seed_data = vec![
                    Person::new("1".to_string(), Some("1".to_string())),
                    Person::new("2".to_string(), None),
                ];

                let seed_actions = seed_data
                    .iter()
                    .map(|person| Statement::Add(person.clone()))
                    .collect();

                // When query the table
                let list_action = Statement::List(Some(QueryPersonData {
                    full_name: QueryMatch::Any,
                    email: QueryMatch::NotNull,
                }));

                // Then we should only get items that have an email, which there is 1
                list_test(seed_actions, list_action, vec![seed_data[0].clone()]);
            }
        }

        mod update {
            use crate::database::table::query::{QueryMatch, QueryPersonData};

            use super::*;

            #[test]
            fn list_all_items() {
                // Given there is a table of one person (added, then updated)
                let person = Person::new("1".to_string(), Some("1".to_string()));

                let seed_actions = vec![
                    Statement::Add(person.clone()),
                    Statement::Update(
                        person.id.clone(),
                        UpdatePersonData {
                            full_name: UpdateStatement::Set("2".to_string()),
                            email: UpdateStatement::NoChanges,
                        },
                    ),
                ];

                // Mimic the update
                let mut updated_person = person.clone();
                updated_person.full_name = "2".to_string();

                // When query the table
                let list_action = Statement::List(None);

                // Then we should get the updated item back
                list_test(seed_actions, list_action, vec![updated_person]);
            }

            #[test]
            fn list_should_match_on_new_values() {
                // Given there is a table of one person (added, then updated)
                let person = Person::new("1".to_string(), Some("1".to_string()));

                let seed_actions = vec![
                    Statement::Add(person.clone()),
                    Statement::Update(
                        person.id.clone(),
                        UpdatePersonData {
                            full_name: UpdateStatement::Set("2".to_string()),
                            email: UpdateStatement::NoChanges,
                        },
                    ),
                ];

                // Mimic the update
                let mut updated_person = person.clone();
                updated_person.full_name = "2".to_string();

                // When querying the table with the current state
                let list_action = Statement::List(Some(QueryPersonData {
                    full_name: QueryMatch::Value("2".to_string()),
                    email: QueryMatch::Any,
                }));

                // Then we should get the updated item back
                list_test(seed_actions, list_action, vec![updated_person]);
            }

            #[test]
            fn list_should_not_match_on_old_values() {
                // Given there is a table of one person (added, then updated)
                let person = Person::new("1".to_string(), Some("1".to_string()));

                let seed_actions = vec![
                    Statement::Add(person.clone()),
                    Statement::Update(
                        person.id.clone(),
                        UpdatePersonData {
                            full_name: UpdateStatement::Set("2".to_string()),
                            email: UpdateStatement::NoChanges,
                        },
                    ),
                ];

                // When we query the table with the old state
                let list_action = Statement::List(Some(QueryPersonData {
                    full_name: QueryMatch::Value("1".to_string()),
                    email: QueryMatch::Any,
                }));

                // Then we should get no items back
                list_test(seed_actions, list_action, vec![]);
            }
        }
    }

    mod delete {
        use crate::database::table::query::{QueryMatch, QueryPersonData};

        use super::*;

        #[test]
        fn list_without_query_should_not_return_deleted_values() {
            // Given there is a table of one person (added, then updated)
            let person = Person::new("1".to_string(), Some("1".to_string()));

            let seed_actions = vec![
                Statement::Add(person.clone()),
                Statement::Remove(person.id.clone()),
            ];

            // When we select all items
            let list_action = Statement::List(None);

            // Then we should get no items back
            list_test(seed_actions, list_action, vec![]);
        }

        #[test]
        fn list_should_not_return_deleted_values() {
            // Given there is a table of one person (added, then updated)
            let person = Person::new("1".to_string(), Some("1".to_string()));

            let seed_actions = vec![
                Statement::Add(person.clone()),
                Statement::Remove(person.id.clone()),
            ];

            // When we select all items
            let list_action = Statement::List(Some(QueryPersonData {
                full_name: QueryMatch::Any,
                email: QueryMatch::Any,
            }));

            // Then we should get no items back
            list_test(seed_actions, list_action, vec![]);
        }
    }

    pub fn list_test(
        statements: Vec<Statement>,
        list_statement: Statement,
        compare_to: Vec<Person>,
    ) -> () {
        let table = PersonTable::new();
        let mut next_transaction_id = TransactionId::new_first_transaction();

        for statement in statements {
            table.apply(statement, next_transaction_id.clone()).unwrap();
            next_transaction_id = next_transaction_id.increment();
        }

        let mut list_results = table
            .apply(list_statement, next_transaction_id)
            .unwrap()
            .list();

        let mut sort_compare_to = compare_to;

        sort_list(&mut list_results);
        sort_list(&mut sort_compare_to);

        assert_eq!(&list_results, &sort_compare_to);
    }
}

mod versioning {
    use crate::database::table::row::UpdatePersonData;

    use super::*;

    /// Tests are broken up into three categories:
    /// - Get Statement
    /// - Row data, normally would not depend on private fields, though MVCC has complex logic so this makes it easier to test
    mod row_data {
        use super::*;

        #[test]
        fn adding_item_creates_version_at_v1() {
            // Given an empty table
            let mut table = PersonTable::new();

            // When we add an item
            let (person, _) = add_test_person_to_empty_database(&mut table);

            // Then we should have: one version, at version 1, with transaction id 1
            let person_row = table.get_version_row_test(&person.id);

            assert_eq!(person_row.version_count(), 1);

            assert_eq!(
                person_row.at_version(VersionId(1)),
                Some(PersonVersion {
                    id: person.id.clone(),
                    state: PersonVersionState::State(person),
                    version: VersionId(1),
                    transaction_id: TransactionId(1),
                })
            );
        }

        #[test]
        fn adding_then_updating_creates_version_v2() {
            // Given an empty table
            let mut table = PersonTable::new();

            // When we add an item
            let (person, next_transaction_id) = add_test_person_to_empty_database(&mut table);

            // And we update the item
            let (updated_person, _) = update_test_person(&mut table, &person, next_transaction_id);

            // Then we should have: two versions, at version 1 and 2, with transaction id 1 and 2
            let person_row = table.get_version_row_test(&person.id);

            assert_eq!(person_row.version_count(), 2);

            assert_eq!(
                person_row.at_version(VersionId(1)),
                Some(PersonVersion {
                    id: person.id.clone(),
                    state: PersonVersionState::State(person),
                    version: VersionId(1),
                    transaction_id: TransactionId(1),
                })
            );

            assert_eq!(
                person_row.at_version(VersionId(2)),
                Some(PersonVersion {
                    id: updated_person.id.clone(),
                    state: PersonVersionState::State(updated_person),
                    version: VersionId(2),
                    transaction_id: TransactionId(2),
                })
            );
        }

        #[test]
        fn adding_then_updating_then_deleting_creates_version_v3() {
            // Given an empty table
            let mut table = PersonTable::new();

            // When we add an item
            let (add_person, next_transaction_id) = add_test_person_to_empty_database(&mut table);

            // And we update the item
            let (updated_person, next_transaction_id) =
                update_test_person(&mut table, &add_person, next_transaction_id);

            // And we delete the item
            let _ = delete_test_person(&mut table, &updated_person.id, next_transaction_id);

            // Then we should have: two versions, at version 1 and 2, with transaction id 1 and 2
            let person_row = table.get_version_row_test(&updated_person.id);

            assert_eq!(person_row.version_count(), 3);

            assert_eq!(
                person_row.at_version(VersionId(1)),
                Some(PersonVersion {
                    id: add_person.id.clone(),
                    state: PersonVersionState::State(add_person),
                    version: VersionId(1),
                    transaction_id: TransactionId(1),
                })
            );

            assert_eq!(
                person_row.at_version(VersionId(2)),
                Some(PersonVersion {
                    id: updated_person.id.clone(),
                    state: PersonVersionState::State(updated_person.clone()),
                    version: VersionId(2),
                    transaction_id: TransactionId(2),
                })
            );

            assert_eq!(
                person_row.at_version(VersionId(3)),
                Some(PersonVersion {
                    id: updated_person.id.clone(),
                    state: PersonVersionState::Delete,
                    version: VersionId(3),
                    transaction_id: TransactionId(3),
                })
            );
        }
    }

    mod get_statement {
        #[warn(unused_imports)]
        use super::*;

        #[test]
        fn add_then_get_person_at_version() {
            // Given an empty table
            let mut table = PersonTable::new();

            // When we add an item
            let (person, next_transaction_id) = add_test_person_to_empty_database(&mut table);

            // Then we should be able to get the item at version 1
            let person_v1 = get_test_person_at_version(
                &mut table,
                &person.id,
                &VersionId(1),
                next_transaction_id,
            )
            .expect("should have person");

            assert_eq!(&person_v1, &person);
        }

        #[test]
        fn add_update_then_get_person_at_version() {
            // Given an empty table
            let mut table = PersonTable::new();

            // When we add an item
            let (person, next_transaction_id) = add_test_person_to_empty_database(&mut table);

            // And we update the item
            let (updated_person, next_transaction_id) =
                update_test_person(&mut table, &person, next_transaction_id);

            // Then we should be able to get the item at version 1
            let person_v1 = get_test_person_at_version(
                &mut table,
                &person.id,
                &VersionId(1),
                next_transaction_id.clone(),
            )
            .expect("should have person");

            assert_eq!(&person_v1, &person);

            // Then we should be able to get the item at version 2
            let person_v2 = get_test_person_at_version(
                &mut table,
                &person.id,
                &VersionId(2),
                next_transaction_id.clone(),
            )
            .expect("should have person");

            assert_eq!(&person_v2, &updated_person);
        }

        #[test]
        fn add_update_delete_then_get_person_at_version() {
            // Given an empty table
            let mut table = PersonTable::new();

            // When we add an item
            let (person, next_transaction_id) = add_test_person_to_empty_database(&mut table);

            // And we update the item
            let (updated_person, next_transaction_id) =
                update_test_person(&mut table, &person, next_transaction_id);

            // And we delete the item
            let next_transaction_id =
                delete_test_person(&mut table, &person.id, next_transaction_id);

            // Then we should be able to get the item at version 1
            let person_v1 = get_test_person_at_version(
                &mut table,
                &person.id,
                &VersionId(1),
                next_transaction_id.clone(),
            )
            .expect("should have person");

            assert_eq!(&person_v1, &person);

            // Then we should be able to get the item at version 2
            let person_v2 = get_test_person_at_version(
                &mut table,
                &person.id,
                &VersionId(2),
                next_transaction_id.clone(),
            )
            .expect("should have person");

            assert_eq!(&person_v2, &updated_person);

            // Then we should NOT be able to get the item at version 3
            let person_v3 = get_test_person_at_version(
                &mut table,
                &person.id,
                &VersionId(3),
                next_transaction_id.clone(),
            );

            assert!(person_v3.is_none());
        }
    }

    #[allow(dead_code)]
    fn add_test_person_to_empty_database(table: &mut PersonTable) -> (Person, TransactionId) {
        let transaction_id = TransactionId::new_first_transaction();
        add_test_person(table, transaction_id)
    }

    #[allow(dead_code)]
    fn add_test_person(
        table: &mut PersonTable,
        next_transaction_id: TransactionId,
    ) -> (Person, TransactionId) {
        let person = Person::new(
            next_transaction_id.to_string(),
            Some(next_transaction_id.to_string()),
        );
        let statement = Statement::Add(person.clone());

        table.apply(statement, next_transaction_id.clone()).unwrap();

        (person, next_transaction_id.increment())
    }

    #[allow(dead_code)]
    fn update_test_person(
        table: &mut PersonTable,
        person: &Person,
        next_transaction_id: TransactionId,
    ) -> (Person, TransactionId) {
        let mut updated_person = person.clone();
        updated_person.email = Some("email".to_string());

        let statement = Statement::Update(
            person.id.clone(),
            UpdatePersonData {
                full_name: UpdateStatement::NoChanges,
                email: UpdateStatement::Set("email".to_string()),
            },
        );

        table.apply(statement, next_transaction_id.clone()).unwrap();

        (updated_person, next_transaction_id.increment())
    }

    #[allow(dead_code)]
    fn delete_test_person(
        table: &mut PersonTable,
        id: &EntityId,
        next_transaction_id: TransactionId,
    ) -> TransactionId {
        let statement = Statement::Remove(id.clone());

        table.apply(statement, next_transaction_id.clone()).unwrap();

        next_transaction_id.increment()
    }

    #[allow(dead_code)]
    fn get_test_person_at_version(
        table: &mut PersonTable,
        id: &EntityId,
        version: &VersionId,
        next_transaction_id: TransactionId,
    ) -> Option<Person> {
        let statement = Statement::GetVersion(id.clone(), version.clone());
        let result = table.apply(statement, next_transaction_id).unwrap();

        match result {
            StatementResult::GetSingle(person) => person,
            _ => {
                // Note: Unsure why but cannot panic here, just assert false
                assert!(false, "should be a single person");
                None
            }
        }
    }
}
