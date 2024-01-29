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

use super::{
    index::FullNameIndex,
    query::{filter, query},
    row::{
        ApplyDeleteResult, ApplyUpdateResult, DropRow, PersonRow, PersonVersion,
        PersonVersionState, UpdateAction,
    },
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

pub struct PersonTable {
    pub person_rows: HashMap<EntityId, PersonRow>,
    pub unique_email_index: HashMap<String, EntityId>,
    pub full_name_index: FullNameIndex,
}

impl PersonTable {
    pub fn new() -> Self {
        Self {
            person_rows: HashMap::<EntityId, PersonRow>::new(),
            unique_email_index: HashMap::<String, EntityId>::new(),
            full_name_index: FullNameIndex::new(),
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
                match self.person_rows.get_mut(&id) {
                    Some(existing_person_row) => {
                        existing_person_row.apply_add(person_to_persist, transaction_id)?;
                    }
                    None => {
                        self.person_rows.insert(
                            id.clone(),
                            PersonRow::new(person_to_persist, transaction_id),
                        );
                    }
                }

                // Persist the email so it cannot be added again
                if let Some(email) = &person.email {
                    self.unique_email_index
                        .insert(email.clone(), person.id.clone());
                }

                // Update index
                self.full_name_index
                    .save_to_index(id, Some(person.full_name.clone()));

                ActionResult::Single(person)
            }
            Action::Update(id, update_person) => {
                let person_row = self
                    .person_rows
                    .get_mut(&id)
                    .ok_or(ApplyErrors::CannotUpdateDoesNotExist(id.clone()))?;

                if let UpdateAction::Set(email_to_update) = &update_person.email {
                    let mut skip_check = false;

                    // Edge case: If we are updating the email to the same value, we don't need to check the uniqueness constraint
                    if let PersonVersionState::State(previous_person) =
                        &person_row.current_version().state
                    {
                        if let Some(previous_email) = &previous_person.email {
                            if previous_email == email_to_update {
                                skip_check = true;
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

                // Persist / remove email from unique constraint index
                match (&update_person.email, &previous.email) {
                    (UpdateAction::Set(email), _) => {
                        self.unique_email_index.insert(email.clone(), id.clone());
                    }
                    (UpdateAction::Unset, Some(email)) => {
                        self.unique_email_index.remove(email);
                    }
                    _ => {}
                }

                // Update index
                if previous.full_name != current.full_name {
                    self.full_name_index.update_index(
                        id,
                        &Some(previous.full_name),
                        Some(current.full_name.clone()),
                    );
                }

                ActionResult::Single(current)
            }
            Action::Remove(id) => {
                let person_row = self
                    .person_rows
                    .get_mut(&id)
                    .ok_or(ApplyErrors::CannotDeleteDoesNotExist(id.clone()))?;

                let ApplyDeleteResult { previous } =
                    person_row.apply_delete(&id, transaction_id)?;

                if let Some(email) = &previous.email {
                    self.unique_email_index.remove(email);
                }

                // Remove from index
                self.full_name_index
                    .remove_from_index(&id, &Some(previous.full_name.clone()));

                ActionResult::Single(previous)
            }
            Action::Get(id) => {
                let person = match &self.person_rows.get(&id) {
                    Some(person_data) => person_data.current_state(),
                    None => return Err(ApplyErrors::CannotGetDoesNotExist(id)),
                };

                ActionResult::GetSingle(person)
            }
            Action::GetVersion(id, version) => {
                let person = match &self.person_rows.get(&id) {
                    Some(person_data) => person_data.person_at_version(version),
                    None => return Err(ApplyErrors::CannotGetAtVersionDoesNotExist(id, version)),
                };

                ActionResult::GetSingle(person)
            }
            Action::List(query_person_data) => {
                let mut people = query(&self, &transaction_id, &query_person_data, true);

                sort_list(&mut people);

                if let Some(q) = query_person_data {
                    people = filter(people, q)
                }

                ActionResult::List(people)
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
            | Action::List(_)
            | Action::ListLatestVersions => {}
        }
    }

    // TODO: Is there a way to centralize the logic for removing constraints? We could run into a situation
    //  where we update the logic here OR the row logic and it could get out of sync. This will likely be important
    //  for indexing as well.
    fn remove_mutation(&mut self, id: EntityId) {
        let person_row = self
            .person_rows
            .get_mut(&id)
            .expect("should exist because there is a rollback");

        // Remove the version that was applied
        let (person_version_to_remove, drop_row) = person_row.rollback_version();

        match person_version_to_remove.state {
            PersonVersionState::State(person) => {
                if let Some(email) = person.email {
                    self.unique_email_index.remove(&email);
                }

                // Note: This should only happen when we rollback an add
                if let DropRow::NoVersionsExist = drop_row {
                    self.person_rows.remove(&id);
                }
            }
            PersonVersionState::Delete => {
                let current_person = person_row.current_version();

                if let PersonVersionState::State(person) = &current_person.state {
                    if let Some(email) = &person.email {
                        self.unique_email_index.insert(email.clone(), id.clone());
                    }
                } else {
                    panic!("delete should always be followed by a state");
                }
            }
        }
    }
}

fn sort_list(people: &mut Vec<Person>) {
    people.sort_by(|a, b| a.id.cmp(&b.id));
}

// TODO: Tests are missing
//  - Constraint / index validation, perhaps we should move index operations into their own class?
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
                    .map(|person| Action::Add(person.clone()))
                    .collect();

                // When query the table
                let list_action = Action::List(None);

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
                    .map(|person| Action::Add(person.clone()))
                    .collect();

                // When query the table
                let list_action = Action::List(Some(QueryPersonData {
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
                    .map(|person| Action::Add(person.clone()))
                    .collect();

                // When query the table
                let list_action = Action::List(Some(QueryPersonData {
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
                    .map(|person| Action::Add(person.clone()))
                    .collect();

                // When query the table
                let list_action = Action::List(Some(QueryPersonData {
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
                    .map(|person| Action::Add(person.clone()))
                    .collect();

                // When query the table
                let list_action = Action::List(Some(QueryPersonData {
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
                    .map(|person| Action::Add(person.clone()))
                    .collect();

                // When query the table
                let list_action = Action::List(Some(QueryPersonData {
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
                    Action::Add(person.clone()),
                    Action::Update(
                        person.id.clone(),
                        UpdatePersonData {
                            full_name: UpdateAction::Set("2".to_string()),
                            email: UpdateAction::NoChanges,
                        },
                    ),
                ];

                // Mimic the update
                let mut updated_person = person.clone();
                updated_person.full_name = "2".to_string();

                // When query the table
                let list_action = Action::List(None);

                // Then we should get the updated item back
                list_test(seed_actions, list_action, vec![updated_person]);
            }

            #[test]
            fn list_should_match_on_new_values() {
                // Given there is a table of one person (added, then updated)
                let person = Person::new("1".to_string(), Some("1".to_string()));

                let seed_actions = vec![
                    Action::Add(person.clone()),
                    Action::Update(
                        person.id.clone(),
                        UpdatePersonData {
                            full_name: UpdateAction::Set("2".to_string()),
                            email: UpdateAction::NoChanges,
                        },
                    ),
                ];

                // Mimic the update
                let mut updated_person = person.clone();
                updated_person.full_name = "2".to_string();

                // When querying the table with the current state
                let list_action = Action::List(Some(QueryPersonData {
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
                    Action::Add(person.clone()),
                    Action::Update(
                        person.id.clone(),
                        UpdatePersonData {
                            full_name: UpdateAction::Set("2".to_string()),
                            email: UpdateAction::NoChanges,
                        },
                    ),
                ];

                // When we query the table with the old state
                let list_action = Action::List(Some(QueryPersonData {
                    full_name: QueryMatch::Value("1".to_string()),
                    email: QueryMatch::Any,
                }));

                // Then we should get no items back
                list_test(seed_actions, list_action, vec![]);
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
                    Action::Add(person.clone()),
                    Action::Remove(person.id.clone()),
                ];

                // When we select all items
                let list_action = Action::List(None);

                // Then we should get no items back
                list_test(seed_actions, list_action, vec![]);
            }

            #[test]
            fn list_should_not_return_deleted_values() {
                // Given there is a table of one person (added, then updated)
                let person = Person::new("1".to_string(), Some("1".to_string()));

                let seed_actions = vec![
                    Action::Add(person.clone()),
                    Action::Remove(person.id.clone()),
                ];

                // When we select all items
                let list_action = Action::List(Some(QueryPersonData {
                    full_name: QueryMatch::Any,
                    email: QueryMatch::Any,
                }));

                // Then we should get no items back
                list_test(seed_actions, list_action, vec![]);
            }
        }

        pub fn list_test(actions: Vec<Action>, list_action: Action, compare_to: Vec<Person>) -> () {
            let mut table = PersonTable::new();
            let mut next_transaction_id = TransactionId::new_first_transaction();

            for action in actions {
                table.apply(action, next_transaction_id.clone()).unwrap();
                next_transaction_id = next_transaction_id.increment();
            }

            let mut list_results = table
                .apply(list_action, next_transaction_id)
                .unwrap()
                .list();

            let mut sort_compare_to = compare_to;

            sort_list(&mut list_results);
            sort_list(&mut sort_compare_to);

            assert_eq!(&list_results, &sort_compare_to);
        }
    }

    mod versioning {
        use super::*;

        /// Tests are broken up into three categories:
        /// - Get action
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
                let person_row = table.person_rows.get(&person.id).expect("should have row");

                assert_eq!(person_row.version_count(), 1);

                assert_eq!(
                    person_row.at_version(VersionId(1)),
                    Some(PersonVersion {
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
                let (updated_person, _) =
                    update_test_person(&mut table, &person, next_transaction_id);

                // Then we should have: two versions, at version 1 and 2, with transaction id 1 and 2
                let person_row = table
                    .person_rows
                    .get(&person.id)
                    .expect("should have a row");

                assert_eq!(person_row.version_count(), 2);

                assert_eq!(
                    person_row.at_version(VersionId(1)),
                    Some(PersonVersion {
                        state: PersonVersionState::State(person),
                        version: VersionId(1),
                        transaction_id: TransactionId(1),
                    })
                );

                assert_eq!(
                    person_row.at_version(VersionId(2)),
                    Some(PersonVersion {
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
                let (add_person, next_transaction_id) =
                    add_test_person_to_empty_database(&mut table);

                // And we update the item
                let (updated_person, next_transaction_id) =
                    update_test_person(&mut table, &add_person, next_transaction_id);

                // And we delete the item
                let _ = delete_test_person(&mut table, &updated_person.id, next_transaction_id);

                // Then we should have: two versions, at version 1 and 2, with transaction id 1 and 2
                let person_row = table
                    .person_rows
                    .get(&updated_person.id)
                    .expect("should have a row");

                assert_eq!(person_row.version_count(), 3);

                assert_eq!(
                    person_row.at_version(VersionId(1)),
                    Some(PersonVersion {
                        state: PersonVersionState::State(add_person),
                        version: VersionId(1),
                        transaction_id: TransactionId(1),
                    })
                );

                assert_eq!(
                    person_row.at_version(VersionId(2)),
                    Some(PersonVersion {
                        state: PersonVersionState::State(updated_person),
                        version: VersionId(2),
                        transaction_id: TransactionId(2),
                    })
                );

                assert_eq!(
                    person_row.at_version(VersionId(3)),
                    Some(PersonVersion {
                        state: PersonVersionState::Delete,
                        version: VersionId(3),
                        transaction_id: TransactionId(3),
                    })
                );
            }
        }

        mod get_action {
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
    }

    mod uniqueness_constraint {
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

            // Then we should hit a uniqueness constraint
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

        /// This caused a bug where we could not update ourself to the same email
        #[test]
        fn updating_item_value_to_itself_does_not_break_uniqueness_constraint() {
            // Given a table with an that has a unique email
            let mut table = PersonTable::new();

            let person = Person::new("1".to_string(), Some("email".to_string()));
            let add_action = Action::Add(person.clone());

            table
                .apply(add_action, TransactionId(1))
                .expect("should not throw an error because there is no table data");

            // When we update ourself to the same email
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

            // Then the update should succeed
            assert_eq!(result, ActionResult::Single(person));
        }
    }

    fn add_test_person_to_empty_database(table: &mut PersonTable) -> (Person, TransactionId) {
        let transaction_id = TransactionId::new_first_transaction();
        add_test_person(table, transaction_id)
    }

    fn add_test_person(
        table: &mut PersonTable,
        next_transaction_id: TransactionId,
    ) -> (Person, TransactionId) {
        let person = Person::new(
            next_transaction_id.to_string(),
            Some(next_transaction_id.to_string()),
        );
        let action = Action::Add(person.clone());

        table.apply(action, next_transaction_id.clone()).unwrap();

        (person, next_transaction_id.increment())
    }

    fn update_test_person(
        table: &mut PersonTable,
        person: &Person,
        next_transaction_id: TransactionId,
    ) -> (Person, TransactionId) {
        let mut updated_person = person.clone();
        updated_person.email = Some("email".to_string());

        let action = Action::Update(
            person.id.clone(),
            UpdatePersonData {
                full_name: UpdateAction::NoChanges,
                email: UpdateAction::Set("email".to_string()),
            },
        );

        table.apply(action, next_transaction_id.clone()).unwrap();

        (updated_person, next_transaction_id.increment())
    }

    fn delete_test_person(
        table: &mut PersonTable,
        id: &EntityId,
        next_transaction_id: TransactionId,
    ) -> TransactionId {
        let action = Action::Remove(id.clone());

        table.apply(action, next_transaction_id.clone()).unwrap();

        next_transaction_id.increment()
    }

    fn get_test_person_at_version(
        table: &mut PersonTable,
        id: &EntityId,
        version: &VersionId,
        next_transaction_id: TransactionId,
    ) -> Option<Person> {
        let action = Action::GetVersion(id.clone(), version.clone());
        let result = table.apply(action, next_transaction_id).unwrap();

        match result {
            ActionResult::GetSingle(person) => person,
            _ => {
                // Note: Unsure why but cannot panic here, just assert false
                assert!(false, "should be a single person");
                None
            }
        }
    }
}
