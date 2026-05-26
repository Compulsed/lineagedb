use std::collections::HashMap;

use crate::{
    consts::consts::{EntityId, TransactionId},
    model::{
        person::Person,
        statement::{Statement, StatementResult},
    },
};

use super::{
    query::filter,
    row::{apply_update_to_person, PersonVersionState},
    table::{ApplyErrors, PersonTable},
};

/// Buffers the writes of an in-flight transaction. Nothing a transaction writes becomes
/// visible to other transactions until it commits; until then its writes live here. At
/// commit the buffered versions are published into the table atomically (under the commit
/// lock) with a single commit timestamp.
///
/// This is the key building block that fixes the Stage 0 failures: because writes are held
/// here and only published all-at-once, a reader can never observe a half-applied or
/// rolled-back transaction (see `docs/mvcc-problem-and-solution.md`).
pub struct WriteSet {
    /// Entities written, in first-seen order. No duplicates: a transaction produces at most
    /// one version per entity (its final state), so writing the same entity twice collapses
    /// into a single published version.
    order: Vec<EntityId>,
    /// Final buffered state per entity, for read-your-writes resolution and publishing.
    latest: HashMap<EntityId, PersonVersionState>,
}

impl WriteSet {
    pub fn new() -> Self {
        Self {
            order: Vec::new(),
            latest: HashMap::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.order.is_empty()
    }

    /// The entities this transaction wrote, used for write-write conflict detection.
    pub fn written_entities(&self) -> &[EntityId] {
        &self.order
    }

    /// The final state per written entity, in first-seen order, consumed for publishing.
    pub fn into_publish_set(mut self) -> Vec<(EntityId, PersonVersionState)> {
        self.order
            .into_iter()
            .map(|id| {
                let state = self
                    .latest
                    .remove(&id)
                    .expect("every ordered entity has a recorded state");
                (id, state)
            })
            .collect()
    }

    fn record(&mut self, id: EntityId, state: PersonVersionState) {
        if !self.latest.contains_key(&id) {
            self.order.push(id.clone());
        }
        self.latest.insert(id, state);
    }

    /// Resolves the entity as this transaction currently sees it: its own buffered writes
    /// first (read-your-writes), then the committed table at the transaction's snapshot.
    fn resolve(
        &self,
        table: &PersonTable,
        snapshot: &TransactionId,
        id: &EntityId,
    ) -> Option<Person> {
        match self.latest.get(id) {
            Some(PersonVersionState::State(person)) => Some(person.clone()),
            Some(PersonVersionState::Delete) => None,
            None => table.read_at_snapshot(id, snapshot),
        }
    }

    /// Executes a mutation against the currently-resolved state and records the resulting
    /// version in the buffer. Does not touch the shared table. Returns the result the
    /// caller should report once the transaction commits.
    pub fn apply_mutation(
        &mut self,
        table: &PersonTable,
        snapshot: &TransactionId,
        statement: Statement,
    ) -> Result<StatementResult, ApplyErrors> {
        let id = statement
            .mutation_entity_id()
            .expect("apply_mutation requires a mutation statement");

        let current = self.resolve(table, snapshot, &id);

        let (new_state, result) = match statement {
            Statement::Add(person) => {
                if current.is_some() {
                    return Err(ApplyErrors::CannotCreateWhenAlreadyExists(person.id));
                }
                (
                    PersonVersionState::State(person.clone()),
                    StatementResult::Single(person),
                )
            }
            Statement::Update(update_id, update) => {
                let previous =
                    current.ok_or(ApplyErrors::CannotUpdateDoesNotExist(update_id.clone()))?;
                let updated = apply_update_to_person(&previous, &update)?;
                (
                    PersonVersionState::State(updated.clone()),
                    StatementResult::Single(updated),
                )
            }
            Statement::Remove(remove_id) => {
                let previous =
                    current.ok_or(ApplyErrors::CannotDeleteDoesNotExist(remove_id.clone()))?;
                (PersonVersionState::Delete, StatementResult::Single(previous))
            }
            Statement::Get(_)
            | Statement::GetVersion(_, _)
            | Statement::List(_)
            | Statement::ListLatestVersions => {
                panic!("apply_mutation requires a mutation statement")
            }
        };

        self.record(id, new_state);

        Ok(result)
    }

    /// Executes a read statement within a write transaction, overlaying the transaction's own
    /// buffered writes on top of the committed snapshot (read-your-writes):
    /// - `Get` returns the buffered value if the entity was written in this transaction.
    /// - `List` starts from the committed rows visible at the snapshot and applies this
    ///   transaction's buffered inserts/updates/deletes before filtering.
    ///
    /// `GetVersion` / `ListLatestVersions` are point-in-time / version-history reads that read
    /// the committed snapshot only (overlaying them is not meaningful).
    pub fn query(
        &self,
        table: &PersonTable,
        snapshot: &TransactionId,
        statement: Statement,
    ) -> Result<StatementResult, ApplyErrors> {
        match statement {
            Statement::Get(id) => {
                if self.latest.contains_key(&id) {
                    Ok(StatementResult::GetSingle(self.resolve(table, snapshot, &id)))
                } else {
                    table.query_statement(Statement::Get(id), snapshot)
                }
            }
            Statement::List(query_person_data) => {
                let mut people = self.overlaid_list(table, snapshot);

                if let Some(query) = query_person_data {
                    people = filter(people, query);
                }

                people.sort_by(|a, b| a.id.cmp(&b.id));

                Ok(StatementResult::List(people))
            }
            other => table.query_statement(other, snapshot),
        }
    }

    /// The committed rows visible at `snapshot`, overlaid with this transaction's buffered
    /// writes (inserts/updates applied, deletes removed). Unfiltered and unsorted.
    fn overlaid_list(&self, table: &PersonTable, snapshot: &TransactionId) -> Vec<Person> {
        let committed = match table.query_statement(Statement::List(None), snapshot) {
            Ok(StatementResult::List(people)) => people,
            _ => Vec::new(),
        };

        let mut by_id: HashMap<EntityId, Person> = committed
            .into_iter()
            .map(|person| (person.id.clone(), person))
            .collect();

        for (id, state) in &self.latest {
            match state {
                PersonVersionState::State(person) => {
                    by_id.insert(id.clone(), person.clone());
                }
                PersonVersionState::Delete => {
                    by_id.remove(id);
                }
            }
        }

        by_id.into_values().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::table::row::{UpdatePersonData, UpdateStatement};

    fn person(id: &str, full_name: &str) -> Person {
        Person {
            id: EntityId(id.to_string()),
            full_name: full_name.to_string(),
            email: None,
        }
    }

    fn names(result: StatementResult) -> Vec<String> {
        match result {
            StatementResult::List(people) => people.into_iter().map(|p| p.full_name).collect(),
            other => panic!("expected List, got {:?}", other),
        }
    }

    #[test]
    fn list_overlays_buffered_inserts_and_deletes() {
        let table = PersonTable::new();
        // Committed at snapshot 1: Alice.
        table
            .apply(Statement::Add(person("1", "Alice")), TransactionId(1))
            .unwrap();

        let snapshot = TransactionId(1);
        let mut write_set = WriteSet::new();
        // In the transaction: add Bob, delete Alice.
        write_set
            .apply_mutation(&table, &snapshot, Statement::Add(person("2", "Bob")))
            .unwrap();
        write_set
            .apply_mutation(&table, &snapshot, Statement::Remove(EntityId("1".to_string())))
            .unwrap();

        // The transaction sees its own writes: Bob present, Alice gone.
        let result = write_set
            .query(&table, &snapshot, Statement::List(None))
            .unwrap();
        assert_eq!(names(result), vec!["Bob".to_string()]);

        // The committed table at the snapshot is untouched (still just Alice).
        let committed = table.query_statement(Statement::List(None), &snapshot).unwrap();
        assert_eq!(names(committed), vec!["Alice".to_string()]);
    }

    #[test]
    fn list_overlays_buffered_update() {
        let table = PersonTable::new();
        table
            .apply(Statement::Add(person("1", "Alice")), TransactionId(1))
            .unwrap();

        let snapshot = TransactionId(1);
        let mut write_set = WriteSet::new();
        write_set
            .apply_mutation(
                &table,
                &snapshot,
                Statement::Update(
                    EntityId("1".to_string()),
                    UpdatePersonData {
                        full_name: UpdateStatement::Set("Alice 2".to_string()),
                        email: UpdateStatement::NoChanges,
                    },
                ),
            )
            .unwrap();

        let result = write_set
            .query(&table, &snapshot, Statement::List(None))
            .unwrap();
        assert_eq!(names(result), vec!["Alice 2".to_string()]);
    }
}
