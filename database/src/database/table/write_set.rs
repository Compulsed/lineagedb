use std::collections::HashMap;

use crate::{
    consts::consts::{EntityId, TransactionId},
    model::{
        person::Person,
        statement::{Statement, StatementResult},
    },
};

use super::{
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

    /// Executes a read statement within a write transaction. A `Get` consults this
    /// transaction's own buffered writes first (read-your-writes); other reads are served
    /// from the committed table at the transaction's snapshot.
    ///
    /// NOTE: `List`/`ListLatestVersions`/`GetVersion` currently read the committed snapshot
    /// only and do not overlay this transaction's not-yet-committed writes. Single-shot
    /// transactions rarely mix these reads with writes; full overlay is deferred to the
    /// long-lived-transaction work (Stage 5).
    pub fn query(
        &self,
        table: &PersonTable,
        snapshot: &TransactionId,
        statement: Statement,
    ) -> Result<StatementResult, ApplyErrors> {
        if let Statement::Get(id) = &statement {
            if self.latest.contains_key(id) {
                return Ok(StatementResult::GetSingle(self.resolve(table, snapshot, id)));
            }
        }

        table.query_statement(statement, snapshot)
    }
}
