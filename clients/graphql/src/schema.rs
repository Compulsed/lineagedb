use std::time::Duration;

use database::{
    consts::consts::EntityId,
    database::{
        commands::{SnapshotTimestamp, TransactionContext},
        request_manager::RequestManager,
        table::{
            query::{QueryMatch, QueryPersonData},
            row::{UpdatePersonData, UpdateStatement},
        },
    },
    model::{person::Person, statement::Statement},
};
use juniper::{EmptySubscription, FieldResult, Nullable, RootNode};
use uuid::Uuid;

pub struct GraphQLContext {
    pub request_manager: RequestManager,
}

// https://graphql-rust.github.io/juniper/master/types/objects/using_contexts.html
impl juniper::Context for GraphQLContext {}

use juniper::{GraphQLInputObject, GraphQLObject};

#[derive(GraphQLObject)]
#[graphql(description = "A humanoid creature in the Star Wars universe")]
struct Human {
    pub id: String,
    pub full_name: String,
    pub email: Option<String>,
}

impl Human {
    pub fn from_person(person: Person) -> Human {
        Human {
            id: person.id.to_string(),
            full_name: person.full_name,
            email: person.email,
        }
    }
}

#[derive(GraphQLInputObject)]
#[graphql(description = "A humanoid creature in the Star Wars universe")]
struct NewHuman {
    pub full_name: String,
    pub email: Option<String>,
}

impl NewHuman {
    pub fn to_person(self) -> Person {
        Person {
            id: EntityId(Uuid::new_v4().to_string()),
            full_name: self.full_name,
            email: self.email,
        }
    }
}

#[derive(GraphQLInputObject)]
#[graphql(description = "A humanoid creature in the Star Wars universe")]
pub struct UpdateHumanData {
    pub full_name: Nullable<String>,
    pub email: Nullable<String>,
}

#[derive(GraphQLInputObject)]
#[graphql(description = "A humanoid creature in the Star Wars universe")]
pub struct QueryHumanData {
    pub full_name: Nullable<String>,
    pub email: Nullable<String>,
}

pub struct QueryRoot;

#[juniper::graphql_object(context = GraphQLContext)]
impl QueryRoot {
    fn human(
        id: String,
        version_id: Option<i32>,
        snapshot_id: Nullable<i32>,
        context: &'db GraphQLContext,
    ) -> FieldResult<Option<Human>> {
        let request_manager = &context.request_manager;

        let entity_id = EntityId(id);

        let snapshot_timestamp = match snapshot_id {
            Nullable::ImplicitNull | Nullable::ExplicitNull => SnapshotTimestamp::Latest,
            Nullable::Some(t) => SnapshotTimestamp::AtTransactionId(t.into()),
        };

        let tx_context = TransactionContext::new(snapshot_timestamp);

        let optional_person = match version_id {
            Some(v) => request_manager.send_get_version(entity_id, v.try_into()?, tx_context)?,
            None => request_manager.send_get(entity_id, tx_context)?,
        };

        Ok(optional_person.and_then(|p| Some(Human::from_person(p))))
    }

    fn list_human(
        query: Nullable<QueryHumanData>,
        snapshot_id: Nullable<i32>,
        context: &'db GraphQLContext,
    ) -> FieldResult<Vec<Human>> {
        let request_manager = &context.request_manager;

        let snapshot_timestamp = match snapshot_id {
            Nullable::ImplicitNull | Nullable::ExplicitNull => SnapshotTimestamp::Latest,
            Nullable::Some(t) => SnapshotTimestamp::AtTransactionId(t.into()),
        };

        let tx_context = TransactionContext::new(snapshot_timestamp);

        let list_query = match query {
            Nullable::ImplicitNull => None,
            Nullable::ExplicitNull => None,
            Nullable::Some(t) => {
                let full_name = match t.full_name {
                    Nullable::ImplicitNull => QueryMatch::Any,
                    Nullable::ExplicitNull => QueryMatch::Null,
                    Nullable::Some(t) => QueryMatch::Value(t),
                };

                let email = match t.email {
                    Nullable::ImplicitNull => QueryMatch::Any,
                    Nullable::ExplicitNull => QueryMatch::Null,
                    Nullable::Some(t) => QueryMatch::Value(t),
                };

                Some(QueryPersonData { full_name, email })
            }
        };

        let result = request_manager
            .send_list(list_query, tx_context)?
            .into_iter()
            .map(Human::from_person)
            .collect();

        return Ok(result);
    }

    fn database_info(context: &'db GraphQLContext) -> FieldResult<Vec<String>> {
        let request_manager = &context.request_manager;

        let database_info = request_manager
            .send_info_request()?
            .into_iter()
            .map(|r| format!("[{}] {}", r.0, r.1))
            .collect();

        return Ok(database_info);
    }

    fn sleep(sleep: i32, context: &'db GraphQLContext) -> FieldResult<String> {
        let request_manager = &context.request_manager;

        let sleep_duration: Duration = Duration::from_secs(sleep as u64);

        let status = request_manager.send_sleep_request(sleep_duration)?;

        return Ok(status);
    }
}

pub struct MutationRoot;

#[juniper::graphql_object(context = GraphQLContext)]
impl MutationRoot {
    fn create_human(new_human: NewHuman, context: &'db GraphQLContext) -> FieldResult<Human> {
        let request_manager = &context.request_manager;

        let transaction_context = TransactionContext::default();

        // Might seem a bit weird, but this is to ensure that the id is unique
        let new_person = request_manager.send_add(new_human.to_person(), transaction_context)?;

        Ok(Human::from_person(new_person))
    }

    fn create_humans(
        new_humans: Vec<NewHuman>,
        context: &'db GraphQLContext,
    ) -> FieldResult<Vec<Human>> {
        let request_manager = &context.request_manager;

        let transaction_context = TransactionContext::default();

        let add_people = new_humans
            .into_iter()
            .map(NewHuman::to_person)
            .map(Statement::Add)
            .collect();

        // TODO: In this context we can use single, but, because it can panic an exception
        //  we probably shouldn't
        let humans = request_manager
            .send_transaction(add_people, transaction_context)?
            .into_iter()
            .map(|r| Human::from_person(r.single()))
            .collect();

        Ok(humans)
    }

    fn update_human(
        id: String,
        update_human: UpdateHumanData,
        context: &'db GraphQLContext,
    ) -> FieldResult<Human> {
        let request_manager = &context.request_manager;

        let transaction_context = TransactionContext::default();

        let full_name_update = match update_human.full_name {
            Nullable::ImplicitNull => UpdateStatement::NoChanges,
            Nullable::ExplicitNull => UpdateStatement::Unset,
            Nullable::Some(t) => UpdateStatement::Set(t),
        };

        let email_update = match update_human.email {
            Nullable::ImplicitNull => UpdateStatement::NoChanges,
            Nullable::ExplicitNull => UpdateStatement::Unset,
            Nullable::Some(t) => UpdateStatement::Set(t),
        };

        let update_person_date = UpdatePersonData {
            full_name: full_name_update,
            email: email_update,
        };

        let person =
            request_manager.send_update(EntityId(id), update_person_date, transaction_context)?;

        Ok(Human::from_person(person))
    }

    fn snapshot(context: &'db GraphQLContext) -> FieldResult<String> {
        let request_manager = &context.request_manager;

        let shutdown_status = request_manager.send_snapshot_request()?;

        return Ok(shutdown_status);
    }

    fn reset(context: &'db GraphQLContext) -> FieldResult<String> {
        let request_manager = &context.request_manager;

        let reset_status = request_manager.send_reset_request()?;

        return Ok(reset_status);
    }
}

pub type Schema = RootNode<'static, QueryRoot, MutationRoot, EmptySubscription<GraphQLContext>>;

pub fn create_schema() -> Schema {
    Schema::new(QueryRoot {}, MutationRoot {}, EmptySubscription::new())
}
