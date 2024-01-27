use database::{
    consts::consts::{EntityId, VersionId, VersionIdVersionError},
    database::{
        request_manager::RequestManager,
        table::row::{UpdateAction, UpdatePersonData},
    },
    model::{action::Action, person::Person},
};
use juniper::{graphql_value, EmptySubscription, FieldError, FieldResult, Nullable, RootNode};
use std::sync::Mutex;
use uuid::Uuid;

pub struct GraphQLContext {
    pub request_manager: Mutex<RequestManager>,
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

pub struct QueryRoot;

#[juniper::graphql_object(context = GraphQLContext)]
impl QueryRoot {
    fn human(
        id: String,
        version_id: Option<i32>,
        context: &'db GraphQLContext,
    ) -> FieldResult<Option<Human>> {
        let database = context.request_manager.lock().unwrap();

        let entity_id = EntityId(id);

        let optional_person = match version_id {
            Some(v) => database.send_get_version(entity_id, v.try_into()?)?,
            None => database.send_get(entity_id)?,
        };

        Ok(optional_person.and_then(|p| Some(Human::from_person(p))))
    }

    fn list_human(context: &'db GraphQLContext) -> FieldResult<Vec<Human>> {
        let database = context.request_manager.lock().unwrap();

        let result = database
            .send_list()?
            .into_iter()
            .map(Human::from_person)
            .collect();

        return Ok(result);
    }
}

pub struct MutationRoot;

#[juniper::graphql_object(context = GraphQLContext)]
impl MutationRoot {
    fn create_human(new_human: NewHuman, context: &'db GraphQLContext) -> FieldResult<Human> {
        let database = context.request_manager.lock().unwrap();

        // Might seem a bit weird, but this is to ensure that the id is unique
        let new_person = database.send_add(new_human.to_person())?;

        Ok(Human::from_person(new_person))
    }

    fn create_humans(
        new_humans: Vec<NewHuman>,
        context: &'db GraphQLContext,
    ) -> FieldResult<Vec<Human>> {
        let database = context.request_manager.lock().unwrap();

        let add_people = new_humans
            .into_iter()
            .map(NewHuman::to_person)
            .map(Action::Add)
            .collect();

        // TODO: In this context we can use single, but, because it can panic an exception
        //  we probably shouldn't
        let humans = database
            .send_transaction(add_people)?
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
        let database = context.request_manager.lock().unwrap();

        let full_name_update = match update_human.full_name {
            Nullable::ImplicitNull => UpdateAction::NoChanges,
            Nullable::ExplicitNull => UpdateAction::Unset,
            Nullable::Some(t) => UpdateAction::Set(t),
        };

        let email_update = match update_human.email {
            Nullable::ImplicitNull => UpdateAction::NoChanges,
            Nullable::ExplicitNull => UpdateAction::Unset,
            Nullable::Some(t) => UpdateAction::Set(t),
        };

        let update_person_date = UpdatePersonData {
            full_name: full_name_update,
            email: email_update,
        };

        let person = database.send_update(EntityId(id), update_person_date)?;

        Ok(Human::from_person(person))
    }
}

pub type Schema = RootNode<'static, QueryRoot, MutationRoot, EmptySubscription<GraphQLContext>>;

pub fn create_schema() -> Schema {
    Schema::new(QueryRoot {}, MutationRoot {}, EmptySubscription::new())
}
