use database::{
    consts::consts::EntityId,
    database::{
        request_manager::RequestManager,
        table::row::{UpdateAction, UpdatePersonData},
    },
    model::{
        action::{Action, ActionResult},
        person::Person,
    },
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
    fn human(id: String, context: &'db GraphQLContext) -> FieldResult<Option<Human>> {
        let database = context.request_manager.lock().unwrap();

        let db_response = database
            .send_request(Action::Get(EntityId(id)))
            .expect("Should not timeout");

        if let Some(person) = db_response.get_single() {
            return Ok(Some(Human::from_person(person)));
        }

        Ok(None)
    }

    fn list_human(context: &'db GraphQLContext) -> FieldResult<Vec<Human>> {
        let database = context.request_manager.lock().unwrap();

        let db_response = database
            .send_request(Action::List)
            .expect("Should not timeout");

        let humans = db_response
            .list()
            .into_iter()
            .map(Human::from_person)
            .collect();

        Ok(humans)
    }
}

pub struct MutationRoot;

#[juniper::graphql_object(context = GraphQLContext)]
impl MutationRoot {
    fn create_human(new_human: NewHuman, context: &'db GraphQLContext) -> FieldResult<Human> {
        let database = context.request_manager.lock().unwrap();

        let person = new_human.to_person();

        let add_transaction = Action::Add(person);

        let db_response = database
            .send_request(add_transaction)
            .expect("Should not timeout");

        println!("{:?}", db_response);

        if let ActionResult::ErrorStatus(s) = db_response {
            return Err(FieldError::new(
                s.clone(),
                graphql_value!({ "bad_request": s }),
            ));
        }

        Ok(Human::from_person(db_response.single()))
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

        let update_transaction = Action::Update(EntityId(id), update_person_date);

        let db_response = database
            .send_request(update_transaction)
            .expect("Should not timeout");

        // TODO: Centralize error handling responses
        match db_response {
            ActionResult::Single(p) => Ok(Human::from_person(p)),
            ActionResult::ErrorStatus(s) => Err(FieldError::new(
                s.clone(),
                graphql_value!({ "BadRequest": s }),
            )),
            _ => Err(FieldError::new(
                "Unexpected response from database".to_string(),
                graphql_value!({ "InternalError": "Unexpected response from database" }),
            )),
        }
    }
}

pub type Schema = RootNode<'static, QueryRoot, MutationRoot, EmptySubscription<GraphQLContext>>;

pub fn create_schema() -> Schema {
    Schema::new(QueryRoot {}, MutationRoot {}, EmptySubscription::new())
}
