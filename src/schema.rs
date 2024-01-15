use juniper::{EmptySubscription, FieldResult, RootNode};
use std::sync::Mutex;
use uuid::Uuid;

use crate::{
    database::request_manager::RequestManager,
    model::{
        action::{Action, ActionResult},
        person::Person,
    },
};

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
    pub fn from_person(person: Option<Person>) -> Option<Human> {
        if let Some(person) = person {
            return Some(Human {
                id: person.id,
                full_name: person.full_name,
                email: person.email,
            });
        }

        return None;
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
        return Person {
            id: Uuid::new_v4().to_string(),
            full_name: self.full_name,
            email: self.email,
        };
    }
}

pub struct QueryRoot;

#[juniper::graphql_object(context = GraphQLContext)]
impl QueryRoot {
    fn human(id: String, context: &'db GraphQLContext) -> FieldResult<Option<Human>> {
        let get_transaction = Action::Get(id);

        let data = context.request_manager.lock().unwrap();

        let db_response = data
            .send_request(get_transaction)
            .expect("Should not timeout");

        // TODO: Convert to a from trait
        if let ActionResult::Single(h) = db_response {
            return Ok(Human::from_person(h));
        }

        panic!("Error")
    }
}

pub struct MutationRoot;

#[juniper::graphql_object(context = GraphQLContext)]
impl MutationRoot {
    fn create_human(new_human: NewHuman, context: &'db GraphQLContext) -> FieldResult<Human> {
        let person = new_human.to_person();

        let add_transaction = Action::Add(person.clone());

        let data = context.request_manager.lock().unwrap();

        let db_response = data
            .send_request(add_transaction)
            .expect("Should not timeout");

        println!("{:?}", db_response);

        // TODO: This mapping feels janky, should be done on the class itself
        Ok(Human {
            id: person.id,
            full_name: person.full_name,
            email: person.email,
        })
    }
}

pub type Schema = RootNode<'static, QueryRoot, MutationRoot, EmptySubscription<GraphQLContext>>;

pub fn create_schema() -> Schema {
    Schema::new(QueryRoot {}, MutationRoot {}, EmptySubscription::new())
}
