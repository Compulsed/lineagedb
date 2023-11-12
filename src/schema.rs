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

#[derive(GraphQLEnum)]
enum Episode {
    NewHope,
    Empire,
    Jedi,
}

use juniper::{GraphQLEnum, GraphQLInputObject, GraphQLObject};

#[derive(GraphQLObject)]
#[graphql(description = "A humanoid creature in the Star Wars universe")]
struct Human {
    id: String,
    name: String,
    appears_in: Vec<Episode>,
    home_planet: String,
}

#[derive(GraphQLInputObject)]
#[graphql(description = "A humanoid creature in the Star Wars universe")]
struct NewHuman {
    name: String,
    appears_in: Vec<Episode>,
    home_planet: String,
}

pub struct QueryRoot;

#[juniper::graphql_object(context = GraphQLContext)]
impl QueryRoot {
    fn human(_id: String) -> FieldResult<Human> {
        Ok(Human {
            id: "1234".to_owned(),
            name: "Luke".to_owned(),
            appears_in: vec![Episode::NewHope],
            home_planet: "Mars".to_owned(),
        })
    }
}

pub struct MutationRoot;

#[juniper::graphql_object(context = GraphQLContext)]
impl MutationRoot {
    fn create_human(new_human: NewHuman, context: &'db GraphQLContext) -> FieldResult<Human> {
        let id = Uuid::new_v4();

        let human = Human {
            id: id.to_string(),
            name: new_human.name.clone(),
            appears_in: new_human.appears_in,
            home_planet: new_human.home_planet,
        };

        let add_transaction = Action::Add(Person {
            id: id.to_string(),
            full_name: new_human.name,
            email: Some(format!("Fake Email - {}", id.to_string())),
        });

        let data = context.request_manager.lock().unwrap();

        let db_response = data
            .send_request(add_transaction)
            .expect("Should not timeout");

        println!("{:?}", db_response);

        Ok(human)
    }
}

pub type Schema = RootNode<'static, QueryRoot, MutationRoot, EmptySubscription<GraphQLContext>>;

pub fn create_schema() -> Schema {
    Schema::new(QueryRoot {}, MutationRoot {}, EmptySubscription::new())
}
