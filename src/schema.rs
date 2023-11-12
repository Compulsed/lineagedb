use juniper::{EmptySubscription, FieldResult, RootNode};
use std::sync::Mutex;

use crate::{
    database::request_manager::RequestManager,
    model::action::{Action, ActionResult},
};

pub struct RequestManagerContext(pub Mutex<RequestManager>);

// Mark the Database as a valid context type for Juniper
impl juniper::Context for RequestManagerContext {}

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

#[juniper::graphql_object(context = RequestManagerContext)]
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

#[juniper::graphql_object(context = RequestManagerContext)]
impl MutationRoot {
    fn create_human(
        new_human: NewHuman,
        context: &'db RequestManagerContext,
    ) -> FieldResult<Human> {
        let mut human = Human {
            id: "1234".to_owned(),
            name: new_human.name,
            appears_in: new_human.appears_in,
            home_planet: new_human.home_planet,
        };

        // let data = context.0.lock().unwrap();

        // let person_result = data
        //     .send_request(Action::List(10000000))
        //     .expect("Should not timeout");

        // if let ActionResult::List(l) = person_result {
        //     human.name = l[0].full_name.clone()
        // }

        Ok(human)
    }
}

pub type Schema =
    RootNode<'static, QueryRoot, MutationRoot, EmptySubscription<RequestManagerContext>>;

pub fn create_schema() -> Schema {
    Schema::new(QueryRoot {}, MutationRoot {}, EmptySubscription::new())
}
