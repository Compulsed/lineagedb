use juniper::{
    graphql_value, EmptySubscription, FieldError, FieldResult, GraphQLScalarValue, RootNode,
};
use std::sync::Mutex;
use uuid::Uuid;

use crate::{
    consts::consts::EntityId,
    database::{
        request_manager::RequestManager,
        table::row::{UpdateAction, UpdatePersonData},
    },
    graphql,
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
        return Person {
            id: EntityId(Uuid::new_v4().to_string()),
            full_name: self.full_name,
            email: self.email,
        };
    }
}

// #[derive(GraphQLInputObject)]
// #[graphql(description = "A humanoid creature in the Star Wars universe")]
// pub struct UpdateHumanData {
//     pub full_name: Option<String>,
//     pub email: Option<String>,
// }

// impl UpdateHumanData {
//     pub fn to_person_update(self) -> UpdatePersonData {
//         UpdatePersonData {
//             full_name: self.full_name.to_person_update(),
//             email: self.email.to_person_update(),
//         }
//     }
// }

// #[derive(GraphQLInputObject)]
// #[graphql(description = "A humanoid creature in the Star Wars universe")]
// pub enum UpdateHumanAction {
//     Set(String),
//     Unset,
//     NoChanges,
// }

// impl UpdateHumanAction {
//     pub fn to_person_update(self) -> UpdateAction {
//         match self {
//             UpdateHumanAction::Set(s) => UpdateAction::Set(s),
//             UpdateHumanAction::Unset => UpdateAction::Unset,
//             UpdateHumanAction::NoChanges => UpdateAction::NoChanges,
//         }
//     }
// }

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

        return Ok(None);
    }

    fn list_human(context: &'db GraphQLContext) -> FieldResult<Vec<Human>> {
        let database = context.request_manager.lock().unwrap();

        let db_response = database
            .send_request(Action::List)
            .expect("Should not timeout");

        let humans = db_response
            .list()
            .into_iter()
            .map(|p| Human::from_person(p))
            .collect();

        return Ok(humans);
    }
}

pub struct MutationRoot;

#[juniper::graphql_object(context = GraphQLContext)]
impl MutationRoot {
    fn create_human(new_human: NewHuman, context: &'db GraphQLContext) -> FieldResult<Human> {
        let database = context.request_manager.lock().unwrap();

        let person = new_human.to_person();

        let add_transaction = Action::Add(person.clone());

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

    fn update_human(id: String, context: &'db GraphQLContext) -> FieldResult<Human> {
        Ok(Human {
            id: "Fake Id".to_string(),
            full_name: "Fake Fullname".to_string(),
            email: Some("fake@gmail.com".to_string()),
        })
    }
}

pub type Schema = RootNode<'static, QueryRoot, MutationRoot, EmptySubscription<GraphQLContext>>;

pub fn create_schema() -> Schema {
    Schema::new(QueryRoot {}, MutationRoot {}, EmptySubscription::new())
}
