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

fn get_version_id(id: String, version_id: i32) -> FieldResult<Action> {
    let err: VersionIdVersionError = match VersionId::try_from(version_id) {
        Ok(v) => return Ok(Action::GetVersion(EntityId(id), v)),
        Err(e) => e,
    };

    let err_field_result = match err {
        VersionIdVersionError::NegativeOrZero(v) => FieldError::new(
            format!("VersionId must be greater than 0, got {}", v),
            graphql_value!({ "bad_request": "VersionId must be greater than 0" }),
        ),
        VersionIdVersionError::TooLarge(v) => FieldError::new(
            format!("VersionId must be less than {}, got {}", u16::MAX, v),
            graphql_value!({ "bad_request": "VersionId must be less than u16::MAX" }),
        ),
    };

    Err(err_field_result)
}

#[juniper::graphql_object(context = GraphQLContext)]
impl QueryRoot {
    fn human(
        id: String,
        version_id: Option<i32>,
        context: &'db GraphQLContext,
    ) -> FieldResult<Option<Human>> {
        let database = context.request_manager.lock().unwrap();

        let action = match version_id {
            Some(v) => get_version_id(id, v)?,
            None => Action::Get(EntityId(id)),
        };

        let db_response = database
            .send_request(vec![action])
            .expect("Should not timeout")
            .single_action_result()?;

        if let Some(person) = db_response.get_single() {
            return Ok(Some(Human::from_person(person)));
        }

        Ok(None)
    }

    fn list_human(context: &'db GraphQLContext) -> FieldResult<Vec<Human>> {
        let database = context.request_manager.lock().unwrap();

        let db_response = database
            .send_request(vec![Action::List])
            .expect("Should not timeout")
            .single_action_result()?;

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
            .send_request(vec![add_transaction])
            .expect("Should not timeout")
            .single_action_result()?
            .single();

        Ok(Human::from_person(db_response))
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

        let humans = database
            .send_request(add_people)
            .expect("Should not timeout")
            .multiple_action_result()?
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

        let update_transaction = Action::Update(EntityId(id), update_person_date);

        let db_response = database
            .send_request(vec![update_transaction])
            .expect("Should not timeout")
            .single_action_result();

        let person = match db_response {
            Ok(db_response) => db_response.single(),
            Err(e) => {
                return Err(FieldError::new(
                    e,
                    graphql_value!({ "bad_request": "Failed to update person" }),
                ))
            }
        };

        Ok(Human::from_person(person))
    }
}

pub type Schema = RootNode<'static, QueryRoot, MutationRoot, EmptySubscription<GraphQLContext>>;

pub fn create_schema() -> Schema {
    Schema::new(QueryRoot {}, MutationRoot {}, EmptySubscription::new())
}
