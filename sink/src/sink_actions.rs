use crate::actions::entities::EntityAction;
use crate::actions::general::GeneralAction;
use crate::actions::spaces::SpaceAction;
use crate::actions::tables::TableAction;
use crate::constants;
use crate::models::*;
use crate::triples::ValueType;
use anyhow::Error;
use futures03::{future::try_join_all, stream::FuturesOrdered};
use migration::DbErr;
use sea_orm::ConnectionTrait;
use sea_orm::DatabaseConnection;
use sea_orm::DatabaseTransaction;
use sea_orm::TransactionTrait;
use strum::EnumIter;
use tokio_stream::StreamExt;

/// This enum represents different actions that the sink should handle. Actions being specific changes to the graph.
/// You should understand that we have two kinds of actions.
///
/// The first being: "Default actions",
/// All Sink actions will also be default actions, but not all default actions will be other sink actions.
/// Because any actions we take in the sink are coming from action triples, we are going to store these action triples in the database.
/// So the default actions are the enum variants:
/// - `SinkAction::CreateTriple`
/// - `SinkAction::DeleteTriple`
/// - `SinkAction::CreateEntity`
///
/// The reason for this is just so we can keep track of what actions we have taken in the database.
/// In addition to specific actions that should do special things, which are the rest of the variants.
#[derive(Debug, Clone)]
pub enum SinkAction {
    Table(TableAction),
    Space(SpaceAction),
    Entity(EntityAction),
    General(GeneralAction),
}

pub async fn handle_sink_actions(
    actions: &Vec<SinkAction>,
    txn: &DatabaseTransaction,
    use_space_queries: bool,
    max_connections: usize,
) -> Result<(), Error> {
    let mut futures = FuturesOrdered::new();
    let mut chunk = Vec::new();

    for action in actions.iter() {
        chunk.push(action.execute(txn, use_space_queries));

        if chunk.len() == max_connections {
            futures.push_back(try_join_all(chunk));
            chunk = Vec::new();
        }
    }

    if !chunk.is_empty() {
        futures.push_back(try_join_all(chunk));
    }

    while let Some(result) = futures.next().await {
        result?;
    }

    Ok(())
}

impl SinkAction {
    pub async fn execute(
        &self,
        db: &impl ConnectionTrait,
        space_queries: bool,
    ) -> Result<(), Error> {
        match self {
            SinkAction::Table(action) => action.execute(db, space_queries).await,
            SinkAction::Entity(action) => action.execute(db, space_queries).await,
            SinkAction::General(action) => action.execute(db).await,
            SinkAction::Space(action) => action.execute(db).await,
        }
    }

    pub fn is_default_action(&self) -> bool {
        match self {
            SinkAction::General(_) => true,
            _ => false,
        }
    }

    pub fn is_global_sink_action(&self) -> bool {
        match self {
            SinkAction::Entity(_) | SinkAction::Table(_) => true,
            _ => false,
        }
    }

    pub fn action_priority(&self) -> i32 {
        // lower priority actions should be executed first
        match self {
            SinkAction::General(_) => 1,
            SinkAction::Entity(_) => 2,
            SinkAction::Space(_) => 3,
            SinkAction::Table(_) => 4,
        }
    }
}

// #[cfg(test)]
// mod tests {
//     use super::SinkAction;
//     use crate::constants::{Attributes, Entities};
//     use crate::triples::{ActionTriple, ValueType};
//     use strum::IntoEnumIterator;

//     const ENTITY_ID: &'static str = "sample-entity-id";

//     fn dummy_triple_for_action(sink_action: &SinkAction) -> ActionTriple {
//         match sink_action {
//             SinkAction::SpaceCreated { .. } => ActionTriple::CreateTriple {
//                 entity_id: ENTITY_ID.to_string(),
//                 attribute_id: Attributes::Space.id().to_string(),
//                 value: ValueType::String {
//                     id: "string-id".to_string(),
//                     value: "0xSpaceAddress".to_string(),
//                 },
//                 space: "space".to_string(),
//                 author: "author".to_string(),
//             },

//             SinkAction::TypeAdded { .. } => ActionTriple::CreateTriple {
//                 entity_id: ENTITY_ID.to_string(),
//                 attribute_id: Attributes::Type.id().to_string(),
//                 value: ValueType::Entity {
//                     id: "some-type-id".to_string(),
//                 },
//                 space: "space".to_string(),
//                 author: "author".to_string(),
//             },

//             SinkAction::AttributeAdded { .. } => ActionTriple::CreateTriple {
//                 entity_id: ENTITY_ID.to_string(),
//                 attribute_id: Attributes::Attribute.id().to_string(),
//                 value: ValueType::Entity {
//                     id: "Whatever".to_string(),
//                 },
//                 space: "space".to_string(),
//                 author: "author".to_string(),
//             },

//             SinkAction::NameAdded { .. } => ActionTriple::CreateTriple {
//                 entity_id: ENTITY_ID.to_string(),
//                 attribute_id: Attributes::Name.id().to_string(),
//                 value: ValueType::String {
//                     id: "String Id".to_string(),
//                     value: "Some Name".to_string(),
//                 },
//                 space: "space".to_string(),
//                 author: "author".to_string(),
//             },

//             SinkAction::DescriptionAdded { .. } => ActionTriple::CreateTriple {
//                 entity_id: ENTITY_ID.to_string(),
//                 attribute_id: Attributes::Description.id().to_string(),
//                 value: ValueType::String {
//                     id: "String Id".to_string(),
//                     value: "Some Description".to_string(),
//                 },
//                 space: "space".to_string(),
//                 author: "author".to_string(),
//             },

//             SinkAction::CoverAdded { .. } => ActionTriple::CreateTriple {
//                 entity_id: ENTITY_ID.to_string(),
//                 attribute_id: Attributes::Cover.id().to_string(),
//                 value: ValueType::String {
//                     id: "String Id".to_string(),
//                     value: "Some Cover Link".to_string(),
//                 },
//                 space: "space".to_string(),
//                 author: "author".to_string(),
//             },

//             SinkAction::AvatarAdded { .. } => ActionTriple::CreateTriple {
//                 entity_id: ENTITY_ID.to_string(),
//                 attribute_id: Attributes::Avatar.id().to_string(),
//                 value: ValueType::String {
//                     id: "String Id".to_string(),
//                     value: "Some Avatar Link".to_string(),
//                 },
//                 space: "space".to_string(),
//                 author: "author".to_string(),
//             },

//             SinkAction::ValueTypeAdded { .. } => ActionTriple::CreateTriple {
//                 entity_id: ENTITY_ID.to_string(),
//                 attribute_id: Attributes::ValueType.id().to_string(),
//                 value: ValueType::Entity {
//                     id: "Some Value Type Id".to_string(),
//                 },
//                 space: "space".to_string(),
//                 author: "author".to_string(),
//             },

//             SinkAction::SubspaceAdded { .. } => ActionTriple::CreateTriple {
//                 entity_id: ENTITY_ID.to_string(),
//                 attribute_id: Attributes::Subspace.id().to_string(),
//                 value: ValueType::Entity {
//                     id: "Some other space".to_string(),
//                 },
//                 space: "space".to_string(),
//                 author: "author".to_string(),
//             },

//             SinkAction::SubspaceRemoved { .. } => ActionTriple::DeleteTriple {
//                 entity_id: ENTITY_ID.to_string(),
//                 attribute_id: Attributes::Subspace.id().to_string(),
//                 value: ValueType::Entity {
//                     id: "Some other space".to_string(),
//                 },
//                 space: "space".to_string(),
//                 author: "author".to_string(),
//             },

//             SinkAction::TripleAdded { .. } => ActionTriple::CreateTriple {
//                 entity_id: ENTITY_ID.to_string(),
//                 attribute_id: Attributes::Subspace.id().to_string(),
//                 value: ValueType::Entity {
//                     id: "Some other space".to_string(),
//                 },
//                 space: "space".to_string(),
//                 author: "author".to_string(),
//             },

//             SinkAction::EntityCreated { .. } => ActionTriple::CreateEntity {
//                 entity_id: "entity-id".to_string(),
//                 space: "space".to_string(),
//                 author: "author".to_string(),
//             },

//             SinkAction::TripleDeleted { .. } => ActionTriple::DeleteTriple {
//                 entity_id: ENTITY_ID.to_string(),
//                 attribute_id: Attributes::Subspace.id().to_string(),
//                 value: ValueType::Entity {
//                     id: "Some other space".to_string(),
//                 },
//                 space: "space".to_string(),
//                 author: "author".to_string(),
//             },
//         }
//     }

//     #[test]
//     fn test_each_sink_action() {
//         for action in SinkAction::iter() {
//             let triple = dummy_triple_for_action(&action);

//             if action.is_default_action() {
//                 // if it is a default action, we don't need to test it
//                 continue;
//             } else {
//                 let sink_action: Option<SinkAction> = triple.try_into().ok();

//                 assert!(
//                     sink_action.is_some(),
//                     "couldnt match for sink action {:?}",
//                     action
//                 );
//             }
//         }
//     }
// }
