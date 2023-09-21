use anyhow::Error;
use futures03::future::{join_all, try_join_all};
use futures03::stream::{FuturesOrdered, FuturesUnordered};
use migration::DbErr;
use sea_orm::{DatabaseConnection, DatabaseTransaction};
use tokio::sync::mpsc::{Sender, UnboundedSender};
use tokio_stream::StreamExt;

use crate::triples::ValueType;

use crate::models::*;

#[derive(Debug)]
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
pub enum SinkAction {
    /// This action denotes a newly created space. The string is the address of the space.
    /// We care about this in the sink because when a new space is created, we need to deploy
    /// a new subgraph for that space.
    SpaceCreated { entity_id: String, space: String },

    /// This action denotes a newly created type. The string is the name of the type.
    /// We care about this in the sink because when a new type is created, we need to deploy
    /// a new subgraph for that type.
    ///
    /// When a type is created in geo, it looks like this:
    ///
    /// `(Entity, "types", TypeEntity)`
    TypeCreated {
        /// The entity ID of the type that was created.
        entity_id: String,
        /// The address of the space that this type was created in.
        space: String,
    },

    /// This action denotes a type being added to an entity in the DB
    /// Something like this:
    ///
    /// `(EntityID, "types", TypeEntity)`
    TypeAdded {
        /// The address of the space that this type was created in.
        space: String,
        /// The ID of the entity that this type was added to.
        entity_id: String,
        /// The ID of the type entity
        type_id: String,
    },

    /// We also care about an attribute being added to an entity, we need the entity ID and the space it was made in
    /// When an attribute is added to a type in geo, it looks like this:
    /// `(EntityID, "attribute", AttributeEntity)`
    ///
    /// or a practical example explaining how the goal type has a subgoal attribute
    ///
    /// `(Goal, "attribute", Subgoal)`
    AttributeAdded {
        /// The address of the space that this attribute was created in.
        space: String,
        /// The ID of the entity that this attribute was added to.
        entity_id: String,
        /// The ID of the attribute entity
        attribute_id: String,
        /// The value of the triple
        value: ValueType,
    },

    /// We care about a name being added to an entity because we need this when adding attributes to a type in the graph.
    NameAdded {
        space: String,
        entity_id: String,
        name: String,
    },

    /// We care about a ValueType being added to an entity because we need this when adding attributes to a type in the graph.
    ValueTypeAdded {
        space: String,
        entity_id: String,
        attribute_id: String,
        value_type: ValueType,
    },

    /// Spaces can have subspaces, and we need to know when a subspace is added to a space so we can deploy a new subgraph for that space.
    SubspaceAdded {
        parent_space: String,
        child_space: String,
    },

    /// Spaces can also remove subspaces, and we need to know when a subspace is removed from a space
    SubspaceRemoved {
        parent_space: String,
        child_space: String,
    },

    /// If we don't have any specific task to take, we will just add the triple to the graph
    TripleAdded {
        space: String,
        entity_id: String,
        attribute_id: String,
        value: ValueType,
        author: String,
    },

    /// If it's an entity creation action, we need to add the entity to the graph
    EntityCreated {
        space: String,
        entity_id: String,
        author: String,
    },

    /// If it's a triple deletion action, we need to remove the entity from the graph
    TripleDeleted {
        space: String,
        entity_id: String,
        attribute_id: String,
        value: ValueType,
        author: String,
    },
}

pub async fn handle_sink_actions(
    sink_actions: Vec<SinkAction>,
    db: &DatabaseConnection,
    sender: &Sender<String>,
) -> Result<(), Error> {
    let mut futures = FuturesOrdered::from_iter(
        sink_actions
            .into_iter()
            .map(|action| action.execute(db, sender)),
    );

    while let Some(result) = futures.next().await {
        result?;
    }
    Ok(())
}

impl SinkAction {
    pub async fn execute(
        self,
        db: &DatabaseConnection,
        sender: &Sender<String>,
    ) -> Result<(), DbErr> {
        match self {
            SinkAction::SpaceCreated { entity_id, space } => {
                let space = space.to_lowercase();
                spaces::create_schema(db, &space, sender).await?;
                spaces::create(db, entity_id, space, sender).await?
            }

            SinkAction::TypeCreated { entity_id, space } => {
                let space = space.to_lowercase();
                // make the entity a type in the global schema
                entities::upsert_is_type(db, entity_id.clone(), true, sender).await?;
                // create a table within the space schema for this type
                entities::create_table(db, &entity_id, &space, sender).await?
            }

            SinkAction::TypeAdded {
                space,
                entity_id,
                type_id,
            } => {
                let space = space.to_lowercase();
                // add the entity_id to the type_id table
                entities::add_type(db, &entity_id, &type_id, sender).await?
            }

            SinkAction::AttributeAdded {
                space,
                entity_id,
                attribute_id,
                value,
            } => {
                let space = space.to_lowercase();
                let value_id = value.id().to_string();
                // add the relation to the entity in the space schema
                entities::add_relation(db, &entity_id, &value_id, &space, sender).await?;
                // add the attribute to the entity in the global schema
                entities::add_attribute(db, entity_id, value_id, sender).await?
            }

            SinkAction::NameAdded {
                space,
                entity_id,
                name,
            } => {
                let space = space.to_lowercase();
                //TODO Update the table name to via a smart comment
                entities::upsert_name(db, entity_id, name, sender).await?;
            }

            SinkAction::ValueTypeAdded {
                space,
                entity_id,
                attribute_id,
                value_type,
            } => {
                // TODO Update the column type? Not 100% sure what I am supposed to do here.
            }

            SinkAction::SubspaceAdded {
                parent_space,
                child_space,
            } => {
                // TODO For the parent space schema endpoint, we need to merge the child space schema into the parent space schema
            }

            SinkAction::SubspaceRemoved {
                parent_space,
                child_space,
            } => {
                // TODO For the parent space schema endpoint, we need to remove the child space schema from the parent space schema
            }

            SinkAction::TripleAdded {
                space,
                entity_id,
                attribute_id,
                value,
                author,
            } => triples::create(db, entity_id, attribute_id, value, space, author, sender).await?,
            SinkAction::TripleDeleted {
                space,
                entity_id,
                attribute_id,
                value,
                author,
            } => {} //triples::delete(db, entity_id, attribute_id, value, space, author, sender).await,

            SinkAction::EntityCreated {
                space, entity_id, ..
            } => {
                let space = space.to_lowercase();
                entities::create(db, entity_id, space, sender).await?
            }
        };
        Ok(())
    }
}
