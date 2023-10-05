use anyhow::Error;
use futures03::future::{join_all, try_join_all};
use futures03::stream::{FuturesOrdered, FuturesUnordered};
use migration::DbErr;
use sea_orm::{DatabaseConnection, DatabaseTransaction};
use strum::EnumIter;
use tokio::sync::mpsc::{Sender, UnboundedSender};
use tokio_stream::StreamExt;

use crate::constants::{Entities, self};
use crate::triples::{ActionTriple, ValueType};

use crate::models::*;

#[derive(Debug, EnumIter)]
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
    SpaceCreated {
        entity_id: String,
        space: String,
        created_in_space: String,
    },

    /// This action denotes a type being added to an entity in the DB
    /// Something like this:
    ///
    /// `(EntityID, "types", TypeEntity)`
    ///
    /// Note that a TypeAdded action may also create a new type if the type_id is the "Type" entity in the root space.
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

    /// We care about a description being added to an entity
    DescriptionAdded {
        space: String,
        entity_id: String,
        description: String,
    },

    /// Covers can be added to spaces, this is the cover image for the webpage
    CoverAdded {
        space: String,
        entity_id: String,
        cover_image: String,
    },

    /// Avatars can be added to users
    AvatarAdded {
        space: String,
        entity_id: String,
        avatar_image: String,
    },

    /// We care about a ValueType being added to an entity because we need this when adding attributes to a type in the graph.
    ValueTypeAdded {
        space: String,
        entity_id: String,
        attribute_id: String,
        /// The entity id of that particular value type
        value_type: String,
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
    sink_actions: Vec<(SinkAction, Option<SinkAction>)>,
    db: &DatabaseConnection,
) -> Result<(), DbErr> {
    let mut futures = FuturesOrdered::new();
    let mut chunk = Vec::new();

    // we want to grab all of the default actions to execute first, than all of the optional actions after
    let mut actions = Vec::new();
    for (default_action, optional_action) in sink_actions {
        // if there is an optional action, we need to execute it
        if let Some(action) = optional_action {
            actions.push(action);
            //optional_actions.push(action);
        }
        actions.push(default_action);

        //default_actions.push(default_action);
    }

    // sort the actions by priority
    actions.sort_by(|a, b| a.action_priority().cmp(&b.action_priority()));

    for action in actions {
        chunk.push(action.execute(db));

        if chunk.len() == crate::MAX_CONNECTIONS {
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
    pub async fn execute(self, db: &DatabaseConnection) -> Result<(), DbErr> {
        match self {
            SinkAction::SpaceCreated {
                entity_id,
                space,
                created_in_space,
            } => {
                let space = space.to_lowercase();
                spaces::create_schema(db, &space).await?;
                spaces::create(db, entity_id, space, created_in_space).await?
            }

            SinkAction::TypeAdded {
                space,
                entity_id,
                type_id,
            } => {
                let space = space.to_lowercase();
                // if the type_id is the "Type" Entity, this means we are designating the entity as a type.
                // Because we are giving it a "type" of "Type"
                //if type_id == Entities::Type.id() {
                entities::upsert_is_type(db, type_id.clone(), true).await?;

                if &type_id == constants::Entities::SchemaType.id() {
                    entities::upsert_is_type(db, entity_id.clone(), true).await?;
                    entities::create_table(db, &entity_id, &space).await?;
                }

                entities::create_table(db, &type_id, &space).await?;
                //}
                // add the entity_id to the type_id table
                entities::add_type(db, &entity_id, &type_id, &space).await?
            }

            SinkAction::AttributeAdded {
                space,
                entity_id,
                value,
                ..
            } => {
                let space = space.to_lowercase();
                let value_id = value.id().to_string();
                // add the relation to the entity in the space schema
                entities::add_relation(db, &entity_id, &value_id, &space).await?;
                // add the attribute to the entity in the global schema
                entities::add_attribute(db, entity_id, value_id).await?
            }

            SinkAction::NameAdded {
                space,
                entity_id,
                name,
            } => {
                let space = space.to_lowercase();
                //TODO Update the table name to via a smart comment
                entities::upsert_name(db, entity_id, name, space).await?;
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
            } => triples::create(db, entity_id, attribute_id, value, space, author).await?,

            SinkAction::TripleDeleted {
                space,
                entity_id,
                attribute_id,
                value,
                author,
            } => triples::delete(db, entity_id, attribute_id, value, space, author).await?,

            SinkAction::EntityCreated {
                space, entity_id, ..
            } => {
                let space = space.to_lowercase();
                entities::create(db, entity_id, space).await?
            }

            SinkAction::DescriptionAdded {
                space,
                entity_id,
                description,
            } => {
                let space = space.to_lowercase();
                entities::upsert_description(db, entity_id, description, space).await?
            }

            SinkAction::CoverAdded {
                space,
                entity_id,
                cover_image,
            } => todo!(),

            SinkAction::AvatarAdded {
                space,
                entity_id,
                avatar_image,
            } => todo!(),
        };
        Ok(())
    }

    pub fn is_default_action(&self) -> bool {
        match self {
            SinkAction::TripleAdded { .. }
            | SinkAction::TripleDeleted { .. }
            | SinkAction::EntityCreated { .. } => true,
            _ => false,
        }
    }

    pub fn is_global_sink_action(&self) -> bool {
        match self {
            SinkAction::SpaceCreated { .. }
            | SinkAction::TypeAdded { .. }
            | SinkAction::AttributeAdded { .. }
            | SinkAction::NameAdded { .. }
            | SinkAction::DescriptionAdded { .. } => true,
            _ => false,
        }
    }

    pub fn action_priority(&self) -> i32 {
        match self {
            // space created is the first action to take in the Db
            SinkAction::AttributeAdded { .. }
            | SinkAction::SpaceCreated { .. } => 5,
            SinkAction::TypeAdded {..} => 4,
            // idk about these yet
            | SinkAction::DescriptionAdded { .. }
            | SinkAction::NameAdded { .. }
            | SinkAction::CoverAdded { .. }
            | SinkAction::AvatarAdded { .. }
            | SinkAction::ValueTypeAdded { .. }
            | SinkAction::SubspaceAdded { .. }
            | SinkAction::SubspaceRemoved { .. } => 3,
            // if it is a default action just leave it at 2
            _ => 2,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::SinkAction;
    use crate::constants::{Attributes, Entities};
    use crate::triples::{ActionTriple, ValueType};
    use strum::IntoEnumIterator;

    const ENTITY_ID: &'static str = "sample-entity-id";

    fn dummy_triple_for_action(sink_action: &SinkAction) -> ActionTriple {
        match sink_action {
            SinkAction::SpaceCreated { .. } => ActionTriple::CreateTriple {
                entity_id: ENTITY_ID.to_string(),
                attribute_id: Attributes::Space.id().to_string(),
                value: ValueType::String {
                    id: "string-id".to_string(),
                    value: "0xSpaceAddress".to_string(),
                },
                space: "space".to_string(),
                author: "author".to_string(),
            },

            SinkAction::TypeAdded { .. } => ActionTriple::CreateTriple {
                entity_id: ENTITY_ID.to_string(),
                attribute_id: Attributes::Type.id().to_string(),
                value: ValueType::Entity {
                    id: "some-type-id".to_string(),
                },
                space: "space".to_string(),
                author: "author".to_string(),
            },

            SinkAction::AttributeAdded { .. } => ActionTriple::CreateTriple {
                entity_id: ENTITY_ID.to_string(),
                attribute_id: Attributes::Attribute.id().to_string(),
                value: ValueType::Entity {
                    id: "Whatever".to_string(),
                },
                space: "space".to_string(),
                author: "author".to_string(),
            },

            SinkAction::NameAdded { .. } => ActionTriple::CreateTriple {
                entity_id: ENTITY_ID.to_string(),
                attribute_id: Attributes::Name.id().to_string(),
                value: ValueType::String {
                    id: "String Id".to_string(),
                    value: "Some Name".to_string(),
                },
                space: "space".to_string(),
                author: "author".to_string(),
            },

            SinkAction::DescriptionAdded { .. } => ActionTriple::CreateTriple {
                entity_id: ENTITY_ID.to_string(),
                attribute_id: Attributes::Description.id().to_string(),
                value: ValueType::String {
                    id: "String Id".to_string(),
                    value: "Some Description".to_string(),
                },
                space: "space".to_string(),
                author: "author".to_string(),
            },

            SinkAction::CoverAdded { .. } => ActionTriple::CreateTriple {
                entity_id: ENTITY_ID.to_string(),
                attribute_id: Attributes::Cover.id().to_string(),
                value: ValueType::String {
                    id: "String Id".to_string(),
                    value: "Some Cover Link".to_string(),
                },
                space: "space".to_string(),
                author: "author".to_string(),
            },

            SinkAction::AvatarAdded { .. } => ActionTriple::CreateTriple {
                entity_id: ENTITY_ID.to_string(),
                attribute_id: Attributes::Avatar.id().to_string(),
                value: ValueType::String {
                    id: "String Id".to_string(),
                    value: "Some Avatar Link".to_string(),
                },
                space: "space".to_string(),
                author: "author".to_string(),
            },

            SinkAction::ValueTypeAdded { .. } => ActionTriple::CreateTriple {
                entity_id: ENTITY_ID.to_string(),
                attribute_id: Attributes::ValueType.id().to_string(),
                value: ValueType::Entity {
                    id: "Some Value Type Id".to_string(),
                },
                space: "space".to_string(),
                author: "author".to_string(),
            },

            SinkAction::SubspaceAdded { .. } => ActionTriple::CreateTriple {
                entity_id: ENTITY_ID.to_string(),
                attribute_id: Attributes::Subspace.id().to_string(),
                value: ValueType::Entity {
                    id: "Some other space".to_string(),
                },
                space: "space".to_string(),
                author: "author".to_string(),
            },

            SinkAction::SubspaceRemoved { .. } => ActionTriple::DeleteTriple {
                entity_id: ENTITY_ID.to_string(),
                attribute_id: Attributes::Subspace.id().to_string(),
                value: ValueType::Entity {
                    id: "Some other space".to_string(),
                },
                space: "space".to_string(),
                author: "author".to_string(),
            },

            SinkAction::TripleAdded { .. } => ActionTriple::CreateTriple {
                entity_id: ENTITY_ID.to_string(),
                attribute_id: Attributes::Subspace.id().to_string(),
                value: ValueType::Entity {
                    id: "Some other space".to_string(),
                },
                space: "space".to_string(),
                author: "author".to_string(),
            },

            SinkAction::EntityCreated { .. } => ActionTriple::CreateEntity {
                entity_id: "entity-id".to_string(),
                space: "space".to_string(),
                author: "author".to_string(),
            },

            SinkAction::TripleDeleted { .. } => ActionTriple::DeleteTriple {
                entity_id: ENTITY_ID.to_string(),
                attribute_id: Attributes::Subspace.id().to_string(),
                value: ValueType::Entity {
                    id: "Some other space".to_string(),
                },
                space: "space".to_string(),
                author: "author".to_string(),
            },
        }
    }

    #[test]
    fn test_each_sink_action() {
        for action in SinkAction::iter() {
            let triple = dummy_triple_for_action(&action);

            if action.is_default_action() {
                // if it is a default action, we don't need to test it
                continue;
            } else {
                let sink_action: Option<SinkAction> = triple.try_into().ok();

                assert!(
                    sink_action.is_some(),
                    "couldnt match for sink action {:?}",
                    action
                );
            }
        }
    }
}
