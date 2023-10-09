use anyhow::Error;
use sea_orm::ConnectionTrait;

use crate::{
    constants::{self, Entities},
    models::{entities, spaces},
    triples::ValueType,
};

#[derive(Debug, Clone)]
pub enum TableAction {
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

    /// We care about a ValueType being added to an entity because we need this when adding attributes to a type in the graph.
    ValueTypeAdded {
        space: String,
        entity_id: String,
        attribute_id: String,
        /// The entity id of that particular value type
        value_type: String,
    },
}

impl TableAction {
    pub async fn execute(
        &self,
        db: &impl ConnectionTrait,
        space_queries: bool,
    ) -> Result<(), Error> {
        match self {
            TableAction::SpaceCreated {
                entity_id,
                space,
                created_in_space,
            } => {
                let space = space.to_lowercase();
                if space_queries {
                    spaces::create_schema(db, &space).await?;
                }
                spaces::create(
                    db,
                    entity_id.to_string(),
                    space,
                    created_in_space.to_string(),
                )
                .await?
            }

            TableAction::TypeAdded {
                space,
                entity_id,
                type_id,
            } => {
                let space = space.to_lowercase();
                // if the type_id is the "SchemaType" Entity, this means we are designating the entity as a type.
                // Because we are giving it a "type" of "Type"

                // if we are giving an entity a type of "Type", we also need to create a table for it
                if type_id == constants::Entities::SchemaType.id() {
                    entities::upsert_is_type(db, entity_id.clone(), true, &space.clone()).await?;

                    if space_queries {
                        entities::create_table(db, &entity_id).await?;
                    }
                }

                if space_queries {
                    entities::create_table(db, &type_id).await?;
                }

                // add the entity_id to the type_id table
                entities::add_type(db, &entity_id, &type_id, &space, space_queries).await?
            }

            TableAction::AttributeAdded {
                space,
                entity_id,
                attribute_id,
                value,
            } => {
                let space = space.to_lowercase();
                let value_id = value.id().to_string();
                if space_queries {
                    // we need to check the value type of the value
                    // if it doesn't have a value type, we need to assume default, which is text? TODO Verify this?
                    // if its text, this means we just add a column to the table that points to a triple?

                    // add the relation to the entity in the space schema
                    entities::add_relation(db, &entity_id, &attribute_id, &value, &space).await?;
                }
                // add the attribute to the entity in the global schema
                entities::add_attribute(db, entity_id.to_string(), value_id).await?
            }

            TableAction::ValueTypeAdded {
                space,
                entity_id,
                attribute_id,
                value_type,
            } => {
                let value_type = match value_type {
                    s if s.starts_with(Entities::Relation.id()) => {
                        Entities::Relation
                    }
                    _ => Entities::Text
                };

                // we just need to update the value type on the entity
                entities::upsert_value_type(
                    db,
                    entity_id.to_string(),
                    value_type.id().to_string(),
                    space.to_string(),
                ).await?;
                // TODO MAYBE WE NEED TO CHANGE ALL COLUMNS THAT MATCH THIS ID???
            }
        };

        Ok(())
    }
}
