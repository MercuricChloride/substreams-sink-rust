use anyhow::Error;
use sea_orm::ConnectionTrait;

use crate::{
    models::{entities, triples},
    triples::ValueType,
};

#[derive(Debug, Clone)]
pub enum GeneralAction {
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

impl GeneralAction {
    pub async fn execute(&self, db: &impl ConnectionTrait) -> Result<(), Error> {
        match self {
            GeneralAction::TripleAdded {
                space,
                entity_id,
                attribute_id,
                value,
                author,
            } => {
                triples::create(
                    db,
                    entity_id.into(),
                    attribute_id.into(),
                    value.clone(),
                    space.into(),
                    author.into(),
                )
                .await?
            }
            GeneralAction::EntityCreated {
                space,
                entity_id,
                author,
            } => {
                let space = space.to_lowercase();
                entities::create(db, entity_id.into(), space).await?
            }
            GeneralAction::TripleDeleted {
                space,
                entity_id,
                attribute_id,
                value,
                author,
            } => {
                triples::delete(
                    db,
                    entity_id.into(),
                    attribute_id.into(),
                    value.clone(),
                    space.into(),
                    author.into(),
                )
                .await?
            }
        };
        Ok(())
    }
}
