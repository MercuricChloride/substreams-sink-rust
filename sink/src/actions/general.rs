use anyhow::Error;
use sea_orm::{ConnectionTrait, DatabaseConnection, DatabaseTransaction};

use crate::{
    constants::Entities,
    models::{entities, triples},
    sink_actions::ActionDependencies,
    sink_actions::{SinkAction, SinkActionDependency as Dep},
    triples::ValueType,
};

use super::tables::TableAction;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
    pub async fn execute(
        self,
        db: &DatabaseTransaction,
        use_space_queries: bool,
    ) -> Result<(), Error> {
        match self {
            GeneralAction::TripleAdded {
                space,
                entity_id,
                attribute_id,
                value,
                author,
            } => {
                if use_space_queries {
                    // insert triple data
                }
                triples::create(db, &entity_id, &attribute_id, value, &space, &author).await?
            }
            GeneralAction::EntityCreated {
                space,
                entity_id,
                author,
            } => entities::create(db, &entity_id, &space).await?,
            GeneralAction::TripleDeleted {
                space,
                entity_id,
                attribute_id,
                value,
                author,
            } => triples::delete(db, &entity_id, &attribute_id, value, &space, &author).await?,
        };
        Ok(())
    }

    pub async fn check_if_exists(&self, db: &DatabaseTransaction) -> Result<bool, Error> {
        match self {
            GeneralAction::EntityCreated {
                space,
                entity_id,
                author,
            } => Ok(entities::exists(db, entity_id).await?),
            _ => todo!("check_if_exists for general action but not entity created"),
        }
    }
}

impl ActionDependencies for GeneralAction {
    fn dependencies(&self) -> Option<Vec<Dep>> {
        match self {
            GeneralAction::TripleAdded {
                space,
                entity_id,
                attribute_id,
                value,
                author,
            } => Some(vec![
                Dep::Exists {
                    entity_id: entity_id.to_string(),
                },
                Dep::Exists {
                    entity_id: attribute_id.to_string(),
                },
                Dep::IsAttribute {
                    entity_id: attribute_id.to_string(),
                },
            ]),
            GeneralAction::TripleDeleted {
                space,
                entity_id,
                attribute_id,
                value,
                author,
            } => None,
            GeneralAction::EntityCreated {
                space,
                entity_id,
                author,
            } => None,
        }
    }

    fn has_fallback(&self) -> bool {
        match self {
            GeneralAction::TripleAdded {
                space,
                entity_id,
                attribute_id,
                value,
                author,
            } => true,
            GeneralAction::EntityCreated {
                space,
                entity_id,
                author,
            } => true,
            GeneralAction::TripleDeleted {
                space,
                entity_id,
                attribute_id,
                value,
                author,
            } => false,
        }
    }
}
