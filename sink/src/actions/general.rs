use anyhow::Error;
use sea_orm::{ConnectionTrait, DatabaseConnection, DatabaseTransaction};

use crate::{
    constants::Entities,
    models::{entities, triples},
    sink_actions::ActionDependencies,
    sink_actions::{SinkAction, SinkActionDependencies as Dep},
    triples::ValueType,
};

use super::tables::TableAction;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub enum GeneralAction<'a> {
    /// If we don't have any specific task to take, we will just add the triple to the graph
    TripleAdded {
        space: &'a str,
        entity_id: &'a str,
        attribute_id: &'a str,
        value: &'a ValueType,
        author: &'a str,
    },

    /// If it's an entity creation action, we need to add the entity to the graph
    EntityCreated {
        space: &'a str,
        entity_id: &'a str,
        author: &'a str,
    },

    /// If it's a triple deletion action, we need to remove the entity from the graph
    TripleDeleted {
        space: &'a str,
        entity_id: &'a str,
        attribute_id: &'a str,
        value: &'a ValueType,
        author: &'a str,
    },
}

impl GeneralAction<'_> {
    pub async fn execute(self, db: &DatabaseTransaction) -> Result<(), Error> {
        match self {
            GeneralAction::TripleAdded {
                space,
                entity_id,
                attribute_id,
                value,
                author,
            } => triples::create(db, entity_id, attribute_id, value, space, author).await?,
            GeneralAction::EntityCreated {
                space,
                entity_id,
                author,
            } => entities::create(db, entity_id, space).await?,
            GeneralAction::TripleDeleted {
                space,
                entity_id,
                attribute_id,
                value,
                author,
            } => triples::delete(db, entity_id, attribute_id, value, space, author).await?,
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

impl<'a> ActionDependencies<'a> for GeneralAction<'a> {
    fn dependencies(&self) -> Option<Vec<SinkAction<'a>>> {
        match self {
            GeneralAction::TripleAdded {
                space,
                entity_id,
                attribute_id,
                value,
                author,
            } => Some(vec![
                SinkAction::General(GeneralAction::EntityCreated {
                    space: "".into(),
                    author: "".into(),
                    entity_id,
                }),
                SinkAction::General(GeneralAction::EntityCreated {
                    space: "".into(),
                    author: "".into(),
                    entity_id: attribute_id,
                }),
                SinkAction::Table(TableAction::TypeAdded {
                    space: "".into(),
                    entity_id: attribute_id,
                    type_id: Entities::Attribute.id().into(),
                }),
            ]),
            GeneralAction::TripleDeleted {
                space,
                entity_id,
                attribute_id,
                value,
                author,
            } => None,
// Some(vec![
//                 SinkAction::General(GeneralAction::EntityCreated {
//                     space: "".into(),
//                     author: "".into(),
//                     entity_id,
//                 }),
//                 SinkAction::General(GeneralAction::EntityCreated {
//                     space: "".into(),
//                     entity_id: attribute_id,
//                     author: "".into(),
//                 }),
//                 SinkAction::Table(TableAction::TypeAdded {
//                     space: "".into(),
//                     entity_id: attribute_id,
//                     type_id: Entities::Attribute.id().into(),
//                 }),
//             ]),
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
            } => false,
            GeneralAction::TripleDeleted {
                space,
                entity_id,
                attribute_id,
                value,
                author,
            } => false,
        }
    }

    fn fallback(&self) -> Option<Vec<crate::sink_actions::SinkAction<'a>>> {
        match self {
            GeneralAction::TripleAdded {
                space,
                entity_id,
                attribute_id,
                value,
                author,
            } => Some(vec![
                SinkAction::General(GeneralAction::EntityCreated {
                    space: space.clone(),
                    entity_id: entity_id.clone(),
                    author: author.clone(),
                }),
                SinkAction::General(GeneralAction::EntityCreated {
                    space: space.clone(),
                    entity_id: attribute_id.clone(),
                    author: author.clone(),
                }),
            ]),
            _ => None,
        }
    }

    fn as_dep(&self) -> SinkAction<'a> {
        match self {
            GeneralAction::TripleAdded {
                space,
                entity_id,
                attribute_id,
                value,
                author,
            } => SinkAction::General(GeneralAction::TripleAdded {
                space: "".into(),
                entity_id,
                attribute_id,
                value,
                author: "".into(),
            }),
            GeneralAction::EntityCreated {
                space,
                entity_id,
                author,
            } => SinkAction::General(GeneralAction::EntityCreated {
                space: "".into(),
                entity_id,
                author: "".into(),
            }),
            GeneralAction::TripleDeleted {
                space,
                entity_id,
                attribute_id,
                value,
                author,
            } => SinkAction::General(GeneralAction::TripleDeleted {
                space: "".into(),
                entity_id,
                attribute_id,
                value,
                author: "".into(),
            }),
        }
    }
}
