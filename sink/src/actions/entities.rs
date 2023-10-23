use anyhow::Error;
use sea_orm::{ConnectionTrait, DatabaseTransaction};

use crate::{
    models::entities,
    sink_actions::{ActionDependencies, SinkAction, SinkActionDependency as Dep},
};

use super::general::GeneralAction;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EntityAction {
    /// Avatars can be added to users
    AvatarAdded {
        space: String,
        entity_id: String,
        avatar_image: String,
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
}

impl EntityAction {
    pub async fn execute(
        &self,
        db: &DatabaseTransaction,
        space_queries: bool,
    ) -> Result<(), Error> {
        match self {
            EntityAction::NameAdded {
                space,
                entity_id,
                name,
            } => {
                entities::upsert_name(db, entity_id, name, space, space_queries).await?;
            }
            EntityAction::DescriptionAdded {
                space,
                entity_id,
                description,
            } => entities::upsert_description(db, entity_id, description, space).await?,
            EntityAction::AvatarAdded {
                space,
                entity_id,
                avatar_image,
            } => todo!(),
        };

        Ok(())
    }
}

impl ActionDependencies for EntityAction {
    fn dependencies(&self) -> Option<Vec<Dep>> {
        match self {
            EntityAction::AvatarAdded {
                space,
                entity_id,
                avatar_image,
            } => Some(vec![Dep::Exists {
                entity_id: entity_id.to_string(),
            }]),
            EntityAction::NameAdded {
                space,
                entity_id,
                name,
            } => Some(vec![Dep::Exists {
                entity_id: entity_id.to_string(),
            }]),
            EntityAction::DescriptionAdded {
                space,
                entity_id,
                description,
            } => Some(vec![Dep::Exists {
                entity_id: entity_id.to_string(),
            }]),
        }
    }

    fn has_fallback(&self) -> bool {
        true
    }
}
