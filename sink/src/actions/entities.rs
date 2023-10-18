use anyhow::Error;
use sea_orm::{ConnectionTrait, DatabaseTransaction};

use crate::{
    models::entities,
    sink_actions::{ActionDependencies, SinkAction, SinkActionDependency as Dep},
};

use super::general::GeneralAction;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub enum EntityAction<'a> {
    /// Avatars can be added to users
    AvatarAdded {
        space: &'a str,
        entity_id: &'a str,
        avatar_image: &'a str,
    },

    /// We care about a name being added to an entity because we need this when adding attributes to a type in the graph.
    NameAdded {
        space: &'a str,
        entity_id: &'a str,
        name: &'a str,
    },

    /// We care about a description being added to an entity
    DescriptionAdded {
        space: &'a str,
        entity_id: &'a str,
        description: &'a str,
    },
}

impl EntityAction<'_> {
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

impl ActionDependencies for EntityAction<'_> {
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
