use anyhow::Error;
use sea_orm::ConnectionTrait;

use crate::models::entities;

#[derive(Debug, Clone)]
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
        db: &impl ConnectionTrait,
        space_queries: bool,
    ) -> Result<(), Error> {
        match self {
            EntityAction::NameAdded {
                space,
                entity_id,
                name,
            } => {
                let space = space.to_lowercase();
                entities::upsert_name(db, entity_id.to_string(), name.into(), space, space_queries)
                    .await?;
            }
            EntityAction::DescriptionAdded {
                space,
                entity_id,
                description,
            } => {
                let space = space.to_lowercase();
                entities::upsert_description(db, entity_id.into(), description.into(), space.into())
                    .await?
            }
            EntityAction::AvatarAdded {
                space,
                entity_id,
                avatar_image,
            } => todo!(),
        };

        Ok(())
    }
}
