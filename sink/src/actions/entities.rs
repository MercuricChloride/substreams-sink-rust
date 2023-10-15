use anyhow::Error;
use sea_orm::{ConnectionTrait, DatabaseTransaction};

use crate::{
    models::entities,
    sink_actions::{ActionDependencies, SinkAction, SinkActionDependencies as Dep},
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

impl<'a> ActionDependencies<'a> for EntityAction<'a> {
    fn dependencies(&self) -> Option<Vec<SinkAction<'a>>> {
        match self {
            EntityAction::AvatarAdded {
                space,
                entity_id,
                avatar_image,
            } => Some(vec![SinkAction::General(GeneralAction::EntityCreated {
                space: "",
                author: "",
                entity_id,
            })]),
            EntityAction::NameAdded {
                space,
                entity_id,
                name,
            } => Some(vec![SinkAction::General(GeneralAction::EntityCreated {
                space: "",
                author: "",
                entity_id,
            })]),
            EntityAction::DescriptionAdded {
                space,
                entity_id,
                description,
            } => Some(vec![SinkAction::General(GeneralAction::EntityCreated {
                space: "",
                author: "",
                entity_id,
            })]),
        }
    }

    fn has_fallback(&self) -> bool {
        match self {
            EntityAction::AvatarAdded {
                space,
                entity_id,
                avatar_image,
            } => false,
            EntityAction::NameAdded {
                space,
                entity_id,
                name,
            } => false,
            EntityAction::DescriptionAdded {
                space,
                entity_id,
                description,
            } => false,
        }
    }

    fn fallback(&self) -> Option<Vec<crate::sink_actions::SinkAction<'a>>> {
        None
    }

    fn as_dep(&self) -> SinkAction<'a> {
        match self {
            EntityAction::AvatarAdded {
                space,
                entity_id,
                avatar_image,
            } => SinkAction::Entity(EntityAction::AvatarAdded {
                space: "",
                entity_id,
                avatar_image,
            }),
            EntityAction::NameAdded {
                space,
                entity_id,
                name,
            } => SinkAction::Entity(EntityAction::NameAdded {
                space: "",
                entity_id,
                name,
            }),
            EntityAction::DescriptionAdded {
                space,
                entity_id,
                description,
            } => SinkAction::Entity(EntityAction::DescriptionAdded {
                space: "",
                entity_id,
                description,
            }),
        }
    }
}
