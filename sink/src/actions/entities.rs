use anyhow::Error;
use sea_orm::ConnectionTrait;

use crate::{
    models::entities,
    sink_actions::{ActionDependencies, SinkAction, SinkActionDependencies as Dep},
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

impl ActionDependencies for EntityAction {
    fn dependencies(&self) -> Option<Vec<SinkAction>> {
        match self {
            EntityAction::AvatarAdded {
                space,
                entity_id,
                avatar_image,
            } => Some(vec![SinkAction::General(GeneralAction::EntityCreated {
                space: "".into(),
                entity_id: entity_id.into(),
                author: "".into(),
            })]),
            EntityAction::NameAdded {
                space,
                entity_id,
                name,
            } => Some(vec![SinkAction::General(GeneralAction::EntityCreated {
                space: "".into(),
                entity_id: entity_id.into(),
                author: "".into(),
            })]),
            EntityAction::DescriptionAdded {
                space,
                entity_id,
                description,
            } => Some(vec![SinkAction::General(GeneralAction::EntityCreated {
                space: "".into(),
                entity_id: entity_id.into(),
                author: "".into(),
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

    fn fallback(&self) -> Option<Vec<crate::sink_actions::SinkAction>> {
        None
    }

    fn as_dep(&self) -> SinkAction {
        match self {
            EntityAction::AvatarAdded {
                space,
                entity_id,
                avatar_image,
            } => SinkAction::Entity(EntityAction::AvatarAdded {
                space: "".into(),
                entity_id: entity_id.clone(),
                avatar_image: avatar_image.clone(),
            }),
            EntityAction::NameAdded {
                space,
                entity_id,
                name,
            } => SinkAction::Entity(EntityAction::NameAdded {
                space: "".into(),
                entity_id: entity_id.clone(),
                name: name.clone(),
            }),
            EntityAction::DescriptionAdded {
                space,
                entity_id,
                description,
            } => SinkAction::Entity(EntityAction::DescriptionAdded {
                space: "".into(),
                entity_id: entity_id.clone(),
                description: description.clone(),
            }),
        }
    }
}
