use anyhow::Error;
use sea_orm::ConnectionTrait;

use crate::{
    models::spaces::upsert_cover,
    sink_actions::{ActionDependencies, SinkAction},
};

use super::tables::TableAction;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SpaceAction {
    /// Covers can be added to spaces, this is the cover image for the webpage
    CoverAdded {
        space: String,
        entity_id: String,
        cover_image: String,
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
}

impl SpaceAction {
    pub async fn execute(&self, db: &impl ConnectionTrait) -> Result<(), Error> {
        match self {
            SpaceAction::CoverAdded {
                space,
                entity_id,
                cover_image,
            } => upsert_cover(db, space.to_string(), cover_image.to_string()).await?,
            SpaceAction::SubspaceAdded {
                parent_space,
                child_space,
            } => todo!("SubspaceAdded"),
            SpaceAction::SubspaceRemoved {
                parent_space,
                child_space,
            } => todo!("SubspaceRemoved"),
        };

        Ok(())
    }
}

impl ActionDependencies for SpaceAction {
    fn dependencies(&self) -> Option<Vec<SinkAction>> {
        match self {
            SpaceAction::CoverAdded {
                space,
                entity_id,
                cover_image,
            } => Some(vec![SinkAction::Table(TableAction::SpaceCreated {
                entity_id: entity_id.into(),
                space: "".to_string(),
                created_in_space: "".to_string(),
                author: "".into(),
            })]),
            SpaceAction::SubspaceAdded {
                parent_space,
                child_space,
            } => Some(vec![
                SinkAction::Table(TableAction::SpaceCreated {
                    entity_id: parent_space.into(),
                    space: "".to_string(),
                    created_in_space: "".to_string(),
                    author: "".into(),
                }),
                SinkAction::Table(TableAction::SpaceCreated {
                    entity_id: child_space.into(),
                    space: "".to_string(),
                    created_in_space: "".to_string(),
                    author: "".into(),
                }),
            ]),
            SpaceAction::SubspaceRemoved {
                parent_space,
                child_space,
            } => Some(vec![
                SinkAction::Table(TableAction::SpaceCreated {
                    entity_id: parent_space.into(),
                    space: "".to_string(),
                    created_in_space: "".to_string(),
                    author: "".into(),
                }),
                SinkAction::Table(TableAction::SpaceCreated {
                    entity_id: child_space.into(),
                    space: "".to_string(),
                    created_in_space: "".to_string(),
                    author: "".into(),
                }),
            ]),
        }
    }

    fn has_fallback(&self) -> bool {
        match self {
            SpaceAction::CoverAdded {
                space,
                entity_id,
                cover_image,
            } => false,
            SpaceAction::SubspaceAdded {
                parent_space,
                child_space,
            } => false,
            SpaceAction::SubspaceRemoved {
                parent_space,
                child_space,
            } => false,
        }
    }

    fn fallback(&self) -> Option<Vec<crate::sink_actions::SinkAction>> {
        None
    }

    fn as_dep(&self) -> SinkAction {
        match self {
            SpaceAction::CoverAdded {
                space,
                entity_id,
                cover_image,
            } => SinkAction::Space(SpaceAction::CoverAdded {
                space: space.clone(),
                entity_id: entity_id.clone(),
                cover_image: cover_image.clone(),
            }),
            SpaceAction::SubspaceAdded {
                parent_space,
                child_space,
            } => SinkAction::Space(SpaceAction::SubspaceAdded {
                parent_space: parent_space.clone(),
                child_space: child_space.clone(),
            }),
            SpaceAction::SubspaceRemoved {
                parent_space,
                child_space,
            } => SinkAction::Space(SpaceAction::SubspaceRemoved {
                parent_space: parent_space.clone(),
                child_space: child_space.clone(),
            }),
        }
    }
}
