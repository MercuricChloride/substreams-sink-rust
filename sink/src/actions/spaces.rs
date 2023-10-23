use anyhow::Error;
use sea_orm::{ConnectionTrait, DatabaseTransaction};

use crate::{
    models::spaces::{self, upsert_cover},
    sink_actions::{ActionDependencies, SinkAction, SinkActionDependency as Dep},
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
    pub async fn execute(&self, db: &DatabaseTransaction) -> Result<(), Error> {
        match self {
            SpaceAction::CoverAdded {
                space,
                entity_id,
                cover_image,
            } => upsert_cover(db, space, cover_image).await?,
            SpaceAction::SubspaceAdded {
                parent_space,
                child_space,
            } => spaces::add_subspace(db, parent_space, child_space).await?,
            SpaceAction::SubspaceRemoved {
                parent_space,
                child_space,
            } => spaces::remove_subspace(db, parent_space, child_space).await?,
        };

        Ok(())
    }
}

impl ActionDependencies for SpaceAction {
    fn dependencies(&self) -> Option<Vec<Dep>> {
        match self {
            SpaceAction::CoverAdded {
                space,
                entity_id,
                cover_image,
            } => Some(vec![
                Dep::Exists {
                    entity_id: entity_id.to_string(),
                },
                Dep::IsSpace {
                    entity_id: entity_id.to_string(),
                },
            ]),
            SpaceAction::SubspaceAdded {
                parent_space,
                child_space,
            } => Some(vec![
                Dep::Exists {
                    entity_id: parent_space.to_string(),
                },
                Dep::Exists {
                    entity_id: child_space.to_string(),
                },
                Dep::IsSpace {
                    entity_id: parent_space.to_string(),
                },
                Dep::IsSpace {
                    entity_id: child_space.to_string(),
                },
            ]),
            SpaceAction::SubspaceRemoved {
                parent_space,
                child_space,
            } => Some(vec![
                Dep::Exists {
                    entity_id: parent_space.to_string(),
                },
                Dep::Exists {
                    entity_id: child_space.to_string(),
                },
                Dep::IsSpace {
                    entity_id: parent_space.to_string(),
                },
                Dep::IsSpace {
                    entity_id: child_space.to_string(),
                },
            ]),
        }
    }

    fn has_fallback(&self) -> bool {
        false
    }
}
