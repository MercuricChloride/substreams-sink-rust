use anyhow::Error;
use sea_orm::{ConnectionTrait, DatabaseTransaction};

use crate::{
    models::spaces::upsert_cover,
    sink_actions::{ActionDependencies, SinkAction, SinkActionDependency as Dep},
};

use super::tables::TableAction;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub enum SpaceAction<'a> {
    /// Covers can be added to spaces, this is the cover image for the webpage
    CoverAdded {
        space: &'a str,
        entity_id: &'a str,
        cover_image: &'a str,
    },
    /// Spaces can have subspaces, and we need to know when a subspace is added to a space so we can deploy a new subgraph for that space.
    SubspaceAdded {
        parent_space: &'a str,
        child_space: &'a str,
    },

    /// Spaces can also remove subspaces, and we need to know when a subspace is removed from a space
    SubspaceRemoved {
        parent_space: &'a str,
        child_space: &'a str,
    },
}

impl SpaceAction<'_> {
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
            } => todo!("SubspaceAdded"),
            SpaceAction::SubspaceRemoved {
                parent_space,
                child_space,
            } => todo!("SubspaceRemoved"),
        };

        Ok(())
    }
}

impl<'a> ActionDependencies<'a> for SpaceAction<'a> {
    fn dependencies(&self) -> Option<Vec<Dep<'a>>> {
        match self {
            SpaceAction::CoverAdded {
                space,
                entity_id,
                cover_image,
            } => Some(vec![Dep::Exists { entity_id }, Dep::IsSpace { entity_id }]),
            SpaceAction::SubspaceAdded {
                parent_space,
                child_space,
            } => Some(vec![
                Dep::Exists {
                    entity_id: parent_space,
                },
                Dep::Exists {
                    entity_id: child_space,
                },
                Dep::IsSpace {
                    entity_id: parent_space,
                },
                Dep::IsSpace {
                    entity_id: child_space,
                },
            ]),
            SpaceAction::SubspaceRemoved {
                parent_space,
                child_space,
            } => Some(vec![
                Dep::Exists {
                    entity_id: parent_space,
                },
                Dep::Exists {
                    entity_id: child_space,
                },
                Dep::IsSpace {
                    entity_id: parent_space,
                },
                Dep::IsSpace {
                    entity_id: child_space,
                },
            ]),
        }
    }

    fn has_fallback(&self) -> bool {
        false
    }
}
