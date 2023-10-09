use anyhow::Error;
use sea_orm::ConnectionTrait;

use crate::models::spaces::upsert_cover;

#[derive(Debug, Clone)]
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
