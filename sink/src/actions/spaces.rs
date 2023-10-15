use anyhow::Error;
use sea_orm::{ConnectionTrait, DatabaseTransaction};

use crate::{
    models::spaces::upsert_cover,
    sink_actions::{ActionDependencies, SinkAction},
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
    fn dependencies(&self) -> Option<Vec<SinkAction<'a>>> {
        match self {
            SpaceAction::CoverAdded {
                space,
                entity_id,
                cover_image,
            } => Some(vec![SinkAction::Table(TableAction::SpaceCreated {
                entity_id,
                space: "",
                created_in_space: "",
                author: "".into(),
            })]),
            SpaceAction::SubspaceAdded {
                parent_space,
                child_space,
            } => Some(vec![
                SinkAction::Table(TableAction::SpaceCreated {
                    entity_id: parent_space,
                    space: "",
                    created_in_space: "",
                    author: "",
                }),
                SinkAction::Table(TableAction::SpaceCreated {
                    entity_id: child_space,
                    space: "",
                    created_in_space: "",
                    author: "",
                }),
            ]),
            SpaceAction::SubspaceRemoved {
                parent_space,
                child_space,
            } => Some(vec![
                SinkAction::Table(TableAction::SpaceCreated {
                    entity_id: parent_space,
                    space: "",
                    created_in_space: "",
                    author: "",
                }),
                SinkAction::Table(TableAction::SpaceCreated {
                    entity_id: child_space,
                    space: "",
                    created_in_space: "",
                    author: "",
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

    fn fallback(&self) -> Option<Vec<crate::sink_actions::SinkAction<'a>>> {
        None
    }

    fn as_dep(&self) -> SinkAction<'a> {
        match self {
            SpaceAction::CoverAdded {
                space,
                entity_id,
                cover_image,
            } => SinkAction::Space(SpaceAction::CoverAdded {
                space,
                entity_id,
                cover_image,
            }),
            SpaceAction::SubspaceAdded {
                parent_space,
                child_space,
            } => SinkAction::Space(SpaceAction::SubspaceAdded {
                parent_space,
                child_space,
            }),
            SpaceAction::SubspaceRemoved {
                parent_space,
                child_space,
            } => SinkAction::Space(SpaceAction::SubspaceRemoved {
                parent_space,
                child_space,
            }),
        }
    }
}
