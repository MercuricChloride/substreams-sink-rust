use sea_orm_migration::{
    prelude::*,
    sea_orm::{DatabaseBackend, Statement},
};

macro_rules! drop_table {
    ($manager:ident, $table:ident) => {
        $manager
            .drop_table(Table::drop().table($table::Table).to_owned())
            .await?;
    };
}

macro_rules! drop_tables {
    ($manager:ident, $($table:ident),+) => {
        $(drop_table!($manager, $table);)+
    };
}

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // create the cursors table
        let connection: &SchemaManagerConnection = &manager.get_connection();

        // create the cursors table
        manager
            .create_table(
                Table::create()
                    .table(Cursors::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(Cursors::Id)
                            .integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(Cursors::Cursor).text().not_null())
                    .to_owned(),
            )
            .await?;

        // add a comment to the cursors table
        connection
            .execute(Statement::from_string(
                DatabaseBackend::Postgres,
                "COMMENT ON TABLE \"public\".\"cursors\" IS E'@name coolCursors';",
            ))
            .await?;

        // create the Accounts table
        manager
            .create_table(
                Table::create()
                    .table(Accounts::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(Accounts::Id)
                            .text()
                            .unique_key()
                            .primary_key(),
                    )
                    .to_owned(),
            )
            .await?;

        // create the LogEntries table
        manager
            .create_table(
                Table::create()
                    .table(LogEntries::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(LogEntries::Id)
                            .text()
                            .unique_key()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(LogEntries::CreatedAtBlock).text().not_null())
                    .col(ColumnDef::new(LogEntries::Uri).text().not_null())
                    .col(ColumnDef::new(LogEntries::CreatedBy).text().not_null())
                    .col(ColumnDef::new(LogEntries::Space).text().not_null())
                    .col(ColumnDef::new(LogEntries::MimeType).text())
                    .col(ColumnDef::new(LogEntries::Decoded).text())
                    .col(ColumnDef::new(LogEntries::Json).text())
                    .to_owned(),
            )
            .await?;

        // create the entity attributes table
        manager
            .create_table(
                Table::create()
                    .table(EntityAttributes::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(EntityAttributes::Id)
                            .text()
                            .unique_key()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(EntityAttributes::EntityId).text().not_null())
                    .col(
                        ColumnDef::new(EntityAttributes::AttributeOf)
                            .text()
                            .not_null(),
                    )
                    .to_owned(),
            )
            .await?;

        // create the entity types table
        manager
            .create_table(
                Table::create()
                    .table(EntityTypes::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(EntityTypes::Id)
                            .text()
                            .unique_key()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(EntityTypes::EntityId).text().not_null())
                    .col(ColumnDef::new(EntityTypes::Type).text().not_null())
                    .to_owned(),
            )
            .await?;

        // create the entities table
        manager
            .create_table(
                Table::create()
                    .table(Entities::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(Entities::Id)
                            .text()
                            .unique_key()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(Entities::Name).string())
                    .col(ColumnDef::new(Entities::Description).string())
                    .col(ColumnDef::new(Entities::IsType).boolean().default(false))
                    .col(ColumnDef::new(Entities::DefinedIn).text())
                    .col(ColumnDef::new(Entities::ValueType).text())
                    .col(ColumnDef::new(Entities::Version).text())
                    .col(ColumnDef::new(Entities::Versions).array(ColumnType::Text))
                    .to_owned(),
            )
            .await?;

        // create the spaces table
        manager
            .create_table(
                Table::create()
                    .table(Spaces::Table)
                    .if_not_exists()
                    .col(ColumnDef::new(Spaces::Id).text().unique_key().primary_key())
                    .col(ColumnDef::new(Spaces::Address).text())
                    .col(ColumnDef::new(Spaces::CreatedAtBlock).integer())
                    .col(ColumnDef::new(Spaces::IsRootSpace).boolean())
                    .col(ColumnDef::new(Spaces::Admins).text())
                    .col(ColumnDef::new(Spaces::EditorControllers).text())
                    .col(ColumnDef::new(Spaces::Editors).text())
                    .col(ColumnDef::new(Spaces::Entity).text())
                    .to_owned(),
            )
            .await?;

        // create the triples table
        manager
            .create_table(
                Table::create()
                    .table(Triples::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(Triples::Id)
                            .text()
                            .unique_key()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(Triples::EntityId).text().not_null())
                    .col(ColumnDef::new(Triples::AttributeId).text().not_null())
                    .col(ColumnDef::new(Triples::ValueId).text().not_null())
                    .col(ColumnDef::new(Triples::ValueType).text().not_null())
                    .col(ColumnDef::new(Triples::DefinedIn).text().not_null())
                    .col(ColumnDef::new(Triples::IsProtected).boolean().not_null())
                    .col(ColumnDef::new(Triples::NumberValue).text())
                    .col(ColumnDef::new(Triples::ArrayValue).text())
                    .col(ColumnDef::new(Triples::StringValue).text())
                    .col(ColumnDef::new(Triples::EntityValue).text())
                    .to_owned(),
            )
            .await?;

        // create the proposals table
        manager
            .create_table(
                Table::create()
                    .table(Proposals::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(Proposals::Id)
                            .text()
                            .unique_key()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(Proposals::Space).text().not_null())
                    .col(ColumnDef::new(Proposals::Name).text())
                    .col(ColumnDef::new(Proposals::Description).text())
                    .col(ColumnDef::new(Proposals::CreatedAt).integer().not_null())
                    .col(
                        ColumnDef::new(Proposals::CreatedAtBlock)
                            .integer()
                            .not_null(),
                    )
                    .col(ColumnDef::new(Proposals::CreatedBy).text())
                    .col(ColumnDef::new(Proposals::Status).text().not_null())
                    .col(ColumnDef::new(Proposals::ProposedVersions).array(ColumnType::Text))
                    .to_owned(),
            )
            .await?;

        // create the proposed version tables
        manager
            .create_table(
                Table::create()
                    .table(ProposedVersions::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(ProposedVersions::Id)
                            .text()
                            .unique_key()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(ProposedVersions::Name).text())
                    .col(ColumnDef::new(ProposedVersions::Description).text())
                    .col(
                        ColumnDef::new(ProposedVersions::CreatedAt)
                            .integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ProposedVersions::CreatedAtBlock)
                            .integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ProposedVersions::CreatedBy)
                            .text()
                            .not_null(),
                    )
                    .col(ColumnDef::new(ProposedVersions::Entity).text().not_null())
                    .col(ColumnDef::new(ProposedVersions::Actions).array(ColumnType::Text))
                    .to_owned(),
            )
            .await?;

        // create the actions table
        manager
            .create_table(
                Table::create()
                    .table(Actions::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(Actions::Id)
                            .text()
                            .unique_key()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(Actions::ActionType).text().not_null())
                    .col(ColumnDef::new(Actions::Entity).text().not_null())
                    .col(ColumnDef::new(Actions::Attribute).text())
                    .col(ColumnDef::new(Actions::ValueType).text())
                    .col(ColumnDef::new(Actions::ValueId).text())
                    .col(ColumnDef::new(Actions::NumberValue).integer())
                    .col(ColumnDef::new(Actions::StringValue).text())
                    .col(ColumnDef::new(Actions::EntityValue).text())
                    .col(ColumnDef::new(Actions::ArrayValue).array(ColumnType::Text))
                    .to_owned(),
            )
            .await?;

        // create the versions table
        manager
            .create_table(
                Table::create()
                    .table(Versions::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(Versions::Id)
                            .text()
                            .unique_key()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(Versions::Name).text())
                    .col(ColumnDef::new(Versions::Description).text())
                    .col(ColumnDef::new(Versions::CreatedAt).integer().not_null())
                    .col(
                        ColumnDef::new(Versions::CreatedAtBlock)
                            .integer()
                            .not_null(),
                    )
                    .col(ColumnDef::new(Versions::CreatedBy).text().not_null())
                    .col(ColumnDef::new(Versions::ProposedVersion).text().not_null())
                    .col(ColumnDef::new(Versions::Actions).array(ColumnType::Text))
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        drop_tables!(
            manager,
            Cursors,
            Spaces,
            Accounts,
            Entities,
            LogEntries,
            EntityAttributes,
            EntityTypes,
            Triples,
            Proposals,
            ProposedVersions,
            Actions,
            Versions
        );
        Ok(())
    }
}

#[derive(DeriveIden)]
enum Cursors {
    Table,
    Id,
    Cursor,
}

#[derive(DeriveIden)]
enum Spaces {
    Table,
    Id,
    Address,
    IsRootSpace,
    CreatedAtBlock,
    Admins,
    Editors,
    EditorControllers,
    Entity,
}

#[derive(DeriveIden)]
enum Entities {
    Table,
    Id,
    Name,
    Description,
    IsType,
    ValueType,
    DefinedIn,
    Version,
    Versions,
}

#[derive(DeriveIden)]
enum Accounts {
    Table,
    Id,
}

#[derive(DeriveIden)]
enum LogEntries {
    Table,
    Id,
    CreatedAtBlock,
    Uri,
    CreatedBy,
    Space,
    MimeType,
    Decoded,
    Json,
}

#[derive(DeriveIden)]
enum EntityAttributes {
    Table,
    Id,
    EntityId,
    AttributeOf,
}

#[derive(DeriveIden)]
enum EntityTypes {
    Table,
    Id,
    EntityId,
    Type,
}

#[derive(DeriveIden)]
enum Triples {
    Table,
    Id,
    EntityId,
    AttributeId,
    ValueId,
    ValueType,
    NumberValue,
    StringValue,
    EntityValue,
    ArrayValue,
    IsProtected,
    DefinedIn,
}

#[derive(DeriveIden)]
enum Proposals {
    Table,
    Id,
    Name,
    Description,
    CreatedBy,
    Space,
    Status,
    CreatedAt, // block timestamp
    ProposedVersions,
    CreatedAtBlock,
}

#[derive(DeriveIden)]
enum ProposedVersions {
    Table,
    Id,
    Name,
    Description,
    CreatedBy,
    Entity,
    CreatedAt, // block timestamp
    CreatedAtBlock,
    Actions,
}

#[derive(DeriveIden)]
enum Versions {
    Table,
    Id,
    Name,
    Description,
    CreatedBy,
    CreatedAt, // block timestamp
    CreatedAtBlock,
    ProposedVersion,
    Actions,
}

#[derive(DeriveIden)]
enum Actions {
    Table,
    Id,
    ActionType,
    Entity,
    Attribute,
    ValueType,
    ValueId,
    NumberValue,
    StringValue,
    EntityValue,
    ArrayValue,
}
