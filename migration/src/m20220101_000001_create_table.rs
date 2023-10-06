use futures03::{
    future::try_join_all,
    stream::{FuturesOrdered, FuturesUnordered},
    StreamExt,
};
use sea_orm_migration::{
    prelude::*,
    sea_orm::{DatabaseBackend, DbBackend, Statement},
};

macro_rules! drop_table {
    ($manager:ident, $table:ident) => {
        $manager
            .drop_table(Table::drop().table($table::Table).cascade().to_owned())
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

        // disable all foreign key checks
        // connection
        //     .execute(Statement::from_string(
        //         DatabaseBackend::Postgres,
        //         "SET session_replication_role = 'replica';",
        //     ))
        //     .await?;

        // connection
        //     .execute(Statement::from_string(
        //         DatabaseBackend::Postgres,
        //         "ALTER SYSTEM SET session_replication_role TO 'replica';",
        //     ))
        //     .await?;

        // connection
        //     .execute(Statement::from_string(
        //         DatabaseBackend::Postgres,
        //         "SELECT pg_reload_conf();",
        //     ))
        //     .await?;

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
                            .primary_key(),
                    )
                    .col(ColumnDef::new(Cursors::Cursor).text().not_null())
                    .col(ColumnDef::new(Cursors::BlockNumber).text())
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
                    .col(ColumnDef::new(Entities::DefinedIn).text().null())
                    .col(ColumnDef::new(Entities::ValueType).text())
                    .col(ColumnDef::new(Entities::Version).text())
                    .col(ColumnDef::new(Entities::Versions).array(ColumnType::Text))
                    .to_owned(),
            )
            .await?;

        // create the entity attribute foreign keys
        manager
            .create_foreign_key(
                ForeignKey::create()
                    .name("entity_attributes_entity_id_fkey")
                    .from(EntityAttributes::Table, EntityAttributes::EntityId)
                    .to(Entities::Table, Entities::Id)
                    .to_owned(),
            )
            .await?;

        manager
            .create_foreign_key(
                ForeignKey::create()
                    .name("entity_attributes_attribute_of_fkey")
                    .from(EntityAttributes::Table, EntityAttributes::AttributeOf)
                    .to(Entities::Table, Entities::Id)
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
                    .foreign_key(
                        ForeignKey::create()
                            .name("entity_types_entity_id_fkey")
                            .from(EntityTypes::Table, EntityTypes::EntityId)
                            .to(Entities::Table, Entities::Id),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("entity_types_type_fkey")
                            .from(EntityTypes::Table, EntityTypes::Type)
                            .to(Entities::Table, Entities::Id),
                    )
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
                    .col(ColumnDef::new(Spaces::Address).text().unique_key())
                    .col(ColumnDef::new(Spaces::CreatedAtBlock).text())
                    .col(ColumnDef::new(Spaces::IsRootSpace).boolean())
                    .col(ColumnDef::new(Spaces::Admins).text())
                    .col(ColumnDef::new(Spaces::EditorControllers).text())
                    .col(ColumnDef::new(Spaces::Editors).text())
                    .col(ColumnDef::new(Spaces::Entity).text())
                    .foreign_key(
                        ForeignKey::create()
                            .name("spaces_id_entity_id_fkey")
                            .from(Spaces::Table, Spaces::Id)
                            .to(Entities::Table, Entities::Id),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_foreign_key(
                ForeignKey::create()
                    .name("entity_defined_in_spaces_address_fkey")
                    .from(Entities::Table, Entities::DefinedIn)
                    .to(Spaces::Table, Spaces::Address)
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
                    .col(
                        ColumnDef::new(Triples::Deleted)
                            .boolean()
                            .not_null()
                            .default(false),
                    )
                    .col(ColumnDef::new(Triples::NumberValue).text())
                    .col(ColumnDef::new(Triples::ArrayValue).text())
                    .col(ColumnDef::new(Triples::StringValue).text())
                    .col(ColumnDef::new(Triples::EntityValue).text())
                    .foreign_key(
                        ForeignKey::create()
                            .name("triples_entity_entity_id_fkey")
                            .from(Triples::Table, Triples::EntityId)
                            .to(Entities::Table, Entities::Id),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("triples_attribute_entity_id_fkey")
                            .from(Triples::Table, Triples::AttributeId)
                            .to(Entities::Table, Entities::Id),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("triples_entity_value_entity_id_fkey")
                            .from(Triples::Table, Triples::EntityValue)
                            .to(Entities::Table, Entities::Id),
                    )
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
                    .col(ColumnDef::new(Actions::NumberValue).text())
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

        let root_space_query =
            format!("CREATE SCHEMA IF NOT EXISTS \"0x170b749413328ac9a94762031a7a05b00c1d2e34\";");

        let table_query =
            format!("CREATE TABLE IF NOT EXISTS \"0x170b749413328ac9a94762031a7a05b00c1d2e34\".\"FOO\" (id text);");

        let bootstrap_root_space_entity =
            format!("INSERT INTO \"public\".\"entities\" (\"id\") VALUES ('root_space');");

        let bootstrap_root_space =
            format!("INSERT INTO \"public\".\"spaces\" (\"id\", \"address\", \"is_root_space\") VALUES ('root_space', '0x170b749413328ac9a94762031a7a05b00c1d2e34', true);");

        let bootstrap_root_space_defined_in =
            format!("UPDATE \"public\".\"entities\" set \"defined_in\" = ('0x170b749413328ac9a94762031a7a05b00c1d2e34') WHERE \"id\" = 'root_space'");

        //iterate over each new table and disable triggers
        let tables = manager
            .get_connection()
            .query_all(Statement::from_string(
                DatabaseBackend::Postgres,
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
            ))
            .await?;

        for table in tables {
            let table_name = table.try_get_by_index::<String>(0).unwrap();
            println!("Disabling triggers for table {}", table_name);
            let query = format!(
                "ALTER TABLE \"public\".\"{}\" DISABLE TRIGGER ALL;",
                table_name
            );

            connection
                .execute(Statement::from_string(DatabaseBackend::Postgres, query))
                .await?;
        }

        connection
            .execute(Statement::from_string(
                DatabaseBackend::Postgres,
                root_space_query,
            ))
            .await?;

        connection
            .execute(Statement::from_string(
                DatabaseBackend::Postgres,
                table_query,
            ))
            .await?;

        connection
            .execute(Statement::from_string(
                DatabaseBackend::Postgres,
                bootstrap_root_space_entity,
            ))
            .await?;

        connection
            .execute(Statement::from_string(
                DatabaseBackend::Postgres,
                bootstrap_root_space,
            ))
            .await?;

        // disable all foreign key checks
        // connection
        //     .execute(Statement::from_string(
        //         DatabaseBackend::Postgres,
        //         "SET session_replication_role = 'origin';",
        //     ))
        //     .await?;

        connection
            .execute(Statement::from_string(
                DatabaseBackend::Postgres,
                bootstrap_root_space_defined_in,
            ))
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // NOTE
        // THIS MIGRATION IS HEAVY
        // YOU WILL PROBABLY NEED TO INCREASE MAX_LOCKS_PER_TX IN YOUR POSTGRES CONFIG
        let schemas = manager
            .get_connection()
            .query_all(Statement::from_string(
                DatabaseBackend::Postgres,
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE '0x%'",
            ))
            .await?;

        let mut table_drops = Vec::new();
        let mut futures = Vec::new();

        for schema in schemas {
            let schema_name = schema.try_get_by_index::<String>(0).unwrap();
            let tables = manager
                .get_connection()
                .query_all(Statement::from_string(
                    DatabaseBackend::Postgres,
                    format!("SELECT table_name FROM information_schema.tables WHERE table_schema = '{}'", schema_name),
                ))
                .await?;

            for table in tables {
                let table_name = table.try_get_by_index::<String>(0).unwrap();
                println!("Dropping table {}", table_name);
                let query = format!(
                    "DROP TABLE IF EXISTS \"{}\".\"{}\" CASCADE;",
                    schema_name, table_name
                );
                table_drops.push(
                    manager
                        .get_connection()
                        .execute(Statement::from_string(DatabaseBackend::Postgres, query)),
                );
            }

            println!("Dropping schema {}", schema_name);
            let query = format!("DROP SCHEMA IF EXISTS \"{}\";", schema_name);
            futures.push(
                manager
                    .get_connection()
                    .execute(Statement::from_string(DatabaseBackend::Postgres, query)),
            );
        }

        try_join_all(table_drops).await?;
        try_join_all(futures).await?;

        drop_tables!(
            manager,
            Cursors,
            Spaces,
            Accounts,
            LogEntries,
            EntityAttributes,
            EntityTypes,
            Triples,
            Proposals,
            ProposedVersions,
            Actions,
            Entities,
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
    BlockNumber,
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
    Deleted,
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
