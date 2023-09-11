use sea_orm_migration::{
    prelude::*,
    sea_orm::{DatabaseBackend, Statement},
};

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
                    .col(ColumnDef::new(Entities::IsType).boolean().default(false))
                    .col(ColumnDef::new(Entities::DefinedIn).text())
                    .col(ColumnDef::new(Entities::ValueType).text())
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
                    .to_owned(),
            )
            .await?;

        // make the triples table
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
                    .col(ColumnDef::new(Triples::Value).text().not_null())
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(Entities::Table).to_owned())
            .await?;

        manager
            .drop_table(Table::drop().table(Spaces::Table).to_owned())
            .await?;

        manager
            .drop_table(Table::drop().table(Cursors::Table).to_owned())
            .await?;

        manager
            .drop_table(Table::drop().table(EntityAttributes::Table).to_owned())
            .await?;

        manager
            .drop_table(Table::drop().table(Triples::Table).to_owned())
            .await?;

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
}

#[derive(DeriveIden)]
enum Entities {
    Table,
    Id,
    Name,
    IsType,
    ValueType,
    DefinedIn,
}

#[derive(DeriveIden)]
enum EntityAttributes {
    Table,
    Id,
    EntityId,
    AttributeOf,
}

#[derive(DeriveIden)]
enum Triples {
    Table,
    Id,
    EntityId,
    AttributeId,
    ValueId,
    ValueType,
    Value,
}
