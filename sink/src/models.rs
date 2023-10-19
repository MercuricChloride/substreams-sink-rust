//! A bunch of modules containing helpers for working with the database
//! These are going to just hide the implementation details of the database
//! and provide a nice interface for the rest of the application to use

pub fn table_comment_string(space: &str, entity_id: &str, entity_name: &str) -> String {
    format!(
        "DO $$
BEGIN
   IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = '{space}' AND tablename = '{entity_id}') THEN
      COMMENT ON TABLE \"{space}\".\"{entity_id}\" IS E'@name {entity_name}entity';
   END IF;
END $$;
",
                        space = space,
                        entity_id = entity_id,
                        entity_name = escape(entity_name)
                    )
}

/// strips all non alphanumeric characters that aren't spaces
pub fn escape(input: &str) -> String {
    input
        .chars()
        .filter(|c| c.is_alphanumeric() || c.is_whitespace())
        .filter(|c| c != &'\n')
        .collect::<String>()
}

pub fn triple_exists_string(entity: &str, attribute: &str, value: &str) -> String {
    format!("SELECT EXISTS(SELECT * from \"public\".\"triples\" WHERE \"entity_id\" = '{entity}' AND \"attribute_id\" = '{attribute}' AND \"value_id\" = '{value}');")
}

pub mod spaces {
    use anyhow::Error;
    use entity::{spaces::*, subspaces};
    use migration::{DbErr, OnConflict};
    use sea_orm::{
        ActiveValue, ConnectionTrait, DatabaseConnection, DatabaseTransaction, DbBackend,
        EntityTrait, Statement,
    };

    pub async fn create_schema(db: &DatabaseTransaction, schema_name: &str) -> Result<(), DbErr> {
        let schema_query = format!("CREATE SCHEMA IF NOT EXISTS \"{schema_name}\";");

        db.execute(Statement::from_string(DbBackend::Postgres, schema_query))
            .await?;

        Ok(())
    }

    pub async fn create(
        db: &DatabaseTransaction,
        space_id: String,
        address: String,
        created_in_space: String,
    ) -> Result<(), DbErr> {
        // make the entity for the space if it doesn't exist
        //super::entities::create(db, space_id.clone(), created_in_space).await?;

        let space = ActiveModel {
            id: ActiveValue::Set(space_id),
            address: ActiveValue::Set(address),
            ..Default::default()
        };

        // create the space
        Entity::insert(space)
            .on_conflict(
                OnConflict::column(Column::Id)
                    .update_column(Column::Address)
                    .to_owned(),
            )
            .exec(db)
            .await?;
        Ok(())
    }

    pub async fn upsert_cover(
        db: &DatabaseTransaction,
        space: &str,
        cover_image: &str,
    ) -> Result<(), DbErr> {
        let space = ActiveModel {
            id: ActiveValue::Set(space.to_string()),
            cover: ActiveValue::Set(Some(cover_image.to_string())),
            ..Default::default()
        };

        // create the space
        Entity::insert(space)
            .on_conflict(
                OnConflict::column(Column::Id)
                    .update_column(Column::Cover)
                    .to_owned(),
            )
            .exec(db)
            .await?;
        Ok(())
    }

    pub async fn exists(db: &DatabaseTransaction, space: &str) -> Result<bool, Error> {
        let space = Entity::find_by_id(space).one(db).await?;
        Ok(space.is_some())
    }

    pub async fn add_subspace(
        db: &DatabaseTransaction,
        parent_space_id: &str,
        child_space_id: &str,
    ) -> Result<(), Error> {
        let id = format!("{parent_space_id}-{child_space_id}");

        let model = subspaces::ActiveModel {
            id: ActiveValue::Set(id),
            parent_space: ActiveValue::Set(parent_space_id.to_string()),
            child_space: ActiveValue::Set(child_space_id.to_string()),
        };

        subspaces::Entity::insert(model)
            .on_conflict(
                OnConflict::column(subspaces::Column::Id)
                    .do_nothing()
                    .to_owned(),
            )
            .do_nothing()
            .exec(db)
            .await?;

        Ok(())
    }

    pub async fn remove_subspace(
        db: &DatabaseTransaction,
        parent_space_id: &str,
        child_space_id: &str,
    ) -> Result<(), Error> {
        let id = format!("{parent_space_id}-{child_space_id}");

        let model = subspaces::ActiveModel {
            id: ActiveValue::Set(id),
            parent_space: ActiveValue::Set(parent_space_id.to_string()),
            child_space: ActiveValue::Set(child_space_id.to_string()),
        };

        subspaces::Entity::delete(model).exec(db).await?;

        Ok(())
    }
}

pub mod entities {
    use anyhow::Error;
    use entity::{entities::*, entity_attributes, entity_types};
    use migration::{DbErr, OnConflict};
    use sea_orm::{
        ActiveModelTrait, ActiveValue, ConnectionTrait, DatabaseConnection, DatabaseTransaction,
        DbBackend, EntityTrait, IsolationLevel, Statement, TransactionTrait,
    };

    use crate::{constants, triples::ValueType};

    use super::table_comment_string;

    pub async fn exists(db: &DatabaseTransaction, entity_id: &str) -> Result<bool, DbErr> {
        let entity = Entity::find_by_id(entity_id).one(db).await?;
        Ok(entity.is_some())
    }

    pub async fn create_table(db: &DatabaseTransaction, entity_id: &str) -> Result<(), Error> {
        // println!("Creating table for entity {}", entity_id);

        let entity = Entity::find_by_id(entity_id.clone()).one(db).await?;

        if let None = entity {
            return Err(Error::msg(format!("Entity {} doesn't exist", entity_id)));
        }
        let entity = entity.unwrap();

        let table_create_statement = format!(
            "CREATE TABLE IF NOT EXISTS \"{space}\".\"{entity_id}\" (
                id TEXT PRIMARY KEY,
                entity_id TEXT NOT NULL REFERENCES \"public\".\"entities\"(id)
            );",
            space = entity.defined_in,
            entity_id = entity_id
        );

        let table_disable_statement = format!(
            "ALTER TABLE \"{space}\".\"{entity_id}\" DISABLE TRIGGER ALL;",
            space = entity.defined_in,
            entity_id = entity_id
        );

        let mut table_create_result = None;

        let mut retry_count = 0;

        while let None = table_create_result {
            let result = db
                .execute(Statement::from_string(
                    DbBackend::Postgres,
                    &table_create_statement,
                ))
                .await;

            db.execute(Statement::from_string(
                DbBackend::Postgres,
                &table_disable_statement,
            ))
            .await?;

            if let Ok(result) = result {
                table_create_result = Some(result);
            } else if retry_count == 3 {
                table_create_result = None;
            } else {
                retry_count += 1;
            }
        }

        // If the entity has a name, we need to add a comment to the table
        if let Some(entity_name) = entity.name {
            let table_comment = table_comment_string(&entity.defined_in, entity_id, &entity_name);

            let result = db
                .execute(Statement::from_string(DbBackend::Postgres, &table_comment))
                .await;
            if let Err(err) = result {
                // println!(
                //     "Couldn't add comment to table for entity {} for space {}. \n\n {:?}",
                //     entity_id, entity.defined_in, err
                // );
                // println!("Comment: {}", table_comment);
            }
        }

        Ok(())
    }

    /// This function adds a relation to an entity's table
    /// Because of the way postgraphile works, we need to add the column, with a reference, to the attribute's table
    /// prefixed with "parent_", and a reference to the entity's table, which is the entity-id
    pub async fn add_relation(
        db: &DatabaseTransaction,
        parent_entity_id: &str,
        attribute_id: &str,
        space: &str,
    ) -> Result<(), Error> {
        let relation_entity = Entity::find_by_id(attribute_id).one(db).await?;

        let is_relation = if let Some(value_entity) = relation_entity {
            if let Some(value_type) = value_entity.value_type {
                if value_type == constants::Entities::Relation.id() {
                    true
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        };

        if is_relation {
            let child_entity_id = attribute_id;
            // println!(
            //     "Adding relation from child {} to parent {} for space {}",
            //     child_entity_id, parent_entity_id, space
            // );

            // grab the entity of the child
            let child_entity = Entity::find_by_id(child_entity_id).one(db).await?;

            if let None = child_entity {
                return Err(Error::msg(format!(
                    "Child entity {} doesn't exist",
                    child_entity_id
                )));
            }
            let child_entity = child_entity.unwrap();

            // grab the entity of the parent
            let parent_entity = Entity::find_by_id(parent_entity_id).one(db).await?;
            if let None = parent_entity {
                return Err(Error::msg(format!(
                    "Parent entity {} doesn't exist",
                    parent_entity_id
                )));
            }
            let parent_entity = parent_entity.unwrap();

            let child_space = child_entity.defined_in;
            let parent_space = parent_entity.defined_in;

            // check if the table exists
            let child_table_exists_statement = format!(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = '{foo}' AND table_name = '{entity}');",
                foo = child_space,
                entity = child_entity_id
            );

            let parent_table_exists_statement = format!(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = '{foo}' AND table_name = '{entity}');",
                foo = parent_space,
                entity = parent_entity_id
            );

            let child_table_result = db
                .query_one(Statement::from_string(
                    DbBackend::Postgres,
                    child_table_exists_statement,
                ))
                .await?;

            let parent_table_result = db
                .query_one(Statement::from_string(
                    DbBackend::Postgres,
                    parent_table_exists_statement,
                ))
                .await?;

            if let (Some(child_table), Some(parent_table)) =
                (child_table_result, parent_table_result)
            {
                let child_table_exists: bool = child_table.try_get_by_index(0 as usize).unwrap();

                let txn = db.begin().await?;
                if !child_table_exists {
                    create_table(&txn, &child_entity.id).await?;
                }
                txn.commit().await?;

                let parent_table_exists: bool = parent_table.try_get_by_index(0 as usize).unwrap();

                let txn = db.begin().await?;
                if !parent_table_exists {
                    create_table(&txn, &parent_entity.id).await?;
                }
                txn.commit().await?;

                let column_add_statement = format!(
                    "ALTER TABLE \"{child_space}\".\"{child_entity}\" ADD COLUMN IF NOT EXISTS \"attr_{parent_entity}\" TEXT REFERENCES \"{parent_space}\".\"{parent_entity}\"(id);",
                    child_space = child_space,
                    child_entity = child_entity_id,
                    parent_space = parent_space,
                    parent_entity = parent_entity_id
                );

                let txn = db.begin().await?;
                txn.execute(Statement::from_string(
                    DbBackend::Postgres,
                    column_add_statement,
                ))
                .await?;
                txn.commit().await?;

                if let Some(parent_entity_name) = parent_entity.name {
                    let txn = db.begin().await?;
                    let column_name_statement = format!(
                        "COMMENT ON COLUMN \"{child_space}\".\"{child_entity}\".\"attr_{parent_entity}\" IS E'@name {parent_name}';",
                        child_space = child_space,
                        child_entity = child_entity_id,
                        parent_entity = parent_entity_id,
                        parent_name = parent_entity_name,
                    );

                    txn.execute(Statement::from_string(
                        DbBackend::Postgres,
                        column_name_statement,
                    ))
                    .await?;

                    txn.commit().await?
                }
            } else {
                panic!("DOESN'T EXIST");
            }
        } else {
            let attribute = Entity::find_by_id(attribute_id.clone()).one(db).await?;

            if let None = attribute {
                return Err(Error::msg(format!(
                    "Attribute {} doesn't exist",
                    attribute_id
                )));
            }
            let attribute = attribute.unwrap();

            let attribute_name = attribute.name.unwrap_or(attribute.id.clone());
            // otherwise we just need to add a column with text
            let column_add_statement = format!(
                "ALTER TABLE \"{space}\".\"{entity}\" ADD COLUMN IF NOT EXISTS \"attr_{attribute}\" TEXT;",
                space = space,
                entity = parent_entity_id,
                attribute = attribute.id
            );

            db.execute(Statement::from_string(
                DbBackend::Postgres,
                column_add_statement,
            ))
            .await?;
            let column_name_statement = format!(
                "COMMENT ON COLUMN \"{space}\".\"{entity}\".\"attr_{attribute_id}\" IS E'@name {attribute_name}';",
                space = space,
                entity = parent_entity_id,
                attribute_id = attribute_id,
                attribute_name = attribute_name,
            );

            db.execute(Statement::from_string(
                DbBackend::Postgres,
                column_name_statement,
            ))
            .await?;
        }

        Ok(())
    }

    /// This function handles a type being added to an entity
    /// It populates the type's table with the entity's id
    pub async fn add_type(
        db: &DatabaseTransaction,
        entity_id: &str,
        type_id: &str,
        space: &str,
        space_queries: bool,
    ) -> Result<(), DbErr> {
        // create the entity and type if they don't exist
        //create(db, entity_id.clone(), space.clone()).await?;

        //create(db, type_id.clone(), space.clone()).await?;

        let entity = entity_types::ActiveModel {
            id: ActiveValue::Set(format!("{}-{}", entity_id, type_id)),
            entity_id: ActiveValue::Set(entity_id.to_owned()),
            r#type: ActiveValue::Set(type_id.to_owned()),
        };

        entity_types::Entity::insert(entity)
            .on_conflict(OnConflict::column(Column::Id).do_nothing().to_owned())
            .do_nothing()
            .exec(db)
            .await?;

        if space_queries {
            // grab the space the type is defined in
            let type_space = Entity::find_by_id(type_id.clone())
                .one(db)
                .await?
                .unwrap()
                .defined_in;

            // println!(
            //     "Adding type {} to entity {} for space {}",
            //     type_id, entity_id, space
            // );

            if !type_space.is_empty() && !type_id.is_empty() && !entity_id.is_empty() {
                let type_insert_statement = format!(
                "INSERT INTO \"{type_space}\".\"{type_id}\" (\"id\", \"entity_id\") VALUES ('{entity_id}', '{entity_id}') ON CONFLICT (id) DO NOTHING;",
                );

                db.execute(Statement::from_string(
                    DbBackend::Postgres,
                    type_insert_statement,
                ))
                .await?;
            }
        }

        Ok(())
    }

    pub async fn create(
        db: &DatabaseTransaction,
        entity_id: &str,
        space: &str,
    ) -> Result<(), DbErr> {
        let entity = ActiveModel {
            id: ActiveValue::Set(entity_id.into()),
            defined_in: ActiveValue::Set(space.into()),
            ..Default::default()
        };

        Entity::insert(entity)
            .on_conflict(OnConflict::column(Column::Id).do_nothing().to_owned())
            .do_nothing()
            .exec(db)
            .await?;

        Ok(())
    }

    pub async fn upsert_value_type(
        db: &DatabaseTransaction,
        entity_id: String,
        value_type: String,
        space: String,
    ) -> Result<(), Error> {
        // make the entity for the space if it doesn't exist
        let entity = Entity::find_by_id(entity_id.clone()).one(db).await?;

        if let None = entity {
            return Err(Error::msg(
                "Couldn't add value type as entity doesn't exist",
            ));
        }

        let entity = entity.unwrap();
        let mut entity: ActiveModel = entity.into();
        entity.value_type = ActiveValue::Set(Some(value_type.to_string()));

        Entity::insert(entity)
            .on_conflict(
                OnConflict::column(Column::Id)
                    .update_column(Column::ValueType)
                    .to_owned(),
            )
            .exec(db)
            .await?;

        Ok(())
    }

    pub async fn upsert_name(
        db: &DatabaseTransaction,
        entity_id: &str,
        name: &str,
        space: &str,
        space_queries: bool,
    ) -> Result<(), DbErr> {
        let entity = ActiveModel {
            id: ActiveValue::Set(entity_id.to_string()),
            name: ActiveValue::Set(Some(name.to_string())),
            defined_in: ActiveValue::Set(space.to_string()),
            ..Default::default()
        };
        Entity::insert(entity)
            .on_conflict(
                OnConflict::column(Column::Id)
                    .update_column(Column::Name)
                    .to_owned(),
            )
            .exec(db)
            .await?;
        // }

        // if the entity is a type, we need to add a comment updating the name of the table
        if space_queries {
            if let Some(entity) = Entity::find_by_id(entity_id).one(db).await? {
                if let (Some(entity_name), Some(is_type)) = (entity.name, entity.is_type) {
                    if !is_type {
                        return Ok(());
                    }

                    let table_comment = table_comment_string(&space, &entity_id, &entity_name);
                    println!("Table comment: {}", table_comment);
                    if space.is_empty() || entity_id.is_empty() || entity_name.is_empty() {
                        return Ok(());
                    }

                    let result = db
                        .execute(Statement::from_string(
                            DbBackend::Postgres,
                            table_comment.clone(),
                        ))
                        .await;
                    if let Err(err) = result {
                        println!(
                            "Couldn't add comment to table for entity {} for space {}. \n\n {:?}",
                            entity_id, space, err
                        );
                    }
                }
            }

            // if the entity is an attribute, we need to rename all of the columns that have the name of: "attr_{entity_id}"
            let tables_query = format!(
                "SELECT table_name
                FROM information_schema.columns
                WHERE column_name = 'attr_{entity_id}';",
            );

            let tables = db
                .query_all(Statement::from_string(DbBackend::Postgres, tables_query))
                .await?;

            for table in tables {
                let table_name: String = table.try_get_by_index(0 as usize).unwrap();
                let column_name_statement = format!(
                    "COMMENT ON COLUMN \"{space}\".\"{table_name}\".\"attr_{entity_id}\" IS E'@name {name}';",
                    space = space,
                    table_name = table_name,
                    entity_id = entity_id,
                    name = name
                );

                db.execute(Statement::from_string(
                    DbBackend::Postgres,
                    column_name_statement,
                ))
                .await?;
            }
        }

        Ok(())
    }

    pub async fn upsert_description(
        db: &DatabaseTransaction,
        entity_id: &str,
        description: &str,
        space: &str,
    ) -> Result<(), DbErr> {
        if let Some(entity) = Entity::find_by_id(entity_id).one(db).await? {
            let mut entity: ActiveModel = entity.into();
            entity.description = ActiveValue::Set(Some(description.to_string()));
            entity.save(db).await?;
        } else {
            let entity = ActiveModel {
                id: ActiveValue::Set(entity_id.to_string()),
                description: ActiveValue::Set(Some(description.to_string())),
                defined_in: ActiveValue::Set(space.to_string()),
                ..Default::default()
            };

            Entity::insert(entity)
                .on_conflict(
                    OnConflict::column(Column::Id)
                        .update_column(Column::Description)
                        .to_owned(),
                )
                .exec(db)
                .await?;
        }

        Ok(())
    }

    pub async fn upsert_is_type(
        db: &DatabaseTransaction,
        entity_id: &str,
        is_type: bool,
        space: &str,
    ) -> Result<(), DbErr> {
        let entity = ActiveModel {
            id: ActiveValue::Set(entity_id.to_string()),
            is_type: ActiveValue::Set(Some(is_type)),
            defined_in: ActiveValue::Set(space.to_string()),
            ..Default::default()
        };

        Entity::insert(entity)
            .on_conflict(
                OnConflict::column(Column::Id)
                    .update_column(Column::IsType)
                    .to_owned(),
            )
            .exec(db)
            .await?;

        Ok(())
    }

    /// !!!NOTE!!! attribute_of_id is the id of the entity we are adding an attribute to
    pub async fn add_attribute(
        db: &DatabaseTransaction,
        entity_id: String,
        attribute_of_id: String,
    ) -> Result<(), DbErr> {
        let id = uuid::Uuid::new_v4().to_string();
        let entity = entity_attributes::ActiveModel {
            id: ActiveValue::Set(id),
            entity_id: ActiveValue::Set(entity_id),
            attribute_of: ActiveValue::Set(attribute_of_id),
        };

        entity_attributes::Entity::insert(entity)
            .on_conflict(
                OnConflict::column(entity_attributes::Column::Id)
                    .do_nothing()
                    .to_owned(),
            )
            .do_nothing()
            .exec(db)
            .await?;

        Ok(())
    }
}

/// A helper module for storing and retrieving the cursor from the db
pub mod cursor {
    use entity::cursors;
    use migration::{DbErr, OnConflict};
    use sea_orm::{ActiveValue, DatabaseConnection, EntityTrait};

    pub async fn store(
        db: &DatabaseConnection,
        cursor_string: String,
        block_number: u64,
    ) -> Result<(), DbErr> {
        let cursor = cursors::Entity::find_by_id(0).one(db).await?;
        if let Some(_) = cursor {
            cursors::Entity::update(cursors::ActiveModel {
                id: ActiveValue::Set(0),
                cursor: ActiveValue::Set(cursor_string),
                block_number: ActiveValue::Set(block_number.to_string()),
            })
            .exec(db)
            .await?;
        } else {
            let cursor = cursors::ActiveModel {
                id: ActiveValue::Set(0),
                cursor: ActiveValue::Set(cursor_string),
                block_number: ActiveValue::Set(block_number.to_string()),
            };

            cursors::Entity::insert(cursor)
                .on_conflict(
                    OnConflict::column(cursors::Column::Id)
                        .do_nothing()
                        .to_owned(),
                )
                .do_nothing()
                .exec(db)
                .await?;
        }

        Ok(())
    }

    pub async fn get(db: &DatabaseConnection) -> Result<Option<String>, DbErr> {
        let cursor = cursors::Entity::find_by_id(0).one(db).await?;

        if let Some(cursor) = cursor {
            return Ok(Some(cursor.cursor));
        } else {
            Ok(None)
        }
    }

    pub async fn get_block_number(db: &DatabaseConnection) -> Result<Option<String>, DbErr> {
        let cursor = cursors::Entity::find_by_id(0).one(db).await?;

        if let Some(cursor) = cursor {
            return Ok(Some(cursor.block_number));
        } else {
            Ok(None)
        }
    }
}

pub mod accounts {
    use entity::accounts::*;
    use migration::{DbErr, OnConflict};
    use sea_orm::{ActiveValue, ConnectionTrait, DatabaseTransaction, EntityTrait};

    pub async fn create(db: &DatabaseTransaction, address: &str) -> Result<(), DbErr> {
        let account = Entity::find_by_id(address.clone()).one(db).await?;

        if let None = account {
            let account = ActiveModel {
                id: ActiveValue::Set(address.to_string()),
                ..Default::default()
            };

            Entity::insert(account)
                .on_conflict(OnConflict::column(Column::Id).do_nothing().to_owned())
                .do_nothing()
                .exec(db)
                .await?;
        } else {
        }

        Ok(())
    }
}

pub mod triples {
    use anyhow::Error;
    use entity::{entity_attributes, entity_types, triples::*};
    use migration::{DbErr, OnConflict};
    use sea_orm::{
        ActiveModelTrait, ActiveValue, ColumnTrait, ConnectionTrait, DatabaseConnection,
        DatabaseTransaction, DbBackend, EntityOrSelect, EntityTrait, QueryFilter, QuerySelect, Set,
        Statement, TransactionTrait,
    };
    use sea_query::{Condition, Query, RcOrArc};
    use tokio::{task, time::sleep};
    use std::{sync::Arc, time::Duration};
    use uuid::Uuid;

    use crate::{
        constants::{Attributes, Entities, ROOT_SPACE_ADDRESS},
        models::entities::{self, upsert_is_type},
        triples::{Action, ActionTriple, ValueType},
        try_action,
    };

    use super::triple_exists_string;

    //  To input the data for a triple:
    //  1. Find all the types the entity has
    //  2. For each of those types, find the types that have the attribute on them
    //  3. For all of those types w the attribute, insert the data on the entity_id row

    pub async fn create(
        db: &DatabaseTransaction,
        entity_id: &str,
        attribute_id: &str,
        value: ValueType,
        space: &str,
        author: &str,
    ) -> Result<(), DbErr> {
        let id = format!("{}", Uuid::new_v4());

        if let Some(_) = Entity::find_by_id(&id).one(db).await? {
            return Ok(());
        } else {
            let mut triple = ActiveModel {
                id: Set(id.clone()),
                entity_id: Set(entity_id.to_string()),
                attribute_id: Set(attribute_id.to_string()),
                value_id: Set(value.id().to_string()),
                value_type: Set(value.value_type().to_string()),
                defined_in: Set(space.to_string()),
                is_protected: Set(false),
                ..Default::default()
            };

            match value {
                ValueType::Number { id: _, value } => {
                    triple.number_value = ActiveValue::Set(Some(value.to_string()));
                }
                ValueType::String { id: _, value } => {
                    triple.string_value = ActiveValue::Set(Some(value.to_string()));
                }
                ValueType::Image { id: _, value } => {
                    triple.string_value = ActiveValue::Set(Some(value.to_string()));
                }
                ValueType::Entity { id } => {
                    triple.entity_value = ActiveValue::Set(Some(id.to_string()));
                }
                ValueType::Date { id: _, value } => {
                    triple.string_value = ActiveValue::Set(Some(value.to_string()));
                }
                ValueType::Url { id: _, value } => {
                    triple.string_value = ActiveValue::Set(Some(value.to_string()));
                }
            }

            Entity::insert(triple)
                .on_conflict(OnConflict::column(Column::Id).do_nothing().to_owned())
                .do_nothing()
                .exec(db)
                .await?;

            Ok(())
        }
    }

    pub async fn delete(
        db: &DatabaseTransaction,
        entity_id: &str,
        attribute_id: &str,
        value: ValueType,
        space: &str,
        author: &str,
    ) -> Result<(), DbErr> {
        let triple = Entity::find()
            .filter(Column::EntityId.contains(entity_id))
            .filter(Column::AttributeId.contains(attribute_id))
            .filter(Column::ValueId.contains(value.id()))
            .one(db)
            .await?;

        if let Some(triple) = triple {
            let mut triple: ActiveModel = triple.into();
            triple.deleted = Set(true);
            triple.save(db).await?;
        }
        Ok(())
    }

    /// This function inserts the triple data into the appropriate table(s)
    pub async fn insert_triple_data(
        db: &DatabaseTransaction,
        entity_id: &str,
        attribute_id: &str,
        value: ValueType,
    ) -> Result<(), DbErr> {
        let value = value.value();

let bulk_insert = format!("
DO $$
DECLARE
    name text;
    name_schema text;
    ATTRIBUTE_ID TEXT := '{attribute_id}';
    ENTITY_ID_VALUE TEXT := '{entity_id}';
    ATTR_VALUE TEXT := '{value}';
BEGIN
    FOR name, name_schema IN
        SELECT table_name, table_schema
        FROM information_schema.columns
        WHERE column_name = 'entity_id'
        AND table_name IN (SELECT table_name FROM information_schema.columns WHERE column_name = 'attr_' || ATTRIBUTE_ID)
    LOOP    
        EXECUTE format('INSERT INTO %I.%I (id, entity_id, \"attr_%s\") VALUES (%L, %L, %L) ON CONFLICT (id) DO UPDATE SET \"attr_%s\" = EXCLUDED.\"attr_%s\"', name_schema, name, ATTRIBUTE_ID, ENTITY_ID_VALUE, ENTITY_ID_VALUE, ATTR_VALUE, ATTRIBUTE_ID, ATTRIBUTE_ID);
    END LOOP;
END $$;
");
        let result = db.execute(Statement::from_string(
            DbBackend::Postgres,
            bulk_insert,
        ))
        .await;

        if let Err(err) = result {
            println!("\n\n\n\n Error inserting triple data: {:?}\n\n\n\n", err);
            sleep(Duration::from_millis(1500)).await;
        }
        Ok(())
    }

    pub async fn bootstrap(db: Arc<DatabaseConnection>) -> Result<(), Error> {
        println!("Starting bootstrap");
        use strum::IntoEnumIterator;
        let txn = db.begin().await?;
        let author = "BOOTSTRAP";

        let name_attribute = Attributes::Name.id();
        let type_attribute = Attributes::Type.id();
        let attribute_entity = Entities::Attribute.id();
        let value_type_attribute = Attributes::ValueType.id();

        let mut action_triples = Vec::new();

        let space = ROOT_SPACE_ADDRESS.to_string();

        for entity in Entities::iter() {
            // make an entity for the entity
            let entity_id = entity.id();
            entities::create(&txn, entity_id, &space).await?;
        }

        for attribute in Attributes::iter() {
            // make an entity for the attribute
            let entity_id = attribute.id();
            entities::create(&txn, entity_id.into(), &space).await?;
        }
        txn.commit().await?;

        for attribute in Attributes::iter() {
            // bootstrap the name of the attribute
            let entity_id = attribute.id();
            let value = attribute.name();
            let value = ValueType::String {
                id: entity_id.into(),
                value: value.into(),
            };

            let action = ActionTriple::CreateTriple {
                entity_id: entity_id.into(),
                attribute_id: name_attribute.into(),
                value: value.into(),
                space: space.clone(),
                author: author.into(),
            };
            action_triples.push(action);

            // bootstrap the attribute to have a type of attribute
            let value = ValueType::Entity {
                id: attribute_entity.into(),
            };
            let action = ActionTriple::CreateTriple {
                entity_id: entity_id.into(),
                attribute_id: type_attribute.into(),
                value: value.into(),
                space: space.clone(),
                author: author.into(),
            };
            action_triples.push(action);

            // bootstrap the value_type of the attribute if it has one
            if let Some(value_type) = attribute.value_type() {
                let value = ValueType::Entity {
                    id: value_type.id().into(),
                };
                let action = ActionTriple::CreateTriple {
                    entity_id: entity_id.into(),
                    attribute_id: value_type_attribute.into(),
                    value: value.into(),
                    space: space.clone(),
                    author: author.into(),
                };
                action_triples.push(action);
            }
        }

        for entity in Entities::iter() {
            // bootstrap the name of the entity
            let entity_id = entity.id();
            let space = ROOT_SPACE_ADDRESS.to_string();
            let value = entity.name();
            let value = ValueType::String {
                id: entity_id.into(),
                value: value.into(),
            };

            // make the entity a type
            let action = ActionTriple::CreateTriple {
                entity_id: entity_id.into(),
                attribute_id: Attributes::Type.id().into(),
                value: ValueType::Entity {
                    id: Entities::SchemaType.id().into(),
                },
                space: space.clone(),
                author: author.to_string(),
            };
            action_triples.push(action);

            let action = ActionTriple::CreateTriple {
                entity_id: entity_id.into(),
                attribute_id: name_attribute.into(),
                value: value.into(),
                space: space.clone(),
                author: author.to_string(),
            };
            action_triples.push(action);

            // bootstrap the entity to have a type of schema type
            let value = ValueType::Entity {
                id: Entities::SchemaType.id().into(),
            };
            let action = ActionTriple::CreateTriple {
                entity_id: entity_id.into(),
                attribute_id: type_attribute.into(),
                value: value.into(),
                space: space.clone(),
                author: author.to_string(),
            };
            action_triples.push(action);

            for attribute in entity.attributes() {
                // add the attribute to the entity
                let value = ValueType::Entity {
                    id: attribute.id().into(),
                };
                let action = ActionTriple::CreateTriple {
                    entity_id: entity_id.into(),
                    attribute_id: attribute_entity.into(),
                    value: value.into(),
                    space: space.clone(),
                    author: author.to_string(),
                };
                action_triples.push(action);
            }
        }

        let action: Action = Action {
            action_type: "asdf".into(),
            version: "asfd".into(),
            actions: action_triples,
            space,
            author: author.into(),
        };
        let sink_actions = action.get_sink_actions();

        try_action(sink_actions, db, true, author, ROOT_SPACE_ADDRESS).await?;
        println!("Finished boostrap");

        Ok(())
    }

    pub async fn exists(
        db: &DatabaseTransaction,
        entity_id: &str,
        attribute_id: &str,
        value_id: &str,
    ) -> Result<bool, Error> {
        let query_string = triple_exists_string(entity_id, attribute_id, value_id);

        let result = db
            .query_one(Statement::from_string(DbBackend::Postgres, query_string))
            .await?;

        if let Some(result) = result {
            let exists: bool = result
                .try_get_by_index(0 as usize)
                .expect("COULDN'T GET EXISTS");
            return Ok(exists);
        } else {
            return Ok(false);
        }
    }
}

/// This module handles the creation of actions in the database
pub mod actions {
    use entity::actions::*;
    use migration::{DbErr, OnConflict};
    use sea_orm::ConnectionTrait;
    use sea_orm::DatabaseTransaction;
    use sea_orm::EntityTrait;
    use sea_orm::Set;

    use crate::triples::{ActionTriple, ValueType};

    pub async fn create(
        db: &DatabaseTransaction,
        action_triple: &ActionTriple,
    ) -> Result<(), DbErr> {
        let id = format!("{}", uuid::Uuid::new_v4());

        let action_type = action_triple.action_type().to_string();

        let mut action = ActiveModel {
            id: Set(id.clone()),
            action_type: Set(action_type),
            entity: Set(action_triple.entity_id().to_string()),
            ..Default::default()
        };

        match action_triple {
            ActionTriple::CreateTriple {
                attribute_id,
                value,
                ..
            } => {
                action.attribute = Set(Some(attribute_id.to_string()));
                action.value_type = Set(Some(value.value_type().to_string()));
                action.value_id = Set(Some(value.id().to_string()));
                match value {
                    ValueType::Number { value, .. } => {
                        action.number_value = Set(Some(value.to_string()));
                    }
                    ValueType::String { value, .. } => {
                        action.string_value = Set(Some(value.to_string()));
                    }
                    ValueType::Image { value, .. } => {
                        action.string_value = Set(Some(value.to_string()));
                    }
                    ValueType::Entity { id, .. } => {
                        action.entity_value = Set(Some(id.to_string()));
                    }
                    ValueType::Date { value, .. } => {
                        action.string_value = Set(Some(value.to_string()));
                    }
                    ValueType::Url { value, .. } => {
                        action.string_value = Set(Some(value.to_string()));
                    }
                }
            }
            ActionTriple::DeleteTriple {
                attribute_id,
                value,
                ..
            } => {
                action.attribute = Set(Some(attribute_id.to_string()));
                action.value_type = Set(Some(value.value_type().to_string()));
                action.value_id = Set(Some(value.id().to_string()));
                match value {
                    ValueType::Number { value, .. } => {
                        action.number_value = Set(Some(value.to_string()));
                    }
                    ValueType::String { value, .. } => {
                        action.string_value = Set(Some(value.to_string()));
                    }
                    ValueType::Image { value, .. } => {
                        action.string_value = Set(Some(value.to_string()));
                    }
                    ValueType::Entity { id, .. } => {
                        action.entity_value = Set(Some(id.to_string()));
                    }
                    ValueType::Date { value, .. } => {
                        action.string_value = Set(Some(value.to_string()));
                    }
                    ValueType::Url { value, .. } => {
                        action.string_value = Set(Some(value.to_string()));
                    }
                }
            }
            _ => {}
        };

        Entity::insert(action)
            .on_conflict(
                OnConflict::column(entity::actions::Column::Id)
                    .do_nothing()
                    .to_owned(),
            )
            .do_nothing()
            .exec(db)
            .await?;

        Ok(())
    }
}
