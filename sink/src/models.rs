//! A bunch of modules containing helpers for working with the database
//! These are going to just hide the implementation details of the database
//! and provide a nice interface for the rest of the application to use

pub mod spaces {
    use entity::spaces::*;
    use migration::{ConnectionTrait, DbErr, OnConflict};
    use sea_orm::{ActiveValue, DatabaseConnection, DbBackend, EntityTrait, Schema, Statement};
    use tokio::sync::mpsc::Sender;

    pub async fn create_schema(
        db: &DatabaseConnection,
        schema_name: &String,
        sender: &Sender<String>,
    ) -> Result<(), DbErr> {
        let message = format!("Creating schema {schema_name}");
        sender.send(message).await.unwrap();

        let schema_query = format!("CREATE SCHEMA IF NOT EXISTS \"{schema_name}\";");

        db.execute(Statement::from_string(DbBackend::Postgres, schema_query))
            .await?;

        let message = format!("Creating table for {schema_name}");
        sender.send(message).await.unwrap();
        let table_query = format!("CREATE TABLE \"{schema_name}\".\"FOO\"(id text);");

        db.execute(Statement::from_string(DbBackend::Postgres, table_query))
            .await?;

        let message = format!("Inserting test data for table {schema_name}");
        sender.send(message).await.unwrap();
        let insert = format!("INSERT INTO \"{schema_name}\".\"FOO\" VALUES ('bar');");
        db.execute(Statement::from_string(DbBackend::Postgres, insert))
            .await?;

        drop(sender);
        Ok(())
    }

    pub async fn create(
        db: &DatabaseConnection,
        space_id: String,
        address: String,
        sender: &Sender<String>,
    ) -> Result<(), DbErr> {
        let space = ActiveModel {
            id: ActiveValue::Set(space_id.to_owned()),
            address: ActiveValue::Set(Some(address)),
            ..Default::default()
        };

        let message = format!("Creating space {}", space_id);
        sender.send(message).await.unwrap();
        drop(sender);

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
}

pub mod entities {
    use entity::{entities::*, entity_attributes};
    use migration::{ConnectionTrait, DbErr, OnConflict};
    use sea_orm::{
        ActiveModelTrait, ActiveValue, DatabaseConnection, DbBackend, EntityTrait, Statement,
    };
    use tokio::sync::mpsc::Sender;

    pub async fn create_table(
        db: &DatabaseConnection,
        entity_id: &String,
        space: &String,
        sender: &Sender<String>,
    ) -> Result<(), DbErr> {
        let table_create_statement = format!(
            "CREATE TABLE IF NOT EXISTS \"{space}\".\"{entity_id}\" (
                id TEXT PRIMARY KEY,
                entity_id TEXT NOT NULL REFERENCES \"public\".\"entities\"(id)
            );",
            space = space,
            entity_id = entity_id
        );

        //let message = format!(
        //"Creating table for entity {} for space {}",
        //entity_id, space
        //);
        sender.send(table_create_statement.clone()).await.unwrap();

        db.execute(Statement::from_string(
            DbBackend::Postgres,
            table_create_statement,
        ))
        .await?;

        drop(sender);
        Ok(())
    }

    /// This function adds a relation to an entity's table
    /// Because of the way postgraphile works, we need to add the column, with a reference, to the attribute's table
    /// prefixed with "parent_", and a reference to the entity's table, which is the entity-id
    pub async fn add_relation(
        db: &DatabaseConnection,
        parent_entity: &String,
        child_entity: &String,
        space: &String,
        sender: &Sender<String>,
    ) -> Result<(), DbErr> {
        let message = format!(
            "Adding relation from child {} to parent {} for space {}",
            child_entity, parent_entity, space
        );
        sender.send(message).await.unwrap();

        let column_add_statement = format!(
            "ALTER TABLE \"{space}\".\"{child_entity}\" ADD COLUMN IF NOT EXISTS \"parent_{parent_entity}\" TEXT REFERENCES \"{space}\".\"{parent_entity}\"(id);",
            space = space,
            child_entity = child_entity,
            parent_entity = parent_entity
        );

        db.execute(Statement::from_string(
            DbBackend::Postgres,
            column_add_statement,
        ))
        .await?;

        drop(sender);
        Ok(())
    }

    /// This function handles a type being added to an entity
    /// It populates the type's table with the entity's id
    pub async fn add_type(
        db: &DatabaseConnection,
        entity_id: &String,
        type_id: &String,
        sender: &Sender<String>,
    ) -> Result<(), DbErr> {
        let message = format!("Checking if type {} exists", type_id);
        sender.send(message).await.unwrap();
        // grab the space the type is defined in
        let space = Entity::find_by_id(type_id.clone())
            .one(db)
            .await?
            .unwrap()
            .defined_in
            .unwrap();

        let message = format!("Adding type {} to entity {}", type_id, entity_id);
        sender.send(message).await.unwrap();
        let type_insert_statement = format!(
            "INSERT INTO \"{space}\".\"{type_id}\" (\"id\", \"entity_id\") VALUES ('{entity_id}', '{entity_id}');",
            space = space,
            type_id = type_id,
            entity_id = entity_id
        );

        db.execute(Statement::from_string(
            DbBackend::Postgres,
            type_insert_statement,
        ))
        .await?;

        drop(sender);
        Ok(())
    }

    pub async fn create(
        db: &DatabaseConnection,
        entity_id: String,
        space: String,
        sender: &Sender<String>,
    ) -> Result<(), DbErr> {
        let message = format!("Checking if entity {} exists", entity_id);
        sender.send(message).await.unwrap();
        let entity = Entity::find_by_id(entity_id.clone()).one(db).await?;

        if let Some(_) = entity {
            drop(sender);
            return Ok(());
        }

        let message = format!("Creating entity {}", entity_id);

        let entity = ActiveModel {
            id: ActiveValue::Set(entity_id),
            defined_in: ActiveValue::Set(Some(space)),
            ..Default::default()
        };

        sender.send(message).await.unwrap();
        Entity::insert(entity)
            .on_conflict(OnConflict::column(Column::Id).do_nothing().to_owned())
            .do_nothing()
            .exec(db)
            .await?;

        drop(sender);
        Ok(())
    }

    pub async fn upsert_name(
        db: &DatabaseConnection,
        entity_id: String,
        name: String,
        sender: &Sender<String>,
    ) -> Result<(), DbErr> {
        let message = format!("Upserting name {} for entity {}", name, entity_id);
        sender.send(message).await.unwrap();
        drop(sender);

        let entity = ActiveModel {
            id: ActiveValue::Set(entity_id),
            name: ActiveValue::Set(Some(name)),
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

        Ok(())
    }

    pub async fn upsert_is_type(
        db: &DatabaseConnection,
        entity_id: String,
        is_type: bool,
        sender: &Sender<String>,
    ) -> Result<(), DbErr> {
        let message = format!("Upserting is_type {} for entity {}", is_type, entity_id);
        sender.send(message).await.unwrap();
        drop(sender);

        let entity = ActiveModel {
            id: ActiveValue::Set(entity_id),
            is_type: ActiveValue::Set(Some(is_type)),
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
        db: &DatabaseConnection,
        entity_id: String,
        attribute_of_id: String,
        sender: &Sender<String>,
    ) -> Result<(), DbErr> {
        let message = format!(
            "Adding attribute {} to entity {}",
            attribute_of_id, entity_id
        );
        sender.send(message).await.unwrap();
        drop(sender);
        let entity = entity_attributes::ActiveModel {
            id: ActiveValue::Set(format!("{}-{}", entity_id, attribute_of_id)),
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
    use tokio::sync::mpsc::Sender;

    pub async fn store(
        db: &DatabaseConnection,
        cursor_string: String,
        block_number: u64,
        sender: &Sender<String>,
    ) -> Result<(), DbErr> {
        let message = format!("Checking if cursor exists");
        sender.send(message).await.unwrap();
        let cursor = cursors::Entity::find_by_id(0).one(db).await?;
        if let Some(_) = cursor {
            let message = format!("Updating cursor");
            sender.send(message).await.unwrap();
            cursors::Entity::update(cursors::ActiveModel {
                id: ActiveValue::Set(0),
                cursor: ActiveValue::Set(cursor_string),
                block_number: ActiveValue::Set(block_number.to_string()),
            })
            .exec(db)
            .await?;
        } else {
            let message = format!("Creating cursor");
            sender.send(message).await.unwrap();
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

        drop(sender);
        Ok(())
    }

    pub async fn get(
        db: &DatabaseConnection,
        sender: &Sender<String>,
    ) -> Result<Option<String>, DbErr> {
        let message = format!("Getting cursor");
        sender.send(message).await.unwrap();
        let cursor = cursors::Entity::find_by_id(0).one(db).await?;

        drop(sender);
        if let Some(cursor) = cursor {
            return Ok(Some(cursor.cursor));
        } else {
            Ok(None)
        }
    }
}

pub mod accounts {
    use entity::accounts::*;
    use migration::{DbErr, OnConflict};
    use sea_orm::{ActiveValue, DatabaseConnection, EntityTrait, InsertResult};
    use tokio::sync::mpsc::Sender;

    pub async fn create(
        db: &DatabaseConnection,
        address: String,
        sender: &Sender<String>,
    ) -> Result<(), DbErr> {
        let account = Entity::find_by_id(address.clone()).one(db).await?;

        if let None = account {
            let message = format!("Creating account {}", address);
            sender.send(message).await.unwrap();
            drop(sender);

            let account = ActiveModel {
                id: ActiveValue::Set(address.clone()),
                ..Default::default()
            };

            Entity::insert(account)
                .on_conflict(OnConflict::column(Column::Id).do_nothing().to_owned())
                .do_nothing()
                .exec(db)
                .await?;
        } else {
            let message = format!("Account {} already existst", address);
            sender.send(message).await.unwrap();
            drop(sender);
        }

        Ok(())
    }
}

pub mod triples {
    use entity::triples::*;
    use migration::{DbErr, OnConflict};
    use sea_orm::{ActiveValue, DatabaseConnection, EntityTrait, InsertResult, Set};
    use tokio::sync::mpsc::Sender;
    use uuid::Uuid;

    use crate::triples::ValueType;

    pub async fn create(
        db: &DatabaseConnection,
        entity_id: String,
        attribute_id: String,
        value: ValueType,
        space: String,
        author: String,
        sender: &Sender<String>,
    ) -> Result<(), DbErr> {
        // create the entity and attribute if they don't exist
        super::entities::create(db, entity_id.clone(), space.clone(), sender).await?;

        super::entities::create(db, attribute_id.clone(), space.clone(), sender).await?;

        let id = format!("{}", Uuid::new_v4());

        if let Some(_) = Entity::find_by_id(id.clone()).one(db).await? {
            return Ok(());
        } else {
            let message = format!(
                "Creating triple {} {} {}",
                entity_id,
                attribute_id,
                value.id(),
            );
            sender.send(message).await.unwrap();
            drop(sender);

            let mut triple = ActiveModel {
                id: Set(id.clone()),
                entity_id: Set(entity_id),
                attribute_id: Set(attribute_id),
                value_id: Set(value.id().to_string()),
                value_type: Set(value.value_type().to_string()),
                defined_in: Set(space),
                is_protected: Set(false),
                ..Default::default()
            };

            match value {
                ValueType::Number { id, value } => {
                    triple.number_value = ActiveValue::Set(Some(value.to_string()));
                }
                ValueType::String { id, value } => {
                    triple.string_value = ActiveValue::Set(Some(value));
                }
                ValueType::Image { id, value } => {
                    triple.string_value = ActiveValue::Set(Some(value));
                }
                ValueType::Entity { id } => {
                    triple.entity_value = ActiveValue::Set(Some(id));
                }
                ValueType::Date { id, value } => {
                    triple.string_value = ActiveValue::Set(Some(value));
                }
                ValueType::Url { id, value } => {
                    triple.string_value = ActiveValue::Set(Some(value));
                }
            }

            let res = Entity::insert(triple)
                .on_conflict(OnConflict::column(Column::Id).do_nothing().to_owned())
                .do_nothing()
                .exec(db)
                .await?;

            Ok(())
        }
    }

    async fn delete(
        db: &DatabaseConnection,
        entity_id: String,
        attribute_id: String,
        value: ValueType,
        space: String,
        author: String,
        sender: &Sender<String>,
    ) -> Result<(), DbErr> {
        todo!("Need to handle delete triples");
        // let triple = ActiveModel {
        //     id: ActiveValue::Set(format!("{}-{}-{}", entity_id, attribute_id, value.id())),
        //     entity_id: ActiveValue::Set(entity_id),
        //     attribute_id: ActiveValue::Set(attribute_id),
        //     value: ActiveValue::Set(value.value().to_string()),
        //     value_id: ActiveValue::Set(value.id().to_string()),
        //     value_type: ActiveValue::Set(value.value_type().to_string()),
        //     defined_in: ActiveValue::Set(space),
        // };

        // Entity::delete(triple).exec(db).await?;

        // Ok(())
    }
}

/// This module handles the creation of actions in the database
pub mod actions {
    use entity::actions::*;
    use migration::{DbErr, OnConflict};
    use sea_orm::EntityTrait;
    use sea_orm::Set;
    use tokio::sync::mpsc::Sender;

    use crate::triples::{ActionTriple, ValueType};

    pub async fn create(
        db: &sea_orm::DatabaseConnection,
        action_triple: &ActionTriple,
        sender: &Sender<String>,
    ) -> Result<(), DbErr> {
        let id = format!("{}", uuid::Uuid::new_v4());
        let message = format!("Creating action {}", id);
        sender.send(message).await;
        drop(sender);

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
