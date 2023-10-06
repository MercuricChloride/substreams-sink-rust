//! A bunch of modules containing helpers for working with the database
//! These are going to just hide the implementation details of the database
//! and provide a nice interface for the rest of the application to use

pub fn table_comment_string(space: &String, entity_id: &String, entity_name: &String) -> String {
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
                        entity_name = entity_name
                    )
}

pub mod spaces {
    use entity::spaces::*;
    use migration::{ConnectionTrait, DbErr, OnConflict};
    use sea_orm::{ActiveValue, DatabaseConnection, DbBackend, EntityTrait, Statement};

    pub async fn create_schema(db: &DatabaseConnection, schema_name: &String) -> Result<(), DbErr> {
        if cfg!(not(feature = "experimental_queries")) {
            return Ok(());
        } else {
            //let message = format!("Creating schema {schema_name}");
            //sender.send(message).await.unwrap();

            let schema_query = format!("CREATE SCHEMA IF NOT EXISTS \"{schema_name}\";");

            db.execute(Statement::from_string(DbBackend::Postgres, schema_query))
                .await?;

            //let message = format!("Creating table for {schema_name}");
            //sender.send(message).await.unwrap();
            //let table_query = format!("CREATE TABLE IF NOT EXISTS\"{schema_name}\".\"FOO\"(id text);");

            // db.execute(Statement::from_string(DbBackend::Postgres, table_query))
            //     .await?;

            //let message = format!("Inserting test data for table {schema_name}");
            //sender.send(message).await.unwrap();
            //let insert = format!("INSERT INTO \"{schema_name}\".\"FOO\" VALUES ('bar');");
            // db.execute(Statement::from_string(DbBackend::Postgres, insert))
            //     .await?;

            //drop(sender);
            Ok(())
        }
    }

    pub async fn create(
        db: &DatabaseConnection,
        space_id: String,
        address: String,
        created_in_space: String,
    ) -> Result<(), DbErr> {
        // make the entity for the space if it doesn't exist
        super::entities::create(db, space_id.clone(), created_in_space).await?;

        let space = ActiveModel {
            id: ActiveValue::Set(space_id),
            address: ActiveValue::Set(Some(address)),
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
}

pub mod entities {
    use entity::{entities::*, entity_attributes, entity_types};
    use migration::{ConnectionTrait, DbErr, OnConflict};
    use sea_orm::{
        ActiveModelTrait, ActiveValue, ColumnTrait, DatabaseConnection, DbBackend, EntityTrait,
        QueryFilter, Statement,
    };
    use tokio::sync::mpsc::Sender;

    use crate::retry::Retry;

    use super::table_comment_string;

    pub async fn create_table(
        db: &DatabaseConnection,
        entity_id: &String,
        space: &String,
    ) -> Result<(), DbErr> {
        if cfg!(not(feature = "experimental_queries")) {
            return Ok(());
        } else {
            println!(
                "Creating table for entity {} for space {}",
                entity_id, space
            );

            let table_create_statement = format!(
                "CREATE TABLE IF NOT EXISTS \"{space}\".\"{entity_id}\" (
                id TEXT PRIMARY KEY,
                entity_id TEXT NOT NULL REFERENCES \"public\".\"entities\"(id)
            );",
                space = space,
                entity_id = entity_id
            );

            let table_disable_statement = format!(
                "ALTER TABLE \"{space}\".\"{entity_id}\" DISABLE TRIGGER ALL;",
                space = space,
                entity_id = entity_id
            );
            let mut table_create_result = None;

            let mut retry_count = 0;

            while let None = table_create_result {
                let result = db
                    .execute(Statement::from_string(
                        DbBackend::Postgres,
                        table_create_statement.clone(),
                    ))
                    .await;
                db.execute(Statement::from_string(
                    DbBackend::Postgres,
                    table_disable_statement.clone(),
                ))
                .await?;
                if let Ok(result) = result {
                    table_create_result = Some(result);
                } else if retry_count == 3 {
                    println!(
                        "Couldn't create table for entity {} for space {}. \n\n {:?}",
                        entity_id, space, result
                    );
                } else {
                    retry_count += 1;
                }
            }

            // If the entity has a name, we need to add a comment to the table
            if let Some(entity) = Entity::find_by_id(entity_id.clone()).one(db).await? {
                if let Some(entity_name) = entity.name {
                    let table_comment = table_comment_string(space, entity_id, &entity_name);

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
                        println!("Comment: {}", table_comment);
                    }
                }
            }

            //drop(sender);
            Ok(())
        }
    }

    /// This function adds a relation to an entity's table
    /// Because of the way postgraphile works, we need to add the column, with a reference, to the attribute's table
    /// prefixed with "parent_", and a reference to the entity's table, which is the entity-id
    pub async fn add_relation(
        db: &DatabaseConnection,
        parent_entity: &String,
        child_entity: &String,
        space: &String,
    ) -> Result<(), DbErr> {
        // let message = format!(
        //     "Adding relation from child {} to parent {} for space {}",
        //     child_entity, parent_entity, space
        // );
        //sender.send(message).await.unwrap();

        if cfg!(not(feature = "experimental_queries")) {
            return Ok(());
        } else {
            println!(
                "Adding relation from child {} to parent {} for space {}",
                child_entity, parent_entity, space
            );

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

            //drop(sender);
            Ok(())
        }
    }

    /// This function handles a type being added to an entity
    /// It populates the type's table with the entity's id
    pub async fn add_type(
        db: &DatabaseConnection,
        entity_id: &String,
        type_id: &String,
        space: &String,
    ) -> Result<(), DbErr> {
        // create the entity and type if they don't exist
        create(db, entity_id.clone(), space.clone()).await?;

        create(db, type_id.clone(), space.clone()).await?;

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

        if cfg!(not(feature = "experimental_queries")) {
            return Ok(());
        } else {
            //let message = format!("Checking if type {} exists", type_id);
            //sender.send(message).await.unwrap();
            // grab the space the type is defined in
            let space = Entity::find_by_id(type_id.clone())
                .one(db)
                .await?
                .unwrap()
                .defined_in
                .unwrap();

            //let message = format!("Adding type {} to entity {}", type_id, entity_id);
            //sender.send(message).await.unwrap();
            println!(
                "Adding type {} to entity {} for space {}",
                type_id, entity_id, space
            );

            let type_insert_statement = format!(
            "INSERT INTO \"{space}\".\"{type_id}\" (\"id\", \"entity_id\") VALUES ('{entity_id}', '{entity_id}') ON CONFLICT (id) DO NOTHING;",
            space = space,
            type_id = type_id,
            entity_id = entity_id
        );

            db.execute(Statement::from_string(
                DbBackend::Postgres,
                type_insert_statement,
            ))
            .await?;

            //drop(sender);
            Ok(())
        }
    }

    pub async fn create(
        db: &DatabaseConnection,
        entity_id: String,
        space: String,
    ) -> Result<(), DbErr> {
        let message = format!("Creating entity {}", entity_id);

        let entity = ActiveModel {
            id: ActiveValue::Set(entity_id),
            defined_in: ActiveValue::Set(Some(space)),
            ..Default::default()
        };

        //sender.send(message).await.unwrap();
        Entity::insert(entity)
            .on_conflict(OnConflict::column(Column::Id).do_nothing().to_owned())
            .do_nothing()
            .exec(db)
            .await?;

        //drop(sender);
        Ok(())
    }

    pub async fn upsert_name(
        db: &DatabaseConnection,
        entity_id: String,
        name: String,
        space: String,
    ) -> Result<(), DbErr> {
        let entity = ActiveModel {
            id: ActiveValue::Set(entity_id.clone()),
            name: ActiveValue::Set(Some(name.clone())),
            defined_in: ActiveValue::Set(Some(space.clone())),
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

        // if the entity is a type, we need to add a comment updating the name of the table
        if cfg!(not(feature = "experimental_queries")) {
            return Ok(());
        } else {
            if let Some(entity) = Entity::find_by_id(entity_id.clone()).one(db).await? {
                if let (Some(entity_name), Some(is_type)) = (entity.name, entity.is_type) {
                    if !is_type {
                        return Ok(());
                    }

                    let table_comment = table_comment_string(&space, &entity_id, &entity_name);

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
                        println!("Comment: {}", table_comment);
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn upsert_description(
        db: &DatabaseConnection,
        entity_id: String,
        description: String,
        space: String,
    ) -> Result<(), DbErr> {
        let message = format!(
            "Upserting description {} for entity {}",
            description, entity_id
        );

        let entity = ActiveModel {
            id: ActiveValue::Set(entity_id),
            description: ActiveValue::Set(Some(description)),
            defined_in: ActiveValue::Set(Some(space)),
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

        Ok(())
    }

    pub async fn upsert_is_type(
        db: &DatabaseConnection,
        entity_id: String,
        is_type: bool,
    ) -> Result<(), DbErr> {
        //let message = format!("Upserting is_type {} for entity {}", is_type, entity_id);
        let entity = Entity::find_by_id(entity_id.to_string()).one(db).await?;

        if let Some(entity) = entity {
            if let Some(entity_is_type) = entity.is_type {
                // if the entities type is the same as we want to set return early.
                if is_type == entity_is_type {
                    return Ok(());
                }
            }
        }

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
    ) -> Result<(), DbErr> {
        let message = format!(
            "Adding attribute {} to entity {}",
            attribute_of_id, entity_id
        );
        //sender.send(message).await.unwrap();
        //drop(sender);
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
    use tokio::sync::mpsc::Sender;

    pub async fn store(
        db: &DatabaseConnection,
        cursor_string: String,
        block_number: u64,
    ) -> Result<(), DbErr> {
        let message = format!("Checking if cursor exists");
        //sender.send(message).await.unwrap();
        let cursor = cursors::Entity::find_by_id(0).one(db).await?;
        if let Some(_) = cursor {
            let message = format!("Updating cursor");
            //sender.send(message).await.unwrap();
            cursors::Entity::update(cursors::ActiveModel {
                id: ActiveValue::Set(0),
                cursor: ActiveValue::Set(cursor_string),
                block_number: ActiveValue::Set(block_number.to_string()),
            })
            .exec(db)
            .await?;
        } else {
            let message = format!("Creating cursor");
            //sender.send(message).await.unwrap();
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

        //drop(sender);
        Ok(())
    }

    pub async fn get(db: &DatabaseConnection) -> Result<Option<String>, DbErr> {
        let message = format!("Getting cursor");
        //sender.send(message).await.unwrap();
        let cursor = cursors::Entity::find_by_id(0).one(db).await?;

        //drop(sender);
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

    pub async fn create(db: &DatabaseConnection, address: String) -> Result<(), DbErr> {
        let account = Entity::find_by_id(address.clone()).one(db).await?;

        if let None = account {
            let account = ActiveModel {
                id: ActiveValue::Set(address.clone()),
                ..Default::default()
            };

            Entity::insert(account)
                .on_conflict(OnConflict::column(Column::Id).do_nothing().to_owned())
                .do_nothing()
                .exec(db)
                .await?;

            let message = format!("Creating account {}", address);
            //sender.send(message).await.unwrap();
            //drop(sender);
        } else {
            let message = format!("Account {} already existst", address);
            //sender.send(message).await.unwrap();
            //drop(sender);
        }

        Ok(())
    }
}

pub mod triples {
    use anyhow::Error;
    use entity::triples::*;
    use migration::{DbErr, OnConflict};
    use sea_orm::{
        ActiveModelTrait, ActiveValue, ColumnTrait, DatabaseConnection, EntityTrait, InsertResult,
        QueryFilter, Set,
    };
    use tokio::sync::mpsc::Sender;
    use uuid::Uuid;

    use crate::{
        constants::{Attributes, Entities, ROOT_SPACE_ADDRESS},
        triples::{ActionTriple, ValueType},
    };

    pub async fn create(
        db: &DatabaseConnection,
        entity_id: String,
        attribute_id: String,
        value: ValueType,
        space: String,
        author: String,
    ) -> Result<(), DbErr> {
        // create the entity and attribute and value if they don't exist
        super::entities::create(db, entity_id.clone(), space.clone()).await?;

        super::entities::create(db, attribute_id.clone(), space.clone()).await?;

        //super::entities::create(db, value.id().to_string(), space.clone()).await?;

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
            //sender.send(message).await.unwrap();
            //drop(sender);

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

            Entity::insert(triple)
                .on_conflict(OnConflict::column(Column::Id).do_nothing().to_owned())
                .do_nothing()
                .exec(db)
                .await?;

            Ok(())
        }
    }

    pub async fn delete(
        db: &DatabaseConnection,
        entity_id: String,
        attribute_id: String,
        value: ValueType,
        space: String,
        author: String,
    ) -> Result<(), DbErr> {
        let triple = Entity::find()
            .filter(Column::EntityId.contains(entity_id.clone()))
            .filter(Column::AttributeId.contains(attribute_id.clone()))
            .filter(Column::ValueId.contains(value.id().to_string()))
            .one(db)
            .await?;

        if let Some(triple) = triple {
            let mut triple: ActiveModel = triple.into();
            triple.deleted = Set(true);
            triple.save(db).await?;
            Ok(())
        } else {
            Ok(())
        }

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

    pub async fn bootstrap(db: &DatabaseConnection) -> Result<(), Error> {
        use strum::IntoEnumIterator;
        let author = "BOOTSTRAP";

        let name_attribute = Attributes::Name.id();
        let type_attribute = Attributes::Type.id();
        let attribute_entity = Entities::Attribute.id();
        let value_type_attribute = Attributes::ValueType.id();

        let mut action_triples = Vec::new();

        for attribute in Attributes::iter() {
            // bootstrap the name of the attribute
            let entity_id = attribute.id();
            let value = attribute.name();
            let value = ValueType::String {
                id: entity_id.to_string(),
                value: value.to_string(),
            };
            let space = ROOT_SPACE_ADDRESS.to_string();
            let action = ActionTriple::CreateTriple {
                entity_id: entity_id.into(),
                attribute_id: name_attribute.into(),
                value: value.into(),
                space: space.clone(),
                author: author.to_string(),
            };
            action_triples.push(action);

            // bootstrap the attribute to have a type of attribute
            let value = ValueType::Entity {
                id: attribute_entity.to_string(),
            };
            let action = ActionTriple::CreateTriple {
                entity_id: entity_id.into(),
                attribute_id: type_attribute.into(),
                value: value.into(),
                space: space.clone(),
                author: author.to_string(),
            };
            action_triples.push(action);

            // bootstrap the value_type of the attribute if it has one
            if let Some(value_type) = attribute.value_type() {
                let value = ValueType::Entity {
                    id: value_type.id().to_string(),
                };
                let action = ActionTriple::CreateTriple {
                    entity_id: entity_id.into(),
                    attribute_id: value_type_attribute.into(),
                    value: value.into(),
                    space: space.clone(),
                    author: author.to_string(),
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
                id: entity_id.to_string(),
                value: value.to_string(),
            };

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
                id: Entities::SchemaType.id().to_string(),
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
                    id: attribute.id().to_string(),
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

        let mut sink_actions = Vec::new();
        for action_triple in action_triples {
            let (sink_action, option_action) = action_triple.get_sink_actions();
            sink_actions.push(sink_action);

            if let Some(option_action) = option_action {
                sink_actions.push(option_action);
            }
        }

        sink_actions.sort_by(|a, b| a.action_priority().cmp(&b.action_priority()));
        println!("{:#?}", sink_actions);

        for action in sink_actions {
            action.execute(db).await?;
        }

        Ok(())
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
    ) -> Result<(), DbErr> {
        let id = format!("{}", uuid::Uuid::new_v4());
        let message = format!("Creating action {}", id);
        //sender.send(message).await;
        //drop(sender);

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
