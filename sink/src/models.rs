//! A bunch of modules containing helpers for working with the database
//! These are going to just hide the implementation details of the database
//! and provide a nice interface for the rest of the application to use

pub mod spaces {
    use entity::spaces::*;
    use migration::{DbErr, OnConflict};
    use sea_orm::{ActiveValue, DatabaseConnection, EntityTrait};
    use tokio::sync::mpsc::Sender;

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
    use migration::{DbErr, OnConflict};
    use sea_orm::{ActiveModelTrait, ActiveValue, DatabaseConnection, EntityTrait};
    use tokio::sync::mpsc::Sender;

    pub async fn create(
        db: &DatabaseConnection,
        entity_id: String,
        space: String,
        sender: &Sender<String>,
    ) -> Result<(), DbErr> {
        let entity = Entity::find_by_id(entity_id.clone()).one(db).await?;

        if let Some(_) = entity {
            let message = format!("Entity {} already exists", entity_id);
            sender.send(message).await.unwrap();
            drop(sender);
            return Ok(());
        }

        let message = format!("Creating entity {}", entity_id);
        sender.send(message).await.unwrap();
        drop(sender);

        let entity = ActiveModel {
            id: ActiveValue::Set(entity_id),
            defined_in: ActiveValue::Set(Some(space)),
            ..Default::default()
        };

        Entity::insert(entity)
            .on_conflict(
                OnConflict::column(Column::Id)
                    .update_column(Column::DefinedIn)
                    .to_owned(),
            )
            .do_nothing()
            .exec(db)
            .await;

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

    pub async fn store(db: &DatabaseConnection, cursor_string: String) -> Result<(), DbErr> {
        let cursor = cursors::Entity::find_by_id(0).one(db).await?;
        if let Some(_) = cursor {
            cursors::Entity::update(cursors::ActiveModel {
                id: ActiveValue::Set(0),
                cursor: ActiveValue::Set(cursor_string),
            })
            .exec(db)
            .await?;
        } else {
            let cursor = cursors::ActiveModel {
                id: ActiveValue::Set(0),
                cursor: ActiveValue::Set(cursor_string),
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
        sender.send(message).await.unwrap();
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
