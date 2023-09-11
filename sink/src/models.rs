//! A bunch of modules containing helpers for working with the database
//! These are going to just hide the implementation details of the database
//! and provide a nice interface for the rest of the application to use

pub mod spaces {
    use entity::spaces::*;
    use migration::DbErr;
    use sea_orm::{ActiveValue, DatabaseConnection, EntityTrait};

    pub async fn create(
        db: &DatabaseConnection,
        space_id: String,
        address: String,
    ) -> Result<(), DbErr> {
        let space = ActiveModel {
            id: ActiveValue::Set(space_id.to_owned()),
            address: ActiveValue::Set(Some(address)),
        };

        Entity::insert(space).exec(db).await?;

        Ok(())
    }
}

pub mod entities {
    use entity::{entities::*, entity_attributes};
    use migration::{DbErr, OnConflict};
    use sea_orm::{ActiveModelTrait, ActiveValue, DatabaseConnection, EntityTrait};

    pub async fn create(
        db: &DatabaseConnection,
        entity_id: String,
        space: String,
    ) -> Result<(), DbErr> {
        let entity = ActiveModel {
            id: ActiveValue::Set(entity_id.to_owned()),
            defined_in: ActiveValue::Set(Some(space)),
            ..Default::default()
        };

        Entity::insert(entity).exec(db).await?;

        Ok(())
    }

    pub async fn upsert_name(
        db: &DatabaseConnection,
        entity_id: String,
        name: String,
    ) -> Result<(), DbErr> {
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
    ) -> Result<(), DbErr> {
        let entity = Entity::find_by_id(entity_id.to_owned()).one(db).await?;

        let mut entity = match entity {
            Some(entity) => entity.into(),
            None => ActiveModel {
                id: ActiveValue::Set(entity_id.to_owned()),
                ..Default::default()
            },
        };

        entity.is_type = ActiveValue::Set(Some(is_type));

        entity.update(db).await?;

        Ok(())
    }

    /// !!!NOTE!!! attribute_of_id is the id of the entity we are adding an attribute to
    pub async fn add_attribute(
        db: &DatabaseConnection,
        entity_id: String,
        attribute_of_id: String,
    ) -> Result<(), DbErr> {
        let entity = entity_attributes::ActiveModel {
            id: ActiveValue::Set(format!("{}-{}", entity_id, attribute_of_id)),
            entity_id: ActiveValue::Set(entity_id),
            attribute_of: ActiveValue::Set(attribute_of_id),
        };

        entity_attributes::Entity::insert(entity).exec(db).await?;

        Ok(())
    }
}

/// A helper module for storing and retrieving the cursor from the db
pub mod cursor {
    use entity::cursors;
    use migration::{DbErr, OnConflict};
    use sea_orm::{ActiveValue, DatabaseConnection, EntityTrait};

    pub async fn store(db: &DatabaseConnection, cursor_string: String) -> Result<(), DbErr> {
        let cursor = cursors::ActiveModel {
            id: ActiveValue::Set(0),
            cursor: ActiveValue::Set(cursor_string),
        };

        cursors::Entity::insert(cursor)
            .on_conflict(
                OnConflict::column(cursors::Column::Id)
                    .update_column(cursors::Column::Cursor)
                    .to_owned(),
            )
            .exec(db)
            .await?;

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

pub mod triples {
    use entity::triples::*;
    use migration::{DbErr, OnConflict};
    use sea_orm::{ActiveValue, DatabaseConnection, EntityTrait};

    use crate::triples::ValueType;

    pub async fn create(
        db: &DatabaseConnection,
        entity_id: String,
        attribute_id: String,
        value: ValueType,
    ) -> Result<(), DbErr> {
        let triple = ActiveModel {
            id: ActiveValue::Set(format!("{}-{}-{}", entity_id, attribute_id, value.id())),
            entity_id: ActiveValue::Set(entity_id),
            attribute_id: ActiveValue::Set(attribute_id),
            value: ActiveValue::Set(value.value()),
            value_id: ActiveValue::Set(value.id().to_string()),
            value_type: ActiveValue::Set(value.value_type().to_string()),
        };

        Entity::insert(triple)
            .on_conflict(
                OnConflict::column(Column::Id)
                    .update_column(Column::Value)
                    .to_owned(),
            )
            .exec(db)
            .await?;

        Ok(())
    }

    pub async fn delete(
        db: &DatabaseConnection,
        entity_id: String,
        attribute_id: String,
        value: ValueType,
    ) -> Result<(), DbErr> {
        let triple = ActiveModel {
            id: ActiveValue::Set(format!("{}-{}-{}", entity_id, attribute_id, value.id())),
            entity_id: ActiveValue::Set(entity_id),
            attribute_id: ActiveValue::Set(attribute_id),
            value: ActiveValue::Set(value.value().to_string()),
            value_id: ActiveValue::Set(value.id().to_string()),
            value_type: ActiveValue::Set(value.value_type().to_string()),
        };

        Entity::delete(triple).exec(db).await?;

        Ok(())
    }
}
