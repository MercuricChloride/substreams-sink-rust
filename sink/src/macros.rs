#[macro_export]
macro_rules! sql_exec {
    ($db: ident, $query: expr) => {
        $db.execute(Statement::from_string(DbBackend::Postgres, $query))
    };
}

#[macro_export]
macro_rules! query_all {
    ($db: ident, $query: expr) => {
        $db.query_all(Statement::from_string(DbBackend::Postgres, $query))
    };
}

#[macro_export]
macro_rules! query_one {
    ($db: ident, $query: expr) => {
        $db.query_one(Statement::from_string(DbBackend::Postgres, $query))
    };
}

#[macro_export]
macro_rules! create_triple {
    ($entity: expr, $attribute: expr, $value: expr) => {
        println!("{}{}", $entity, $attribute);
    };
}

#[macro_export]
macro_rules! entity_insert {
    ($entity_struct: path, $entity: ident, $conflict_column: path, $( $update_column: path ),*) => {
        <$entity_struct>::insert($entity)
            .on_conflict(
                OnConflict::column($conflict_column)
                    $( .update_column($update_column) )*
                    .to_owned(),
            )
    };

    ($entity_struct: path, $entity: ident, $conflict_column: path, $( $update_column: path ),*) => {
        <$entity_struct>::insert($entity)
            .on_conflict(
                OnConflict::column($conflict_column)
                    $( .update_column($update_column) )*
                    .to_owned(),
            )
    };

    ($entity: ident, $( $update_column: ident ),*) => {
        Entity::insert($entity)
            .on_conflict(
                OnConflict::column(Column::Id)
                    $( .update_column(Column::$update_column) )*
                    .to_owned(),
            )
    };

    ($entity: ident) => {
        Entity::insert($entity)
            .on_conflict(
                OnConflict::column(Column::Id)
                    .do_nothing()
                    .to_owned(),
            )
            .do_nothing()
    };
}

#[macro_export]
macro_rules! find_entity {
    ($db: ident, $entity_id: expr) => {
        Entity::find_by_id($entity_id)
            .one($db)
            .await?
            .ok_or(Error::msg(format!("Entity {} doesn't exist", $entity_id)))?
    };

    ($db: ident, $entity_id: expr, $message: expr) => {
        Entity::find_by_id($entity_id)
            .one($db)
            .await?
            .ok_or(Error::msg(format!($message)))?
    };
}
