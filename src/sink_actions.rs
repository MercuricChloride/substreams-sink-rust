use std::process;

use tokio_postgres::Client;

use crate::{persist::Persist, triples::ValueType};

#[derive(Debug)]
/// This enum represents different actions that the sink should handle. Actions being specific changes to the graph.
pub enum SinkAction {
    /// This action denotes a newly created space. The string is the address of the space.
    /// We care about this in the sink because when a new space is created, we need to deploy
    /// a new subgraph for that space.
    SpaceCreated { entity_id: String, space: String },

    /// This action denotes a newly created type. The string is the name of the type.
    /// We care about this in the sink because when a new type is created, we need to deploy
    /// a new subgraph for that type.
    ///
    /// When a type is created in geo, it looks like this:
    ///
    /// `(Entity, "types", TypeEntity)`
    TypeCreated {
        /// The entity ID of the type that was created.
        entity_id: String,
        /// The address of the space that this type was created in.
        space: String,
    },

    /// We also care about an attribute being added to an entity, we need the entity ID and the space it was made in
    /// When an attribute is added to a type in geo, it looks like this:
    /// `(EntityID, "attribute", AttributeEntity)`
    ///
    /// or a practical example explaining how the goal type has a subgoal attribute
    ///
    /// `(Goal, "attribute", Subgoal)`
    AttributeAdded {
        /// The address of the space that this attribute was created in.
        space: String,
        /// The ID of the entity that this attribute was added to.
        entity_id: String,
        /// The ID of the attribute entity
        attribute_id: String,
        /// The value of the triple
        value: ValueType,
    },

    /// We care about a name being added to an entity because we need this when adding attributes to a type in the graph.
    NameAdded {
        space: String,
        entity_id: String,
        name: String,
    },

    /// We care about a ValueType being added to an entity because we need this when adding attributes to a type in the graph.
    ValueTypeAdded {
        space: String,
        entity_id: String,
        attribute_id: String,
        value_type: ValueType,
    },

    /// Spaces can have subspaces, and we need to know when a subspace is added to a space so we can deploy a new subgraph for that space.
    SubspaceAdded {
        parent_space: String,
        child_space: String,
    },

    /// Spaces can also remove subspaces, and we need to know when a subspace is removed from a space
    SubspaceRemoved {
        parent_space: String,
        child_space: String,
    },
}

impl SinkAction {
    pub fn handle_sink_action(&self, persist: &mut Persist) -> Result<(), String> {
        match &self {
            SinkAction::SpaceCreated { entity_id, space } => {
                let query = format!(
                    "INSERT INTO spaces (id, space) VALUES ('{}', '{}')",
                    entity_id, space
                );

                persist.tasks.push(query);

                // push the space to the persist
                //persist.add_space(entity_id.to_string(), space.to_string());

                //let command = format!(
                //"(cd external/subgraph && ./deploy.sh mercuricchloride/geo-test {})",
                //space
                //);

                //println!("Deploying subgraph for space: {}", space);
                //process::Command::new(command);
            }

            SinkAction::TypeCreated { entity_id, space } => {
                let query = format!(
                    "INSERT INTO entity_types (id, space) VALUES ('{}', '{}')",
                    entity_id, space
                );

                persist.tasks.push(query);

                let table_creation = format!(
                    "
DO $$
DECLARE
  new_table_name TEXT;
BEGIN
  -- Generate the new table name based on the existing table name
  SELECT name INTO new_table_name FROM entity_names WHERE id = '{}';

  EXECUTE 'CREATE TABLE ' || new_table_name || ' (id TEXT)';
END $$;
",
                    entity_id
                );

                persist.retry_tasks.push(table_creation);

                //persist.add_type(entity_id, space);
                //println!("Type added: {:?} in space {:?}", entity_id, space);
            }

            SinkAction::AttributeAdded {
                space,
                entity_id,
                attribute_id: _,
                value,
            } => match value {
                // TODO Clean this up
                ValueType::Entity { id } => {
                    let query = format!(
                        "INSERT INTO entity_attributes (id, belongs_to) VALUES ('{}', '{}')",
                        id, entity_id
                    );

                    persist.tasks.push(query);

                    let column_creation = format!(
                        "
DO $$
DECLARE
    new_column TEXT;
BEGIN
    -- Generate the new column name based on the existing column name
    SELECT name INTO new_column FROM entity_names WHERE id = '{}';

    -- Create a new column on the table
    EXECUTE 'ALTER TABLE {} ADD COLUMN ' || new_column || ' {} ';
END $$;
",
                        id,
                        entity_id,
                        value.sql_type()
                    );
                    persist.retry_tasks.push(column_creation);
                }

                _ => {
                    let query = format!(
                        "INSERT INTO entity_attributes (id, belongs_to) VALUES ('{}', '{}')",
                        value.id(),
                        entity_id
                    );

                    persist.tasks.push(query);

                    let column_creation = format!(
                        "
DO $$
DECLARE
    new_column TEXT;
    parent_table TEXT;
BEGIN
    -- Generate the new column name based on the existing column name
    SELECT name INTO new_column FROM entity_names WHERE id = '{}';
    SELECT name INTO parent_table FROM entity_names WHERE id = '{}';

    -- Create a new column on the table
    EXECUTE 'ALTER TABLE ' || parent_table || ' ADD COLUMN ' || new_column || ' {};';
END $$;
",
                        value.id(),
                        entity_id,
                        value.sql_type()
                    );
                    persist.retry_tasks.push(column_creation);
                }
            },

            SinkAction::NameAdded {
                space,
                entity_id,
                name,
            } => {
                //println!("Name added on entity {:?} in space {:?}", entity_id, space);

                let query = format!(
                    "INSERT INTO entity_names (id, name, space) VALUES ('{}', '{}', '{}')",
                    entity_id, name, space
                );

                persist.tasks.push(query);
            }

            SinkAction::ValueTypeAdded {
                space,
                attribute_id: _,
                value_type,
                entity_id,
            } => {
                // println!(
                //     "ValueType added to entity {:?} in space {:?}",
                //     entity_id, space
                // );

                let query = format!(
                    "INSERT INTO entity_value_types (id, value_type, space) VALUES ('{}', '{}', '{}')",
                    entity_id, value_type.id(), space
                );

                persist.tasks.push(query);
            }

            SinkAction::SubspaceAdded {
                parent_space,
                child_space,
            } => {
                // println!(
                //     "Subspace added to space {:?} with child space {:?}",
                //     parent_space, child_space
                // );

                let query = format!(
                    "INSERT INTO subspaces (id, subspace_of) VALUES ('{}', '{}')",
                    child_space, parent_space
                );

                persist.tasks.push(query);
            }

            SinkAction::SubspaceRemoved {
                parent_space,
                child_space,
            } => {
                // println!(
                //     "Subspace removed from space {:?} with child space {:?}",
                //     parent_space, child_space
                // );

                let query = format!(
                    "DELETE FROM subspaces WHERE id = '{}' AND subspace_of = '{}'",
                    child_space, parent_space
                );

                persist.tasks.push(query);
            }
        };
        Ok(())
    }
}
