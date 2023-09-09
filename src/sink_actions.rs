use futures03::future::join_all;
use tokio_postgres::Client;

use crate::triples::ValueType;

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

pub async fn handle_sink_actions(
    sink_actions: Vec<SinkAction>,
    client: &Client,
) -> Vec<Result<(), SqlError>> {
    join_all(sink_actions.iter().map(|action| action.execute(&client))).await
}

#[derive(Clone)]
pub enum SqlError {
    /// This error happens when a presumed safe query fails
    SafeQueryFailure(Vec<String>),
    /// This error happens when an unsafe query fails
    UnsafeFailure(String),
    /// This error happens when both queries fail, returns (safe_query, unsafe_query)
    BothQueriesFailed(Vec<String>, String),
}

impl SqlError {
    // pub fn from_queries<T, E>(
    //     safe_query: Result<T, E>,
    //     unsafe_query: Option<Result<T, E>>,
    // ) -> Result<(), Self> {
    //     if let Some(unsafe_query) = unsafe_query {
    //         match (safe_query, unsafe_query) {
    //             (Ok(_), Ok(_)) => Ok(()),
    //             (Err(_), Ok(_)) => Err(SqlError::SafeQueryFailure("".to_string())),
    //             (Ok(_), Err(_)) => Err(SqlError::UnsafeFailure("".to_string())),
    //             (Err(_), Err(_)) => {
    //                 Err(SqlError::BothQueriesFailed("".to_string(), "".to_string()))
    //             }
    //         }
    //     } else {
    //         match safe_query {
    //             Ok(_) => Ok(()),
    //             Err(_) => Err(SqlError::SafeQueryFailure("".to_string())),
    //         }
    //     }
    // }

    pub async fn retry_query(&self, client: &Client) -> Result<(), Self> {
        match self {
            SqlError::UnsafeFailure(query) => {
                let unsafe_execute = client.execute(query, &[]).await;
                if let Err(_) = unsafe_execute {
                    Err(SqlError::UnsafeFailure(query.clone()))
                } else {
                    Ok(())
                }
            }

            SqlError::SafeQueryFailure(queries) => {
                let mut failures = vec![];
                for query in queries {
                    let safe_execute = client.execute(query, &[]).await;
                    if let Err(_) = safe_execute {
                        failures.push(query.clone());
                    }
                }

                if failures.is_empty() {
                    Ok(())
                } else {
                    Err(SqlError::SafeQueryFailure(failures))
                }
            }

            SqlError::BothQueriesFailed(safe_queries, unsafe_query) => {
                let mut failures = vec![];
                for query in safe_queries {
                    let safe_execute = client.execute(query, &[]).await;
                    if let Err(_) = safe_execute {
                        failures.push(query.clone());
                    }
                }

                let unsafe_execute = client.execute(unsafe_query, &[]).await;
                match (failures.is_empty(), unsafe_execute) {
                    (true, Ok(_)) => Ok(()),
                    (false, Ok(_)) => Err(SqlError::SafeQueryFailure(failures)),
                    (true, Err(_)) => Err(SqlError::UnsafeFailure(unsafe_query.clone())),
                    (false, Err(_)) => {
                        Err(SqlError::BothQueriesFailed(failures, unsafe_query.clone()))
                    }
                }
            }
        }
    }
}

impl SinkAction {
    pub async fn execute(&self, client: &Client) -> Result<(), SqlError> {
        let (safe_queries, unsafe_query) = &self.queries();

        for query in safe_queries {
            let safe_execute = client.execute(query, &[]).await;
            if let Err(err) = safe_execute {
                panic!("Safe query failed: {} with err: {:?}", query, err);
            }
        }

        if let Some(unsafe_query) = unsafe_query {
            let unsafe_execute = client.execute(unsafe_query, &[]).await;
            if let Err(err) = unsafe_execute {
                panic!(
                    "Unsafe query failed: {:?} with err: {:?}",
                    unsafe_query, err
                );
            }
            Ok(())
            // match (safe_execute, unsafe_execute) {
            //     (Ok(_), Ok(_)) => Ok(()),
            //     (Err(_), Ok(_)) => Err(SqlError::SafeQueryFailure(query.clone())),
            //     (Ok(_), Err(_)) => Err(SqlError::UnsafeFailure(unsafe_query.clone())),
            //     (Err(_), Err(_)) => Err(SqlError::BothQueriesFailed(
            //         query.clone(),
            //         unsafe_query.clone(),
            //     )),
            // }
        } else {
            Ok(())
            // match safe_execute {
            //     Ok(_) => Ok(()),
            //     Err(_) => Err(SqlError::SafeQueryFailure(query.clone())),
            // }
        }
    }

    /// This function returns a tuple containing (SafeQuery, Option<UnsafeQuery>)
    fn queries(&self) -> (Vec<String>, Option<String>) {
        (self.get_query(), self.get_unsafe_query())
    }

    /// This function returns the SQL query we will always execute for this action.
    ///
    /// It is important to also try and grab the unsafe query that might fail, use the `get_unsafe_query` function for that
    fn get_query(&self) -> Vec<String> {
        match self {
            SinkAction::SpaceCreated { entity_id, space } => {
                vec![format!("INSERT INTO spaces (id, space) VALUES ('{entity_id}', '{space}')")]
            }

            SinkAction::TypeCreated { entity_id, space } => {
                vec![
                format!("
INSERT INTO entity_types (id, space) VALUES ('{entity_id}', '{space}');
"),
format!("CREATE TABLE \"{entity_id}\" (id TEXT);")
                    ]
            }

            SinkAction::AttributeAdded {
                entity_id, value, ..
            } => {
                let belongs_to = value.id();
                vec![
                    format!(
                        "INSERT INTO \"entity_attributes\" (id, belongs_to) VALUES ('{entity_id}', '{belongs_to}')"
                    )
                ]
            }

            SinkAction::NameAdded {
                space,
                entity_id,
                name,
            } => {
                let name = name.replace("'", "''");
                vec![
                format!("
INSERT INTO entity_names (id, name, space)
VALUES ('{entity_id}', '{name}', '{space}')
ON CONFLICT (id)
DO UPDATE SET
name = '{name}', space = '{space}'
                ")
                ]
            }

            SinkAction::ValueTypeAdded {
                space,
                entity_id,
                value_type,
                ..
            } => {
                let value_type = value_type.sql_type();
                vec![
                format!("
INSERT INTO \"entity_value_types\" (id, value_type, space)
VALUES ('{entity_id}', '{value_type}', '{space}')
ON CONFLICT (id)
DO UPDATE SET
value_type = '{value_type}', space = '{space}';
                ")
                ]
            }

            SinkAction::SubspaceAdded {
                parent_space,
                child_space,
            } => vec![format!("INSERT INTO \"subspaces\" (id, subspace_of) VALUES ('{child_space}', '{parent_space}')")],

            SinkAction::SubspaceRemoved { parent_space, child_space } => {
                vec![format!("DELETE FROM \"subspaces\" WHERE id = '{child_space}' AND subspace_of = '{parent_space}'")]
            }
        }
    }

    /// This function returns the SQL query that might fail for this action.
    /// If there is no query that might fail, this function returns `None`
    fn get_unsafe_query(&self) -> Option<String> {
        match self {
            SinkAction::AttributeAdded {
                value, entity_id, ..
            } => {
                let value_id = value.id();
                let value_type = value.sql_type();
                Some(format!(
                    "
ALTER TABLE \"{entity_id}\" ADD COLUMN \"{value_id}\" {value_type};
"
                ))
            }
            SinkAction::TypeCreated { entity_id, .. } => Some(format!(
                "
DO $$
DECLARE
    entity_name TEXT;
BEGIN
    SELECT name INTO entity_name FROM entity_names WHERE id = '{entity_id}';
    EXECUTE 'COMMENT ON TABLE \"{entity_id}\" IS E''@name ' || entity_name || '''';
END $$;
"
            )),
            _ => None,
        }
    }
}
