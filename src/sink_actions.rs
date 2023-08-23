use std::process;

use crate::{persist::Persist, triples::ValueType};

#[derive(Debug)]
/// This enum represents different actions that the sink should handle. Actions being specific changes to the graph.
pub enum SinkAction {
    /// This action denotes a newly created space. The string is the address of the space.
    /// We care about this in the sink because when a new space is created, we need to deploy
    /// a new subgraph for that space.
    SpaceCreated { space: String },

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
}

impl SinkAction {
    pub fn handle_sink_action(&self, persist: &mut Persist) -> Result<(), String> {
        match &self {
            SinkAction::SpaceCreated { space } => {
                // push the space to the persist
                persist.push_space(space.to_string());
                let command = format!(
                    "(cd external/subgraph && ./deploy.sh mercuricchloride/geo-test {})",
                    space
                );
                println!("Deploying subgraph for space: {}", space);
                process::Command::new(command);
            }

            SinkAction::TypeCreated { entity_id, space } => {
                persist.add_type(entity_id, space);
                println!("Type added: {:?} in space {:?}", entity_id, space);
            }

            SinkAction::AttributeAdded {
                space,
                entity_id,
                attribute_id,
                value,
            } => match value {
                ValueType::Entity { id } => {
                    persist.add_attribute(entity_id, id, space);
                    println!(
                        "Attribute added: {:?} in space {:?} on entity {:?}",
                        attribute_id, space, entity_id
                    );
                }
                _ => {}
            },

            SinkAction::NameAdded {
                space,
                entity_id,
                name,
            } => {
                println!("Name added on entity {:?} in space {:?}", entity_id, space);
                persist.add_name(entity_id, name);
            }

            SinkAction::ValueTypeAdded {
                space,
                attribute_id,
                value_type,
                entity_id: _,
            } => {
                println!(
                    "ValueType added to attribute {:?} in space {:?}",
                    attribute_id, space
                );
                persist.add_value_type(attribute_id, value_type.clone());
            }
        };
        Ok(())
    }
}
