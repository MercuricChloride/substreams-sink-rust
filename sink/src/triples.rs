//! This module contains definitions for geo triples and helpers for working with them.
//!
//! Triples were confusing to me when I was first using them. So I'm going to try to explain them here.
//!
//! You can use the terms Triples and Edges interchangeably.
//! You can also use the terms Entity and Node interchangeably.
//!
//! They are used to show relationships between things.
//! So if I want to show that I own a car, I would create a triple that looks like this:
//!
//! `(Me, Owns, Car)`
//!
//! Where everything within a triple is an Entity.
//! So I would have an Entity for Me, an Entity for Owns, and an Entity for Car.
//! And then I would create a triple that connects them.
//!
//! However, there is an important distinction to make here.
//! In geo we also have triples that have primitive "values"
//! So if I wanted to show that I am 22 years old, I could create a triple that looks like this:
//!
//! `(Me, YearsOld, 22)`

use anyhow::Error;
use base64::{engine::general_purpose, Engine as _};
use futures03::future::{join_all, try_join_all};
use migration::DbErr;
use sea_orm::DatabaseConnection;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use tokio::sync::mpsc::Sender;

use crate::{
    constants::Attributes,
    models::{self, accounts, actions},
    pb::schema::EntryAdded,
    sink_actions::SinkAction,
};

pub const IPFS_ENDPOINT: &str = "https://ipfs.network.thegraph.com/api/v0/cat?arg=";

// An action is a collection of action triples, this is used to represent a change to the graph.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Action {
    /// Tbh I'm not sure why this is called type, but it is.
    #[serde(rename = "type")]
    pub action_type: String,
    /// ???
    pub version: String,
    /// The collection of action triples that make up this action.
    pub actions: Vec<ActionTriple>,
    /// the space that this action was emitted from
    #[serde(skip)]
    pub space: String,
    /// The author of this action
    #[serde(skip)]
    pub author: String,
}

impl Action {
    /// This function adds all of the action triples in this action to the database.
    pub async fn execute_action_triples(
        &self,
        db: &DatabaseConnection,
        sender: &Sender<String>,
    ) -> Result<(), DbErr> {
        try_join_all(
            self.actions
                .iter()
                .map(|action_triple| action_triple.execute(db, sender)),
        )
        .await?;
        Ok(())
    }

    pub async fn add_author_to_db(
        &self,
        db: &DatabaseConnection,
        sender: &Sender<String>,
    ) -> Result<(), DbErr> {
        let author_address = self.author.clone();
        accounts::create(db, author_address, sender).await
    }

    /// This function returns a vector of all the sink actions that should be handled in this action.
    pub fn get_sink_actions(self) -> Vec<SinkAction> {
        self.actions
            .into_iter()
            .flat_map(|action| action.get_sink_actions())
            .collect::<Vec<SinkAction>>()
    }

    fn decode_with_space(
        json: &[u8],
        updated_space: &str,
        updated_author: &str,
    ) -> Result<Self, Error> {
        let mut action: Action = serde_json::from_slice(json)?;
        for action_triple in action.actions.iter_mut() {
            match action_triple {
                ActionTriple::CreateEntity { space, author, .. } => {
                    *space = updated_space.to_string();
                    *author = updated_author.to_string();
                }
                ActionTriple::CreateTriple { space, author, .. } => {
                    *space = updated_space.to_string();
                    *author = updated_author.to_string();
                }
                ActionTriple::DeleteTriple { space, author, .. } => {
                    *space = updated_space.to_string();
                    *author = updated_author.to_string();
                }
            }
        }
        action.space = updated_space.to_string();
        action.author = updated_author.to_string();
        Ok(action)
    }

    pub async fn decode_from_entry(entry: &EntryAdded) -> Result<Self, Error> {
        let uri = &entry.uri;
        match uri {
            uri if uri.starts_with("data:application/json;base64,") => {
                let data = uri.split("base64,").last().unwrap();
                let decoded = general_purpose::URL_SAFE.decode(data.as_bytes()).unwrap();
                let space = &entry.space;
                let author = &entry.author;
                Action::decode_with_space(&decoded, space, author)
            }
            uri if uri.starts_with("ipfs://") => {
                let cid = uri.trim_start_matches("ipfs://");
                let url = format!("{}{}", IPFS_ENDPOINT, cid);

                // check if we have a locally cached version of the file
                let path = format!("./ipfs-data/{}.json", cid);

                if let Ok(data) = std::fs::read_to_string(&path) {
                    let space = &entry.space;
                    let author = &entry.author;
                    return Action::decode_with_space(data.as_bytes(), space, author);
                } else {
                    let mut attempts: i32 = 0;
                    let data;
                    loop {
                        match reqwest::get(&url).await {
                            Ok(ipfs_data) => {
                                data = ipfs_data.text().await.unwrap();
                                break;
                            }

                            Err(err) => {
                                attempts += 1;

                                if attempts > 3 {
                                    return Err(Error::msg(format!(
                                        "Failed to fetch IPFS data: {}",
                                        err
                                    )));
                                }
                            }
                        }
                    }
                    let space = &entry.space;
                    let author = &entry.author;

                    // cache the file locally
                    std::fs::write(&path, &data)?;

                    Action::decode_with_space(&data.as_bytes(), space, author)
                }
            }
            _ => Err(Error::msg("Invalid URI")),
        }
    }
}

/// In geo we have a concept of actions, which represent changes to make in the graph.
/// This enum represents the different types of actions that can be taken.
#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub enum ActionTriple {
    CreateEntity {
        entity_id: String,
        #[serde(skip)]
        space: String,
        #[serde(skip)]
        author: String,
    },
    CreateTriple {
        entity_id: String,
        attribute_id: String,
        value: ValueType,
        #[serde(skip)]
        space: String,
        #[serde(skip)]
        author: String,
    },
    DeleteTriple {
        entity_id: String,
        attribute_id: String,
        value: ValueType,
        #[serde(skip)]
        space: String,
        #[serde(skip)]
        author: String,
    },
}

impl<'de> Deserialize<'de> for ActionTriple {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let val = Value::deserialize(deserializer)?;

        match val.get("type").and_then(Value::as_str) {
            Some("createEntity") => Ok(ActionTriple::CreateEntity {
                entity_id: val["entityId"].as_str().unwrap().to_string(),
                space: "".to_string(),
                author: "".to_string(),
            }),
            Some("createTriple") => Ok(ActionTriple::CreateTriple {
                entity_id: val["entityId"].as_str().unwrap().to_string(),
                attribute_id: val["attributeId"].as_str().unwrap().to_string(),
                value: serde_json::from_value(val["value"].clone()).unwrap(),
                space: "".to_string(),
                author: "".to_string(),
            }),
            Some("deleteTriple") => Ok(ActionTriple::DeleteTriple {
                entity_id: val["entityId"].as_str().unwrap().to_string(),
                attribute_id: val["attributeId"].as_str().unwrap().to_string(),
                value: serde_json::from_value(val["value"].clone()).unwrap(),
                space: "".to_string(),
                author: "".to_string(),
            }),
            _ => Err(serde::de::Error::custom("Unknown type")),
        }
    }
}

impl ActionTriple {
    /// This method includes the action_triple in the database
    pub async fn execute(
        &self,
        db: &DatabaseConnection,
        sender: &Sender<String>,
    ) -> Result<(), DbErr> {
        actions::create(db, self, sender).await
    }

    pub fn action_type(&self) -> &'static str {
        match self {
            ActionTriple::CreateEntity { .. } => "createEntity",
            ActionTriple::CreateTriple { .. } => "createTriple",
            ActionTriple::DeleteTriple { .. } => "deleteTriple",
        }
    }

    pub fn entity_id(&self) -> &String {
        match self {
            ActionTriple::CreateEntity { entity_id, .. } => entity_id,
            ActionTriple::CreateTriple { entity_id, .. } => entity_id,
            ActionTriple::DeleteTriple { entity_id, .. } => entity_id,
        }
    }

    /// This method returns a vector of all the sink actions that should be handled in this action triple.
    pub fn get_sink_actions(self) -> Vec<SinkAction> {
        // all of the possible actions that can be taken in an action triple.
        let actions = vec![
            self.get_type_created(),
            self.get_space_created(),
            self.get_attribute_added(),
            self.get_name_added(),
            self.get_value_type_added(),
            self.get_subspace_added(),
        ];

        let default_action = self.get_default_action();

        // if there are any actions, return them, otherwise return the default action.
        if let Some(action) = actions.into_iter().flatten().next() {
            vec![default_action, action]
        } else {
            vec![default_action]
        }
    }

    fn get_default_action(self) -> SinkAction {
        match self {
            ActionTriple::CreateEntity {
                entity_id,
                space,
                author,
            } => SinkAction::EntityCreated {
                space,
                entity_id,
                author,
            },
            ActionTriple::CreateTriple {
                entity_id,
                attribute_id,
                value,
                space,
                author,
            } => SinkAction::TripleAdded {
                space,
                entity_id,
                attribute_id,
                value,
                author,
            },
            ActionTriple::DeleteTriple {
                entity_id,
                attribute_id,
                value,
                space,
                author,
            } => SinkAction::TripleDeleted {
                space,
                entity_id,
                attribute_id,
                value,
                author,
            },
        }
    }

    fn get_type_created(&self) -> Option<SinkAction> {
        match self {
            ActionTriple::CreateTriple {
                entity_id,
                attribute_id,
                space,
                ..
            } if attribute_id.starts_with(Attributes::Type.id()) => Some(SinkAction::TypeCreated {
                entity_id: entity_id.to_string(),
                space: space.to_string(),
            }),
            _ => None,
        }
    }

    fn get_space_created(&self) -> Option<SinkAction> {
        match self {
            ActionTriple::CreateTriple {
                attribute_id,
                value,
                entity_id,
                ..
            } if attribute_id.starts_with(Attributes::Space.id()) => {
                // if the attribute id is space, and the value is a string, then we have created a space.
                if let ValueType::String { id: _, value } = value {
                    Some(SinkAction::SpaceCreated {
                        entity_id: entity_id.to_string(),
                        space: value.clone(),
                    })
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn get_attribute_added(&self) -> Option<SinkAction> {
        match self {
            ActionTriple::CreateTriple {
                entity_id,
                attribute_id,
                value,
                space,
                ..
            } if attribute_id.starts_with(Attributes::Attribute.id()) => {
                // if the attribute id is attribute, then we have added an attribute to an entity.
                if let ValueType::Entity { id } = value {
                    return Some(SinkAction::AttributeAdded {
                        space: space.clone(),
                        entity_id: entity_id.clone(),
                        attribute_id: id.clone(),
                        value: value.clone(),
                    });
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn get_name_added(&self) -> Option<SinkAction> {
        match self {
            ActionTriple::CreateTriple {
                entity_id,
                attribute_id,
                value,
                space,
                ..
            } if attribute_id.starts_with(Attributes::Name.id()) => {
                // if the attribute id is attribute, then we have added an attribute to an entity.
                if let ValueType::String { value, .. } = value {
                    return Some(SinkAction::NameAdded {
                        space: space.clone(),
                        entity_id: entity_id.clone(),
                        name: value.clone(),
                    });
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn get_value_type_added(&self) -> Option<SinkAction> {
        match self {
            ActionTriple::CreateTriple {
                entity_id,
                attribute_id,
                value,
                space,
                ..
            } if attribute_id.starts_with(Attributes::ValueType.id()) => {
                Some(SinkAction::ValueTypeAdded {
                    space: space.clone(),
                    entity_id: entity_id.clone(),
                    attribute_id: attribute_id.clone(),
                    value_type: value.clone(),
                })
            }
            _ => None,
        }
    }

    fn get_subspace_added(&self) -> Option<SinkAction> {
        match self {
            ActionTriple::CreateTriple {
                entity_id,
                attribute_id,
                value,
                ..
            } if attribute_id.starts_with(Attributes::Subspace.id()) => {
                let child_space_id = match value {
                    ValueType::Entity { id } => id,
                    _ => return None,
                };

                Some(SinkAction::SubspaceAdded {
                    parent_space: entity_id.clone(),
                    child_space: child_space_id.clone(),
                })
            }
            _ => None,
        }
    }

    fn get_subspace_removed(&self) -> Option<SinkAction> {
        match self {
            ActionTriple::DeleteTriple {
                entity_id,
                attribute_id,
                value,
                ..
            } if attribute_id.starts_with(Attributes::Subspace.id()) => {
                let child_space_id = match value {
                    ValueType::Entity { id } => id,
                    _ => return None,
                };

                Some(SinkAction::SubspaceRemoved {
                    parent_space: entity_id.clone(),
                    child_space: child_space_id.clone(),
                })
            }
            _ => None,
        }
    }
}

/// This represents the value type of a triple. IE The final part of a triple. (Entity, Attribute, _Value_)
#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub enum ValueType {
    /// The number value
    Number {
        /// The uuid of this specific number
        id: String,
        /// The value of the number
        value: f64,
    },
    /// The string value
    String {
        /// The uuid of this specific string
        id: String,
        /// The value of the string
        value: String,
    },
    /// The url of the image I think?
    Image {
        /// The uuid of this specific image
        id: String,
        /// The link to the image
        value: String,
    },
    Entity {
        /// The id of the entity
        id: String,
    },
    Date {
        /// The id of the date
        id: String,
        /// The date string ISO 8601
        value: String,
    },
    Url {
        /// The id of the url
        id: String,
        /// The url string
        value: String,
    },
}

impl ValueType {
    pub fn id(&self) -> &str {
        match self {
            ValueType::Number { id, .. } => id,
            ValueType::String { id, .. } => id,
            ValueType::Image { id, .. } => id,
            ValueType::Entity { id } => id,
            ValueType::Date { id, .. } => id,
            ValueType::Url { id, .. } => id,
        }
    }

    pub fn value(&self) -> String {
        match self {
            ValueType::Number { value, .. } => value.to_string(),
            ValueType::String { value, .. } => value.to_string(),
            ValueType::Image { value, .. } => value.to_string(),
            ValueType::Entity { id } => id.to_string(),
            ValueType::Date { value, .. } => value.to_string(),
            ValueType::Url { value, .. } => value.to_string(),
        }
    }

    pub fn value_type(&self) -> &str {
        match self {
            ValueType::Number { .. } => "number",
            ValueType::String { .. } => "string",
            ValueType::Image { .. } => "image",
            ValueType::Entity { .. } => "entity",
            ValueType::Date { .. } => "date",
            ValueType::Url { .. } => "url",
        }
    }

    pub fn sql_type(&self) -> &str {
        match self {
            ValueType::Number { .. } => "INTEGER",
            ValueType::String { .. } => "TEXT",
            ValueType::Image { .. } => "TEXT",
            ValueType::Entity { .. } => "TEXT FOREIGN KEY REFERENCES ' || new_column ||' ",
            ValueType::Date { .. } => "TEXT",
            ValueType::Url { .. } => "TEXT",
        }
    }
}

impl<'de> Deserialize<'de> for ValueType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let val = Value::deserialize(deserializer)?;

        match val.get("type").and_then(Value::as_str) {
            Some("number") => Ok(ValueType::Number {
                id: val["id"].as_str().unwrap().to_string(),
                value: val["value"].as_f64().unwrap(),
            }),
            Some("string") => Ok(ValueType::String {
                id: val["id"].as_str().unwrap().to_string(),
                value: val["value"].as_str().unwrap().to_string(),
            }),
            Some("image") => Ok(ValueType::Image {
                id: val["id"].as_str().unwrap().to_string(),
                value: val["value"].as_str().unwrap().to_string(),
            }),
            Some("entity") => Ok(ValueType::Entity {
                id: val["id"].as_str().unwrap().to_string(),
            }),
            Some("date") => Ok(ValueType::Date {
                id: val["id"].as_str().unwrap().to_string(),
                value: val["value"].as_str().unwrap().to_string(),
            }),
            Some("url") => Ok(ValueType::Url {
                id: val["id"].as_str().unwrap().to_string(),
                value: val["value"].as_str().unwrap().to_string(),
            }),
            _ => Err(serde::de::Error::custom("Unknown type")),
        }
    }
}

impl Into<Vec<SinkAction>> for ActionTriple {
    fn into(self) -> Vec<SinkAction> {
        self.get_sink_actions()
    }
}

// #[cfg(test)]
// mod tests {
//     use std::fs;

//     use crate::persist::Persist;

//     use super::*;

//     const DEFAULT_SPACE: &'static str = "0xSpaceAddress";

//     /// This function will bootstrap the persist with a type, give it a name
//     /// This is used for testing
//     /// It's id is "basic-type"
//     /// It's name is "Basic Type"
//     fn bootstrap_persist(persist: &mut Persist) {
//         // a triple that marks the basic-type as a "type"
//         let simple_type = ActionTriple::CreateTriple {
//             entity_id: "basic-type".to_string(),
//             attribute_id: Attributes::Type.id().to_string(),
//             value: ValueType::Entity {
//                 id: "type-uuid".to_string(),
//             },
//             space: DEFAULT_SPACE.to_string(),
//         };

//         assert!(matches!(
//             simple_type.get_sink_action(),
//             Some(SinkAction::TypeCreated { .. })
//         ));

//         // a triple that gives a name to the basic type
//         let simple_name = ActionTriple::CreateTriple {
//             entity_id: "basic-type".to_string(),
//             attribute_id: Attributes::Name.id().to_string(),
//             value: ValueType::String {
//                 id: "string-uuid".to_string(),
//                 value: "Basic Type".to_string(),
//             },
//             space: DEFAULT_SPACE.to_string(),
//         };

//         assert!(matches!(
//             simple_name.get_sink_action(),
//             Some(SinkAction::NameAdded { .. })
//         ));

//         // bootstrap the persist with a simple type
//         simple_type
//             .get_sink_action()
//             .unwrap()
//             .handle_sink_action(persist)
//             .unwrap();

//         // Give a name to the basic type
//         simple_name
//             .get_sink_action()
//             .unwrap()
//             .handle_sink_action(persist)
//             .unwrap();
//     }

//     //#[test]
//     fn can_get_spaces_created() {
//         let mut persist = Persist::default();

//         // bootstrap the persist
//         bootstrap_persist(&mut persist);

//         let space_created = ActionTriple::CreateTriple {
//             entity_id: "entity-id".to_string(),
//             attribute_id: Attributes::Space.id().to_string(),
//             value: ValueType::String {
//                 id: "some-uuid".to_string(),
//                 value: "some-space".to_string(),
//             },
//             space: DEFAULT_SPACE.to_string(),
//         };

//         let sink_action = space_created.get_sink_action().unwrap();

//         // check that the sink action is a space created action
//         assert!(matches!(sink_action, SinkAction::SpaceCreated { .. }));

//         // handle the sink action
//         sink_action.handle_sink_action(&mut persist).unwrap();

//         // check that the space was created in the persist
//         assert_eq!(persist.spaces.get("entity-id").unwrap(), "some-space");

//         let no_space_created = ActionTriple::CreateTriple {
//             entity_id: "entity-id".to_string(),
//             attribute_id: Attributes::Attribute.id().to_string(),
//             value: ValueType::String {
//                 id: "some-uuid".to_string(),
//                 value: "some-space".to_string(),
//             },
//             space: DEFAULT_SPACE.to_string(),
//         };

//         assert!(matches!(no_space_created.get_sink_action(), None));
//     }

//     //#[test]
//     fn can_get_attribute_added() {
//         let mut persist = Persist::default();

//         bootstrap_persist(&mut persist);

//         // make the entity-id a type
//         let action = ActionTriple::CreateTriple {
//             entity_id: "entity-id".to_string(),
//             attribute_id: Attributes::Type.id().to_string(),
//             value: ValueType::Entity {
//                 id: "type-uuid".to_string(),
//             },
//             space: DEFAULT_SPACE.to_string(),
//         };

//         let sink_action = action.get_sink_action().unwrap();

//         assert!(matches!(sink_action, SinkAction::TypeCreated { .. }));

//         sink_action.handle_sink_action(&mut persist).unwrap();

//         // add the basic-type as an attribute
//         let action = ActionTriple::CreateTriple {
//             entity_id: "entity-id".to_string(),
//             attribute_id: Attributes::Attribute.id().to_string(),
//             value: ValueType::Entity {
//                 id: "basic-type".to_string(),
//             },
//             space: DEFAULT_SPACE.to_string(),
//         };

//         let sink_action = action.get_sink_action().unwrap();

//         assert!(matches!(sink_action, SinkAction::AttributeAdded { .. }));

//         sink_action.handle_sink_action(&mut persist).unwrap();

//         let attribute = persist.attributes.get("basic-type").unwrap();

//         assert_eq!(attribute.name, "Basic Type");
//         assert_eq!(attribute.entity_id, "basic-type");

//         // the entity-id have the basic-type as an attribute
//         let type_ = persist.types.get("entity-id").unwrap();

//         assert_eq!(type_.attributes, vec!["basic-type".to_string()]);
//     }

//     //#[test]
//     fn can_add_value_type() {
//         let mut persist = Persist::default();

//         bootstrap_persist(&mut persist);

//         // add a valuetype to the basic-type
//         let action = ActionTriple::CreateTriple {
//             entity_id: "basic-type".to_string(),
//             attribute_id: Attributes::ValueType.id().to_string(),
//             value: ValueType::Entity {
//                 id: "text-value-type".to_string(),
//             },
//             space: DEFAULT_SPACE.to_string(),
//         };

//         let sink_action = action.get_sink_action().unwrap();

//         assert!(matches!(sink_action, SinkAction::ValueTypeAdded { .. }));

//         sink_action.handle_sink_action(&mut persist).unwrap();

//         let value_type = persist.value_types.get("basic-type").unwrap();

//         match value_type {
//             ValueType::Entity { id } => {
//                 assert_eq!(id, "text-value-type");
//             }
//             _ => panic!("value type should be an entity"),
//         }
//     }

//     //#[test]
//     fn can_add_subspaces() {
//         let mut persist = Persist::default();

//         bootstrap_persist(&mut persist);

//         // create a space
//         let first_space = ActionTriple::CreateTriple {
//             entity_id: "first-space".to_string(),
//             attribute_id: Attributes::Space.id().to_string(),
//             value: ValueType::String {
//                 id: "some-uuid".to_string(),
//                 value: "0xfirst".to_string(),
//             },
//             space: DEFAULT_SPACE.to_string(),
//         };

//         let second_space = ActionTriple::CreateTriple {
//             entity_id: "second-space".to_string(),
//             attribute_id: Attributes::Space.id().to_string(),
//             value: ValueType::String {
//                 id: "another-uuid".to_string(),
//                 value: "0xsecond".to_string(),
//             },
//             space: DEFAULT_SPACE.to_string(),
//         };

//         let first_space_sink_action = first_space.get_sink_action().unwrap();

//         assert!(matches!(
//             first_space_sink_action,
//             SinkAction::SpaceCreated { .. }
//         ));

//         first_space_sink_action
//             .handle_sink_action(&mut persist)
//             .unwrap();

//         let second_space_sink_action = second_space.get_sink_action().unwrap();

//         assert!(matches!(
//             second_space_sink_action,
//             SinkAction::SpaceCreated { .. }
//         ));

//         second_space_sink_action
//             .handle_sink_action(&mut persist)
//             .unwrap();

//         assert_eq!(
//             persist.spaces.get("first-space"),
//             Some(&"0xfirst".to_string())
//         );

//         assert_eq!(
//             persist.spaces.get("second-space"),
//             Some(&"0xsecond".to_string())
//         );

//         // add the second space as a subspace of the first space
//         let subspace = ActionTriple::CreateTriple {
//             entity_id: "first-space".to_string(),
//             attribute_id: Attributes::Subspace.id().to_string(),
//             value: ValueType::Entity {
//                 id: "second-space".to_string(),
//             },
//             space: DEFAULT_SPACE.to_string(),
//         };

//         let subspace_sink_action = subspace.get_sink_action().unwrap();

//         assert!(matches!(
//             subspace_sink_action,
//             SinkAction::SubspaceAdded { .. }
//         ));

//         subspace_sink_action
//             .handle_sink_action(&mut persist)
//             .unwrap();
//     }

//     #[test]
//     fn decoding_stress_test() {
//         let file = fs::read_to_string("city-new-entity-actions.json").unwrap();

//         let mega_file = file.repeat(100);

//         println!("mega file size: {}", mega_file.len());

//         let actions: Vec<ActionTriple> = serde_json::from_str(&file).unwrap();

//         let sink_actions: Vec<SinkAction> = actions
//             .iter()
//             .filter_map(|action| action.get_sink_action())
//             .collect();

//         println!("sink actions: {:?}", sink_actions.len());
//     }
// }
