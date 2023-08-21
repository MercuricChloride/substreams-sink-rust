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

use base64::{engine::general_purpose, Engine as _};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

use crate::{constants::Attributes, pb::schema::EntryAdded, sink_actions::SinkAction};

pub const IPFS_ENDPOINT: &str = "https://ipfs.network.thegraph.com/api/v0/cat?arg=";

// An action is a collection of action triples, this is used to represent a change to the graph.
#[derive(Serialize, Deserialize, Debug)]
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
}

impl Action {
    fn decode_with_space(json: &[u8], updated_space: &str) -> Self {
        let mut action: Action = serde_json::from_slice(json).unwrap();
        for action_triple in action.actions.iter_mut() {
            match action_triple {
                ActionTriple::CreateEntity { space, .. } => {
                    *space = updated_space.to_string();
                }
                ActionTriple::CreateTriple { space, .. } => {
                    *space = updated_space.to_string();
                }
                ActionTriple::DeleteTriple { space, .. } => {
                    *space = updated_space.to_string();
                }
            }
        }
        action.space = updated_space.to_string();
        action
    }

    /// This function returns a vector of all the sink actions that should be handled in this action.
    pub fn get_sink_actions(&self) -> Option<Vec<SinkAction>> {
        let sink_actions = self
            .actions
            .iter()
            .filter_map(|action| action.get_sink_action())
            .collect::<Vec<SinkAction>>();

        if sink_actions.is_empty() {
            None
        } else {
            Some(sink_actions)
        }
    }

    pub async fn decode_from_entry(entry: &EntryAdded) -> Self {
        let uri = &entry.uri;
        match uri {
            uri if uri.starts_with("data:application/json;base64,") => {
                let data = uri.split("base64,").last().unwrap();
                let decoded = general_purpose::URL_SAFE.decode(data.as_bytes()).unwrap();
                let space = entry.space.clone();
                Action::decode_with_space(&decoded, &space)
            }
            uri if uri.starts_with("ipfs://") => {
                let cid = uri.trim_start_matches("ipfs://");
                let url = format!("{}{}", IPFS_ENDPOINT, cid);
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
                                panic!("{err}, IPFS fetch failed more than 3 times")
                            }
                        }
                    }
                }
                let space = entry.space.clone();
                Action::decode_with_space(&data.as_bytes(), &space)
            }
            _ => panic!("Invalid URI"), //TODO Handle this gracefully
        }
    }
}

// #[derive(Serialize, Deserialize, Debug, Clone)]
// #[serde(rename_all = "camelCase")]
// pub struct ActionTriple {
//     #[serde(rename = "type")]
//     pub action_triple_type: ActionTripleType,
//     pub entity_id: String,
//     pub attribute_id: String,
//     pub value: ValueType,
//     // this is not part of the triple, but it is used to store the space that the triple is in.
//     #[serde(skip)]
//     pub space: String,
// }

#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub enum ActionTriple {
    CreateEntity {
        entity_id: String,
        #[serde(skip)]
        space: String,
    },
    CreateTriple {
        entity_id: String,
        attribute_id: String,
        value: ValueType,
        #[serde(skip)]
        space: String,
    },
    DeleteTriple {
        entity_id: String,
        attribute_id: String,
        value: ValueType,
        #[serde(skip)]
        space: String,
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
            }),
            Some("createTriple") => Ok(ActionTriple::CreateTriple {
                entity_id: val["entityId"].as_str().unwrap().to_string(),
                attribute_id: val["attributeId"].as_str().unwrap().to_string(),
                value: serde_json::from_value(val["value"].clone()).unwrap(),
                space: "".to_string(),
            }),
            Some("deleteTriple") => Ok(ActionTriple::DeleteTriple {
                entity_id: val["entityId"].as_str().unwrap().to_string(),
                attribute_id: val["attributeId"].as_str().unwrap().to_string(),
                value: serde_json::from_value(val["value"].clone()).unwrap(),
                space: "".to_string(),
            }),
            _ => Err(serde::de::Error::custom("Unknown type")),
        }
    }
}

impl ActionTriple {
    /// This method returns a vector of all the sink actions that should be handled in this action triple.
    pub fn get_sink_action(&self) -> Option<SinkAction> {
        // all of the possible actions that can be taken in an action triple.
        let actions = vec![
            self.get_type_created(),
            self.get_space_created(),
            self.get_attribute_added(),
            self.get_name_added(),
            self.get_value_type_added(),
        ];

        // return the action if any
        actions.into_iter().flatten().next()
    }

    fn get_type_created(&self) -> Option<SinkAction> {
        match self {
            ActionTriple::CreateTriple {
                entity_id,
                attribute_id,
                value,
                space,
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
                ..
            } if attribute_id.starts_with(Attributes::Space.id()) => {
                // if the attribute id is space, and the value is a string, then we have created a space.
                if let ValueType::String { id: _, value } = value {
                    Some(SinkAction::SpaceCreated {
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
            } if attribute_id.starts_with(Attributes::Attribute.id()) => {
                // if the attribute id is attribute, then we have added an attribute to an entity.
                if let ValueType::Entity { id } = value {
                    return Some(SinkAction::AttributeAdded {
                        space: space.clone(),
                        entity_id: entity_id.clone(),
                        attribute_id: id.clone(),
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
            } if attribute_id.starts_with(Attributes::ValueType.id()) => {
                Some(SinkAction::ValueTypeAdded {
                    space: space.clone(),
                    entity_id: entity_id.clone(),
                    value_type: value.clone(),
                })
            }
            _ => None,
        }
    }
}

/// In geo we have a concept of actions, which represent changes to make in the graph.
/// This enum represents the different types of actions that can be taken.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ActionTripleType {
    /// This is used to create a new entity.
    #[serde(rename = "createTriple")]
    Create,
    /// This is used to update an existing triple.
    #[serde(rename = "updateTriple")]
    Update,
    /// This is used to delete an existing entity.
    #[serde(rename = "deleteTriple")]
    Delete,
}

/// An Entity in geo is a node in the graph that has a unique identifier.
#[derive(Serialize, Deserialize, Debug)]
pub struct Entity(pub String);

/// This represents a triple in the graph. (Entity, Attribute, Value)
/// Where the Entity is the node that the triple is connected to.
/// The Attribute is the type of relationship that the triple represents. (Which is also an entity / node in the graph)
/// And the Value is the value of the triple. (Either an entity or a primitive value)
#[derive(Serialize, Deserialize, Debug)]
pub struct Triple {
    pub entity: Entity,
    pub attribute: Entity,
    pub value: ValueType,
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

impl TryInto<SinkAction> for ActionTriple {
    type Error = String;

    fn try_into(self) -> Result<SinkAction, Self::Error> {
        self.get_sink_action()
            .ok_or("No sink action found".to_string())
    }
}

//pub fn handle_action_triple(action_triple: ActionTripleType) {}
// TODO I need to add the space to the Action
#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    const DATA: &'static str = "data:application/json;base64,eyJ0eXBlIjoicm9vdCIsInZlcnNpb24iOiIwLjAuMSIsImFjdGlvbnMiOlt7InR5cGUiOiJjcmVhdGVUcmlwbGUiLCJlbnRpdHlJZCI6IjgyYWU1ZTJiLWUwN2QtNDQ2MS1hODhiLTExNTg5MzFlNjliOCIsImF0dHJpYnV0ZUlkIjoibmFtZSIsInZhbHVlIjp7InR5cGUiOiJzdHJpbmciLCJ2YWx1ZSI6IkhlYWx0aCIsImlkIjoiYzYzODNiNTctMGRhYy00Mjg4LTliMDYtYWE2OWZmYTRkNjJlIn19LHsidHlwZSI6ImNyZWF0ZVRyaXBsZSIsImVudGl0eUlkIjoiODJhZTVlMmItZTA3ZC00NDYxLWE4OGItMTE1ODkzMWU2OWI4IiwiYXR0cmlidXRlSWQiOiJzcGFjZSIsInZhbHVlIjp7InR5cGUiOiJzdHJpbmciLCJ2YWx1ZSI6IjB4ZTNkMDg3NjM0OThlMzI0N0VDMDBBNDgxRjE5OUIwMThmMjE0ODcyMyIsImlkIjoiNjA4OWM3MzctMzJhOC00YzUxLWI4MjgtNjk0OWI5MjE2OWI0In19LHsidHlwZSI6ImNyZWF0ZVRyaXBsZSIsImVudGl0eUlkIjoiM2FkNGRmMjctMTMyZi00ZWY2LTg3ZjgtMDcwZjA2M2IwNzRjIiwiYXR0cmlidXRlSWQiOiJuYW1lIiwidmFsdWUiOnsidHlwZSI6InN0cmluZyIsInZhbHVlIjoiU2FuIEZyYW5jaXNjbyIsImlkIjoiMmUxZmY2ZDctYjU4Zi00ZDFmLTk0OWMtMTJlOTViMzM3YWY3In19LHsidHlwZSI6ImNyZWF0ZVRyaXBsZSIsImVudGl0eUlkIjoiM2FkNGRmMjctMTMyZi00ZWY2LTg3ZjgtMDcwZjA2M2IwNzRjIiwiYXR0cmlidXRlSWQiOiJzcGFjZSIsInZhbHVlIjp7InR5cGUiOiJzdHJpbmciLCJ2YWx1ZSI6IjB4YzQ2NjE4QzIwMGYwMkVGMUVFQTI4OTIzRkMzODI4MzAxZTYzQzRCZCIsImlkIjoiYTExYmQxN2YtZjNkZC00NjQxLWE2Y2ItNjhmMDkwOThkZGU3In19XX0=";

    #[test]
    fn can_serialize_mock_data() {
        let mock_data = fs::read_to_string("./mock-data.json").unwrap();
        let action: Action = serde_json::from_str(&mock_data).unwrap();
        println!("{:?}", action);
    }

    #[test]
    fn can_get_find_spaces_created() {
        let mock_data = fs::read_to_string("./mock-data.json").unwrap();
        let action: Action = serde_json::from_str(&mock_data).unwrap();
        let test_spaces = vec![
            "0xe3d08763498e3247EC00A481F199B018f2148723".to_string(),
            "0xc46618C200f02EF1EEA28923FC3828301e63C4Bd".to_string(),
        ];
        //let spaces = action.get_created_spaces();
        //assert_eq!(spaces, test_spaces);
    }

    #[test]
    fn can_decode_uri_data() {
        //let action: Action = Action::decode_from_uri(DATA.to_string()).await;
        //println!("{:?}", action);
    }
}
