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
use serde::{Deserialize, Serialize};

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
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ActionTriple {
    #[serde(rename = "type")]
    pub action_triple_type: ActionTripleType,
    pub entity_id: String,
    pub attribute_id: String,
    pub value: ActionTripleValue,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ActionTripleValue {
    #[serde(rename = "type")]
    pub value_type: ValueType,
    pub value: Option<String>,
    pub id: String,
}

/// In geo we have a concept of actions, which represent changes to make in the graph.
/// This enum represents the different types of actions that can be taken.
#[derive(Serialize, Deserialize, Debug)]
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
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub enum ValueType {
    String,
    Number,
    Entity,
    Null,
}

impl Action {
    /// This function returns a vector of all the spaces that were created in this action.
    /// A space is created when we create an action triple describing a space, and the value is a string that is an address of the space
    pub fn get_created_spaces(&self) -> Vec<String> {
        self.actions
            .iter()
            .filter_map(|action| {
                if action.attribute_id != "space" {
                    return None;
                }

                match action.action_triple_type {
                    ActionTripleType::Create => (),
                    _ => return None,
                };

                match &action.value {
                    ActionTripleValue {
                        value_type: ValueType::String,
                        value: Some(value),
                        ..
                    } => Some(value.clone()),
                    _ => None,
                }
            })
            .collect::<Vec<String>>()
    }

    pub async fn decode_from_uri(uri: &String) -> Self {
        match &uri {
            uri if uri.starts_with("data:application/json;base64,") => {
                let data = uri.split("base64,").last().unwrap();
                let decoded = general_purpose::URL_SAFE.decode(data.as_bytes()).unwrap();
                let actions = serde_json::from_slice(&decoded).unwrap();
                actions
            }
            uri if uri.starts_with("ipfs://") => {
                let cid = uri.trim_start_matches("ipfs://");
                let url = format!("{}{}", IPFS_ENDPOINT, cid);
                let mut attempts: i32 = 0;
                let data;
                loop {
                    match reqwest::get(&url).await {
                        Ok(ipfs_data) => {
                            data = ipfs_data
                                .text()
                                .await
                                .expect("Failed to get text from ipfs_data");
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
                let action: Action = serde_json::from_str(&data).unwrap();
                action
            }
            _ => panic!("Invalid URI"), //TODO Handle this gracefully
        }
    }
}

//pub fn handle_action_triple(action_triple: ActionTripleType) {}

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
        let spaces = action.get_created_spaces();
        assert_eq!(spaces, test_spaces);
    }

    #[test]
    fn can_decode_uri_data() {
        //let action: Action = Action::decode_from_uri(DATA.to_string()).await;
        //println!("{:?}", action);
    }
}
