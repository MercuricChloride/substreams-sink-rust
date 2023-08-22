use std::{collections::HashMap, fs::File};

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

use crate::triples::ValueType;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Persist {
    /// The cursor to start from
    //pub cursor: Option<String>,
    /// A map from entity id -> fields
    pub types: Option<HashMap<String, Type>>,
    /// An array of spaces created
    pub spaces: Option<Vec<String>>,
    /// A map from attribute id -> attribute
    pub attributes: Option<HashMap<String, Attribute>>,
    /// A map from entity id -> name
    pub names: Option<HashMap<String, String>>,
    /// A map from entity id -> value type
    pub value_types: Option<HashMap<String, ValueType>>,
}

impl Persist {
    pub fn from_file(path: Option<String>) -> Self {
        let file;

        if let Some(path) = path {
            file = File::open(path).unwrap();
        } else {
            file = File::open("persist.json").unwrap();
        };

        serde_json::from_reader(file).unwrap()
    }

    pub fn open() -> Self {
        if let Ok(opened_file) = File::open("persist.json") {
            serde_json::from_reader(opened_file).unwrap()
        } else {
            let persist = Self::default();
            persist.save();
            persist
        }
    }

    pub fn save(&self) {
        let file = File::create("persist.json").unwrap();
        serde_json::to_writer(file, &self).unwrap();
    }

    pub fn save_to_file(&self, path: String) {
        let file = File::create(path).unwrap();
        serde_json::to_writer(file, &self).unwrap();
    }

    pub fn push_space(&mut self, space: String) {
        if let Some(spaces) = &mut self.spaces {
            spaces.push(space);
        } else {
            self.spaces = Some(vec![space]);
        }
    }

    pub fn add_type(&mut self, entity_id: &String, space: &String) {
        if let Some(types) = &mut self.types {
            types.insert(
                entity_id.clone(),
                Type {
                    entity_id: entity_id.clone(),
                    space: space.clone(),
                    attributes: vec![],
                },
            );
        } else {
            let mut types = HashMap::new();
            types.insert(
                entity_id.clone(),
                Type {
                    entity_id: entity_id.clone(),
                    space: space.clone(),
                    attributes: vec![],
                },
            );
            self.types = Some(types);
        }
    }

    /// entity_id is the id of the entity that the attribute was added to
    pub fn add_attribute(&mut self, entity_id: &String, attribute_id: &String, space: &String) {
        if let Some(types) = &mut self.types {
            if let Some(type_) = types.get_mut(entity_id) {
                type_.attributes.push(attribute_id.clone());
            } else {
                self.add_type(entity_id, space);
                self.add_attribute(entity_id, attribute_id, space);
            }
        } else {
            panic!("Tried to add an attribute before types was initialized in the persist");
        }
    }

    pub fn add_name(&mut self, entity_id: &String, name: &String) {
        if let Some(names) = &mut self.names {
            names.insert(entity_id.clone(), name.clone());
        } else {
            let mut names = HashMap::new();
            names.insert(entity_id.clone(), name.clone());
            self.names = Some(names);
        }
    }

    pub fn add_value_type(&mut self, entity_id: &String, value_type: ValueType) {
        if let Some(value_types) = &mut self.value_types {
            value_types.insert(entity_id.clone(), value_type);
        } else {
            let mut value_types = HashMap::new();
            value_types.insert(entity_id.clone(), value_type);
            self.value_types = Some(value_types);
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Type {
    /// The Id of the type
    pub entity_id: String,
    /// The space that this type was created in
    pub space: String,
    /// An array of Attributes that are attributes of this type
    pub attributes: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Attribute {
    pub entity_id: String,
    pub name: String,
    pub value: ValueType,
}
