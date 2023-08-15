use std::{collections::HashMap, fs::File};

use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Persist {
    /// The cursor to start from
    pub cursor: Option<String>,
    /// A map from entity id -> fields
    pub types: Option<HashMap<String, Type>>,
    /// An array of spaces created
    pub spaces: Option<Vec<String>>,
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
                    fields: vec![],
                },
            );
        } else {
            let mut types = HashMap::new();
            types.insert(
                entity_id.clone(),
                Type {
                    entity_id: entity_id.clone(),
                    space: space.clone(),
                    fields: vec![],
                },
            );
            self.types = Some(types);
        }
    }

    /// entity_id is the id of the entity that the attribute was added to
    pub fn add_attribute(&mut self, entity_id: &String, attribute_id: &String, space: &String) {
        if let Some(types) = &mut self.types {
            if let Some(type_) = types.get_mut(entity_id) {
                type_.fields.push(attribute_id.clone());
            } else {
                self.add_type(entity_id, space);
                self.add_attribute(entity_id, attribute_id, space);
            }
        } else {
            panic!("Tried to add an attribute before types was initialized in the persist");
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Type {
    /// The Id of the type
    pub entity_id: String,
    /// The space that this type was created in
    pub space: String,
    /// An array of entity ids that are fields of this type
    pub fields: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Field {
    pub entity_id: String,
}
