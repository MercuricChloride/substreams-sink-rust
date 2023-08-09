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
        self.save();
    }

    pub fn add_type(&mut self, type_name: &String, fields: Option<Vec<Field>>) {
        if let Some(types) = &mut self.types {
            types.insert(
                type_name.clone(),
                Type {
                    name: type_name.clone(),
                    fields: fields.unwrap_or_default(),
                },
            );
        } else {
            let mut types = HashMap::new();
            types.insert(
                type_name.clone(),
                Type {
                    name: type_name.clone(),
                    fields: fields.unwrap_or_default(),
                },
            );
            self.types = Some(types);
        }
        self.save();
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Type {
    pub name: String,
    pub fields: Vec<Field>,
}

// TODO Make this an enum
#[derive(Debug, Serialize, Deserialize)]
pub struct Field {
    pub name: String,
    pub type_name: String,
}
