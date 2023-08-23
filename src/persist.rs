use std::{collections::HashMap, fs::File};

use serde::{Deserialize, Serialize};

use crate::triples::ValueType;

#[derive(Debug, Serialize, Deserialize)]
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

impl Default for Persist {
    fn default() -> Self {
        Self {
            types: Some(HashMap::new()),
            spaces: Some(vec![]),
            attributes: Some(HashMap::new()),
            names: Some(HashMap::new()),
            value_types: Some(HashMap::new()),
        }
    }
}

impl Persist {
    // pub fn from_file(path: Option<String>) -> Self {
    //     let file;

    //     if let Some(path) = path {
    //         file = File::open(path).unwrap();
    //     } else {
    //         file = File::open("persist.json").unwrap();
    //     };

    //     serde_json::from_reader(file).unwrap()
    // }

    // pub fn save_to_file(&self, path: String) {
    //     let file = File::create(path).unwrap();
    //     serde_json::to_writer(file, &self).unwrap();
    // }

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
        // get the attributes map
        let attributes = self.attributes.as_mut().unwrap();

        // get the types map
        let types = self.types.as_mut().unwrap();

        // get the names map
        let names = self.names.as_mut().unwrap();

        // get the type of the attribute_id, because all attributes should be a type
        let _ = types
            .get_mut(attribute_id)
            .expect("No type found for the attribute");

        // get the type of the entity_id, because it too should be a type
        let entity_type = types
            .get_mut(entity_id)
            .expect("No type found for the entity");

        // get the name of the attribute
        let attribute_name = names
            .get(attribute_id)
            .expect("No name found for the attribute");

        // create the attribute
        let attribute = Attribute {
            entity_id: attribute_id.clone(),
            name: attribute_name.clone(),
            space: Some(space.clone()),
            value: None,
        };

        // add the attribute to the attributes map
        attributes.insert(attribute_id.clone(), attribute);

        // add the attribute to the entity type
        entity_type.attributes.push(attribute_id.clone());
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
        if self.value_types.is_none() {
            self.value_types = Some(HashMap::new());
        }
        if self.attributes.is_none() {
            self.attributes = Some(HashMap::new());
        }

        let value_types = self.value_types.as_mut().unwrap();

        let attributes = self.attributes.as_mut().unwrap();

        // if the entity_id is an attribute, we will store the value type as a value type for the attribute
        if let Some(_) = attributes.get(entity_id) {
            value_types.insert(entity_id.clone(), value_type);
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
    pub value: Option<ValueType>,
    pub space: Option<String>,
}
