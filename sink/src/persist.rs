use std::{collections::HashMap, fs::File};

use futures03::future::join_all;
use serde::{Deserialize, Serialize};
use tokio_postgres::Client;

use crate::triples::ValueType;

#[derive(Serialize, Deserialize)]
pub struct Persist {
    #[serde(default)]
    /// A stack of all the queries to the DB we need to do
    pub tasks: Vec<String>,

    #[serde(default)]
    /// A stack of queries to the DB that may fail / be retried
    pub retry_tasks: Vec<String>,

    #[serde(default)]
    /// A map from entity id -> fields
    pub types: HashMap<String, Type>,

    #[serde(default)]
    /// A map from entity-id to address of a space
    pub spaces: HashMap<String, String>,

    #[serde(default)]
    /// A map from entity-id to a vector of subspaces
    pub subspaces: HashMap<String, Vec<String>>,

    #[serde(default)]
    /// A map from attribute id -> attribute
    pub attributes: HashMap<String, Attribute>,

    #[serde(default)]
    /// A map from entity id -> name
    pub names: HashMap<String, String>,

    #[serde(default)]
    /// A map from entity id -> value type
    pub value_types: HashMap<String, ValueType>,
}

impl Default for Persist {
    fn default() -> Self {
        Self {
            tasks: vec![],
            retry_tasks: vec![],
            types: HashMap::new(),
            spaces: HashMap::new(),
            subspaces: HashMap::new(),
            attributes: HashMap::new(),
            names: HashMap::new(),
            value_types: HashMap::new(),
        }
    }
}

// NOTE: I might want to add functions for loading and saving from a non-defaut file
impl Persist {
    pub fn open() -> Self {
        if let Ok(opened_file) = File::open("persist.json") {
            serde_json::from_reader(opened_file).unwrap()
        } else {
            let persist = Self::default();
            //persist.save();
            persist
        }
    }

    /// This function will process all of the tasks in the task queue, and then clear the queue
    pub async fn save(&mut self, client: &Client) {
        // process all tasks that are not in the retry queue
        join_all(self.tasks.iter().map(|x| client.execute(x, &[]))).await;

        self.tasks.clear();

        // process all tasks that are in the retry queue
        let retry_tasks = join_all(self.retry_tasks.iter().map(|x| client.execute(x, &[]))).await;

        let mut tasks_to_retry = vec![];

        // if any of the retry tasks failed, add them back to the task queue
        for (index, task) in retry_tasks.iter().enumerate() {
            if let Err(_) = task {
                tasks_to_retry.push(self.retry_tasks[index].clone());
            }
        }

        self.retry_tasks = tasks_to_retry;
    }

    pub fn add_space(&mut self, entity_id: String, space: String) {
        self.spaces.insert(entity_id, space);
    }

    pub fn add_subspace(&mut self, parent_space: String, child_space: String) {
        let subspaces = self.subspaces.entry(parent_space).or_insert(vec![]);
        if !subspaces.contains(&child_space) {
            subspaces.push(child_space);
        } else {
            println!("Subspace already exists");
        }
    }

    pub fn remove_subspace(&mut self, parent_space: String, child_space: String) {
        let subspaces = self.subspaces.entry(parent_space).or_insert(vec![]);
        if let Some(index) = subspaces.iter().position(|x| *x == child_space) {
            subspaces.remove(index);
        } else {
            println!("Subspace does not exist");
        }
    }

    pub fn add_type(&mut self, entity_id: &String, space: &String) {
        self.types.insert(
            entity_id.clone(),
            Type {
                entity_id: entity_id.clone(),
                space: space.clone(),
                attributes: vec![],
            },
        );
    }

    /// entity_id is the id of the entity that the attribute was added to
    pub fn add_attribute(&mut self, entity_id: &String, attribute_id: &String, space: &String) {
        let Self {
            attributes,
            types,
            names,
            ..
        } = self;

        // get the type of the attribute_id, because all attributes should be a type
        let _ = types
            .get(attribute_id)
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
        self.names.insert(entity_id.clone(), name.clone());
    }

    pub fn add_value_type(&mut self, entity_id: &String, value_type: ValueType) {
        // NOTE Should something be forced to be an attribute if it's a valueType?
        self.value_types.insert(entity_id.clone(), value_type);
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Type {
    /// The Id of the type
    pub entity_id: String,
    /// The space that this type was created in
    pub space: String,
    /// An array of entity-ids that are attributes of this type
    pub attributes: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Attribute {
    pub entity_id: String,
    pub name: String,
    pub value: Option<ValueType>,
    pub space: Option<String>,
}
