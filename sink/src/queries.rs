pub fn table_comment_string(space: &str, entity_id: &str, entity_name: &str) -> String {
    format!(
        "DO $$
BEGIN
   IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = '{space}' AND tablename = '{entity_id}') THEN
      COMMENT ON TABLE \"{space}\".\"{entity_id}\" IS E'@name {entity_name}entity';
   END IF;
END $$;
",
                        space = space,
                        entity_id = entity_id,
                        entity_name = escape(entity_name)
                    )
}

pub fn create_attribute_table_string(entity_id: &str) -> String {
    format!(
        "
CREATE TABLE IF NOT EXISTS \"attributes\".\"{entity_id}\" (
    id TEXT PRIMARY KEY,
    entity_id TEXT NOT NULL REFERENCES \"public\".\"entities\"(id),
    attribute_of TEXT NOT NULL REFERENCES \"public\".\"entities\"(id)
);",
        entity_id = entity_id
    )
}

pub fn column_name_statement(space: &str, table_name: &str, entity_id: &str, name: &str) -> String {
    format!(
        "COMMENT ON COLUMN \"{space}\".\"{table_name}\".\"attr_{entity_id}\" IS E'@name {name}';",
    )
}

pub fn tables_query(space: &str, entity_id: &str) -> String {
    format!(
        "SELECT table_name FROM information_schema.columns WHERE table_schema = '{space}' AND column_name = 'attr_{entity_id}';"
    )
}

/// strips all non alphanumeric characters that aren't spaces
pub fn escape(input: &str) -> String {
    input
        .chars()
        .filter(|c| c.is_alphanumeric() || c.is_whitespace())
        .filter(|c| c != &'\n')
        .collect::<String>()
}

pub fn triple_exists_string(entity: &str, attribute: &str, value: &str) -> String {
    format!("SELECT EXISTS(SELECT * from \"public\".\"triples\" WHERE \"entity_id\" = '{entity}' AND \"attribute_id\" = '{attribute}' AND \"value_id\" = '{value}');")
}

pub fn table_exists_statement(table_schema: &str, table_name: &str) -> String {
    format!(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = '{table_schema}' AND table_name = '{table_name}');",
            )
}

pub fn relation_column_add_statement(
    table_schema: &str,
    table_name: &str,
    relation_schema: &str,
    relation_table: &str,
) -> String {
    format!(
                    "ALTER TABLE \"{table_schema}\".\"{table_name}\" ADD COLUMN IF NOT EXISTS \"attr_{relation_table}\" TEXT REFERENCES \"{relation_schema}\".\"{relation_table}\"(id);",
                )
}

pub fn column_rename_statement(
    table_schema: &str,
    table_name: &str,
    relation_table: &str,
    new_column_name: &str,
) -> String {
    format!(
                        "COMMENT ON COLUMN \"{table_schema}\".\"{table_name}\".\"attr_{relation_table}\" IS E'@name {new_column_name}';",
                    )
}

pub fn text_column_add_statement(
    table_schema: &str,
    table_name: &str,
    attribute_id: &str,
) -> String {
    format!(
        "ALTER TABLE \"{table_schema}\".\"{table_name}\" ADD COLUMN IF NOT EXISTS \"attr_{attribute_id}\" TEXT;",
    )
}

pub fn table_create_statement(table_schema: &str, table_name: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS \"{table_schema}\".\"{table_name}\" (
                id TEXT PRIMARY KEY,
                entity_id TEXT NOT NULL REFERENCES \"public\".\"entities\"(id)
            );",
    )
}

pub fn table_disable_statement(table_schema: &str, table_name: &str) -> String {
    format!("ALTER TABLE \"{table_schema}\".\"{table_name}\" DISABLE TRIGGER ALL;")
}
