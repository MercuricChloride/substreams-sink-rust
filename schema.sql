CREATE TABLE IF NOT EXISTS "cursors" (id INTEGER, cursor TEXT);
INSERT INTO "cursors" (id, cursor) VALUES ('0', '');

COMMENT ON TABLE "public"."cursors" IS E'@name coolCursors';

CREATE TABLE IF NOT EXISTS spaces ( id TEXT UNIQUE, space TEXT);

CREATE TABLE IF NOT EXISTS entity_types ( id TEXT, space TEXT);

CREATE TABLE IF NOT EXISTS entity_names ( id TEXT UNIQUE, name TEXT, space TEXT);

CREATE TABLE IF NOT EXISTS entity_attributes ( id TEXT, belongs_to TEXT);

CREATE TABLE IF NOT EXISTS entity_value_types ( id TEXT UNIQUE, value_type TEXT, space TEXT);

CREATE TABLE IF NOT EXISTS subspaces ( id TEXT, value_type TEXT, space TEXT);
