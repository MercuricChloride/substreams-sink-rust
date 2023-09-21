CREATE TABLE IF NOT EXISTS "cursors" (id INTEGER, cursor TEXT);
INSERT INTO "cursors" (id, cursor) VALUES ('0', '');

COMMENT ON TABLE "public"."cursors" IS E'@name coolCursors';

DROP TABLE IF EXISTS "public"."entities";
CREATE TABLE IF NOT EXISTS entities ( id TEXT UNIQUE, name TEXT);

INSERT INTO entities (id, name) VALUES ('1', 'End Homelessness in SF');
INSERT INTO entities (id, name) VALUES ('2', 'Release a population of a million crickets on the moon');
INSERT INTO entities (id, name) VALUES ('3', 'Make SF safer');
INSERT INTO entities (id, name) VALUES ('4', 'Get a million crickets');
INSERT INTO entities (id, name) VALUES ('5', 'Crickets have the ability to survive on the moon');

DROP SCHEMA IF EXISTS "0xSomeSpace" CASCADE;
CREATE SCHEMA IF NOT EXISTS "0xSomeSpace";
DROP SCHEMA IF EXISTS "0xAnotherSpace" CASCADE;
CREATE SCHEMA IF NOT EXISTS "0xAnotherSpace";

CREATE TABLE IF NOT EXISTS "0xSomeSpace"."example-goal-id" (id TEXT UNIQUE, "entity-id" TEXT UNIQUE REFERENCES "public"."entities"(id));
INSERT INTO "0xSomeSpace"."example-goal-id" (id, "entity-id") VALUES ('1', '1');
INSERT INTO "0xSomeSpace"."example-goal-id" (id, "entity-id") VALUES ('2', '2');
COMMENT ON TABLE "0xSomeSpace"."example-goal-id" IS E'@name Goal';

CREATE TABLE IF NOT EXISTS "0xSomeSpace"."example-subgoals-id" (id TEXT UNIQUE, "entity-id" TEXT UNIQUE REFERENCES "public"."entities"(id), "parent_goal" TEXT REFERENCES "0xSomeSpace"."example-goal-id"(id));
COMMENT ON TABLE "0xSomeSpace"."example-subgoals-id" IS E'@name Subgoal';
INSERT INTO "0xSomeSpace"."example-subgoals-id" (id, "entity-id", "parent_goal") VALUES ('3', '3', '1');
INSERT INTO "0xSomeSpace"."example-subgoals-id" (id, "entity-id", "parent_goal") VALUES ('4', '4', '2');

CREATE TABLE IF NOT EXISTS "0xAnotherSpace"."example-claim-id" (id TEXT UNIQUE, "entity-id" TEXT UNIQUE REFERENCES "public"."entities"(id), supporting_goal TEXT REFERENCES "0xSomeSpace"."example-goal-id"(id));
COMMENT ON TABLE "0xAnotherSpace"."example-claim-id" IS E'@name Claim';

INSERT INTO "0xAnotherSpace"."example-claim-id" (id, "entity-id", "supporting_goal") VALUES ('5', '5', '2');
