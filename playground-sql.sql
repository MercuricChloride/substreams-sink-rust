--==========--
-- SNIPPETS --
--==========--
create type foo_with_stuff as (
  foo_id int,
  stuff int
);
comment on type foo_with_stuff is '@foreignKey (foo_id) references foos';

CREATE TYPE shop_with_extra_field (shop shop, extra_field text);
CREATE FUNCTION shops_with_extra_field()
  RETURNS SETOF shop_with_extra_field AS
$func$
SELECT s, 'extra field'::text FROM shop s;
$func$  LANGUAGE sql;

-- Entity IDs:
-- Type: d7ab4092-0ab5-441e-88c3-5c27952de773
-- Subgoal: 377ac7e8-18ab-443c-bc26-29ff04745f99
-- Goal: 11ffed68-a58b-42c6-903a-d245e7a46149

--==========--
-- START V1 --
--==========--
CREATE TYPE public.type_goal AS (
  name text,
  description text,	
  id text
);

CREATE FUNCTION public.goals() RETURNS SETOF public.type_goal AS $$
BEGIN
  RETURN QUERY
  SELECT e.name::text, e.description::text, e.id::text
  FROM entities e
  WHERE e.id = '8bfe812b-e34c-473a-b9f1-bd682098c941';
END;
$$ LANGUAGE plpgsql STRICT STABLE;

drop function public.goals;
drop type public.type_goal;

--==========--
-- START V2 --
--==========--
SELECT e.id, e.name, e.description
	FROM entities e
	JOIN triples t ON e.id = t.entity_id
	WHERE t.attribute_id = 'type'
	AND t.value_id = '11ffed68-a58b-42c6-903a-d245e7a46149';


--==========--
-- START V3 --
--==========--    
CREATE TYPE public.type_goal AS (
  id text,  
  description text,	
  name text,
);

CREATE FUNCTION public.goals() RETURNS SETOF public.type_goal AS $$
BEGIN
  RETURN QUERY
  SELECT e.id::text, e.name::text, e.description::text, ROW(s.*)::public.entities
	FROM entities e
	JOIN triples t ON e.id = t.entity_id
	WHERE t.attribute_id = 'type'
	AND t.value_id = '11ffed68-a58b-42c6-903a-d245e7a46149';
END;
$$ LANGUAGE plpgsql STRICT STABLE;


--==========--
-- START V4 --
--==========--  


-- Returns all the types from the entities table 
CREATE OR REPLACE FUNCTION allTypes()
RETURNS SETOF entities AS $$
BEGIN
  RETURN QUERY
  SELECT e.*
  FROM entities e
  WHERE e.is_type = true;
END;
$$ LANGUAGE plpgsql STRICT STABLE;
comment on function allTypes() is E'@filterable';

-- Returns all the entity attributes for an entity
CREATE OR REPLACE FUNCTION entities_attributes(e_row entities)
RETURNS SETOF entities AS $$
BEGIN
  RETURN QUERY
  
  SELECT e.*
	FROM entities e
	WHERE e.id IN (
	    SELECT t.value_id
	    FROM triples t
	    WHERE t.entity_id = e_row.id and t.attribute_id = '01412f83-8189-4ab1-8365-65c7fd358cc1'
	);
 END;
$$ LANGUAGE plpgsql STRICT STABLE;


-- allClaims, filterable and sortable
CREATE OR REPLACE FUNCTION "entities-fa8e8e54-f742-4c00-b73c-05adee2b4545"() 
RETURNS SETOF entities AS $$
BEGIN
  RETURN QUERY
  SELECT e.*
	FROM entities e
	WHERE e.id IN (
	    SELECT t.entity_id
	    FROM triples t
	    WHERE  t.attribute_id = 'type' and t.value_id = 'd7ab4092-0ab5-441e-88c3-5c27952de773'
	);
 END;
END;
$$ LANGUAGE plpgsql STRICT STABLE;
comment on function "entities-fa8e8e54-f742-4c00-b73c-05adee2b4545"() is E'@name allClaims
@sortable
@filterable';

-- adds opposingArguments attribute to entity types
CREATE OR REPLACE FUNCTION "entity_types_0c0a2a95-1928-4ec4-876d-cc04075b7927"(et entity_types) RETURNS SETOF public.triples AS $$
BEGIN
  RETURN QUERY
  SELECT *
	FROM public.triples t
	WHERE t.entity_id = et.entity_id
  AND t.attribute_id = '0c0a2a95-1928-4ec4-876d-cc04075b7927';
END;
$$ LANGUAGE plpgsql STRICT STABLE;
comment on function "entity_types_0c0a2a95-1928-4ec4-876d-cc04075b7927"(et entity_types) is E'@fieldName opposingArguments';

CREATE INDEX idx_entity_attribute ON public.triples(entity_id, attribute_id);

--==========--
-- START V5 --
--==========--  

-- Returns all the types from the entities table 
CREATE OR REPLACE FUNCTION all_schema_types()
RETURNS SETOF entities AS $$
BEGIN
  RETURN QUERY
  SELECT e.*
        FROM entities e
        WHERE e.id IN (
            SELECT t.entity_id
            FROM triples t
            WHERE t.attribute_id = 'type' AND t.value_id = 'd7ab4092-0ab5-441e-88c3-5c27952de773'
        );
END;
$$ LANGUAGE plpgsql STRICT STABLE;
comment on function all_schema_types() is E'@filterable';

-- Returns all the types from the entities table - NOTE THAT "STRICT" MUST BE REMOVED FOR DEFAULT NULL TO WORK
CREATE OR REPLACE FUNCTION all_schema_type_entities(type_id text default null)
RETURNS SETOF entities AS $$
BEGIN
  RETURN QUERY
  SELECT e.*
        FROM entities e
        WHERE e.id IN (
            SELECT t.entity_id
            FROM triples t
            WHERE t.attribute_id = 'type'
            AND (type_id IS NULL OR t.value_id = type_id)
        );
END;
$$ LANGUAGE plpgsql STABLE;
COMMENT ON FUNCTION all_schema_type_entities(text) IS E'@filterable';

-- Computed Column "entities.types". Returns types of entities. 
CREATE OR REPLACE FUNCTION entities_types(e_row entities)
RETURNS SETOF entities AS $$
BEGIN
    RETURN QUERY
    SELECT e.*
    FROM entities e
    WHERE e.id IN (
        SELECT t.value_id
        FROM triples t
        WHERE t.entity_id = e_row.id AND t.attribute_id = 'type'
    );
END;
$$ LANGUAGE plpgsql STRICT STABLE;

-- Computed Column "entities.schema". Returns the schemas for each of the types associated with the entity
CREATE OR REPLACE FUNCTION entities_schema(e_row entities)
RETURNS SETOF entities AS $$
BEGIN
    -- Using CTE to first fetch all types of the given entity
    RETURN QUERY 
    WITH entity_types AS (
        SELECT t.value_id AS type_id
        FROM triples t
        WHERE t.entity_id = e_row.id AND t.attribute_id = 'type'
    ),
    type_attributes AS (
        -- For each type, fetch the associated attributes
        SELECT DISTINCT t.value_id AS attribute_id
        FROM entity_types et
        JOIN triples t ON t.entity_id = et.type_id AND t.attribute_id = '01412f83-8189-4ab1-8365-65c7fd358cc1'
    )
    SELECT e.*
    FROM entities e
    JOIN type_attributes ta ON e.id = ta.attribute_id;
END;
$$ LANGUAGE plpgsql STRICT STABLE;


-- Custom query. Returns all entities with a type of "Claim"
CREATE OR REPLACE FUNCTION "entities-fa8e8e54-f742-4c00-b73c-05adee2b4545"() 
RETURNS SETOF entities AS $$
BEGIN
  RETURN QUERY
  SELECT e.*
  FROM entities e
  WHERE e.id IN (
      SELECT t.entity_id
      FROM triples t
      WHERE  t.attribute_id = 'type' AND t.value_id = 'fa8e8e54-f742-4c00-b73c-05adee2b4545'
  );
END;
$$ LANGUAGE plpgsql STRICT STABLE;
comment on function "entities-fa8e8e54-f742-4c00-b73c-05adee2b4545"() is E'@name allClaims
@sortable
@filterable';

-- Computed column "entity_types.opposing_arguments". 
CREATE OR REPLACE FUNCTION "entity_types_0c0a2a95-1928-4ec4-876d-cc04075b7927"(et entity_types) RETURNS SETOF public.triples AS $$
BEGIN
  RETURN QUERY
  SELECT *
	FROM public.triples t
	WHERE t.entity_id = et.entity_id
  AND t.attribute_id = '0c0a2a95-1928-4ec4-876d-cc04075b7927';
END;
$$ LANGUAGE plpgsql STRICT STABLE;
comment on function "entity_types_0c0a2a95-1928-4ec4-876d-cc04075b7927"(et entity_types) is E'@fieldName opposingArguments';

CREATE INDEX idx_entity_attribute ON public.triples(entity_id, attribute_id);
CREATE INDEX idx_entity_attribute_value_id ON public.triples(entity_id, attribute_id, value_id);
