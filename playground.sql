
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
  id text  
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


CREATE TYPE public.type_goal AS (
  id text,  
  name text,
  description text,	
  subgoals public.entities[] 
);

CREATE FUNCTION public.goals() RETURNS SETOF public.type_goal AS $$
BEGIN
  RETURN QUERY
  SELECT e.id::text, e.name::text, e.description::text, ROW(e.*)::public.entities
	FROM entities e
	JOIN triples t ON e.id = t.entity_id
	WHERE t.attribute_id = 'type'
	AND t.value_id = '11ffed68-a58b-42c6-903a-d245e7a46149';
END;
$$ LANGUAGE plpgsql STRICT STABLE;
