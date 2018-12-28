/*
Projekt 'Grøn Registrering', 27/12/2018

Udarbejdet af
	Casper Bertelsen Jensen
	Have- og parkingeniør

	Kontakt: casperbj94@gmail.com



Licens:
This program is free software: you can redistribute it and/or modify it under the terms of the GNU General
Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
option) any later version.
This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See
the GNU General Public License for more details.



Grøn registrering er udviklet med henblik på at opbygge en struktur og funktionalitet til anvendelse i
organisationer, der styrer driften af større grønne områder.
Projektet er oprindeligt udviklet til Frederikssund Kommune i løbet af vinteren '16/'17, hvorefter jeg har arbejdet videre
med projektet, gjort det lettere at vedligeholde og gøre det mere generelt anvendeligt.
Projektet er oprindeligt udviklet til PostgreSQL v. 9.6. Denne nye version er udviklet og testet på v. 10 og sidenhen rettet til, så den også
er anvendelig på v. 9.6.
Projektet tager udgangspunkt i QGIS 3 som brugerflade og er således udviklet i retning mod at kunne fungere på netop denne platform.
*/


--
-- DROP SCHEMAS AND MISC
--

DROP SCHEMA IF EXISTS grunddata CASCADE;

DROP SCHEMA IF EXISTS basis CASCADE;

DROP SCHEMA IF EXISTS greg CASCADE;

DROP SCHEMA IF EXISTS skitse CASCADE;


---
--- SET
---

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;


--
-- REVOKE CONNECT
--

DO $$

BEGIN

	EXECUTE FORMAT( -- Revoke ability to CONNECT to the database from PUBLIC
		'REVOKE CONNECT
			ON DATABASE %s
		FROM PUBLIC',
		current_database()
	);

END $$;





--
-- INDIVIDUAL MODULES
--


-- Initial setup of schema custom
--
--
--
--


--
-- ALTER SCHEMAS
--

DO $BODY$

DECLARE

	_path text;

BEGIN

--
-- Existing search_path
--

	SHOW search_path INTO _path;

--
-- Check whether schema already exists
-- And whether or not the schema is a part of the search_path
--

	IF EXISTS(
		SELECT
			'1'
		FROM pg_catalog.pg_namespace
		WHERE nspname = 'custom'
	) AND NOT EXISTS(
		SELECT
			'1'
		WHERE 'custom' IN(SELECT regexp_split_to_table(_path, ', '))
	) THEN

--
-- If it isn't a part of search_path rename the schema
--

		ALTER SCHEMA custom
			RENAME TO custom_old;

	END IF;

END $BODY$;


--
-- ALTER SEARCH_PATH
--

DO $BODY$

DECLARE

	_path text;

BEGIN

--
-- Existing search_path
--

	SHOW search_path INTO _path;

--
-- If schema isn't a part of the search_path
--

	IF NOT EXISTS(
		SELECT
			'1'
		WHERE 'custom' IN(SELECT regexp_split_to_table(_path, ', '))
	) THEN

--
-- If public is part of search_path
-- Set custom right after public
--

		IF EXISTS(
			SELECT
				'1'
			WHERE 'public' IN(SELECT regexp_split_to_table(_path, ', '))
		) THEN

			SELECT
				regexp_replace(_path, 'public', 'public, custom')
			INTO _path;

			EXECUTE FORMAT(
				'ALTER DATABASE %s SET search_path = %s',
				current_database(), _path
			);

--
-- Otherwise set custom as first schema
--

		ELSE

			EXECUTE FORMAT(
				'ALTER DATABASE %s SET search_path = %s',
				current_database(), 'custom' || COALESCE(', ' || _path, '')
			);

		END IF;

	END IF;

END $BODY$;


--
-- CREATE EXTENSIONS
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;
COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language.';

CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public;
COMMENT ON EXTENSION postgis IS 'PostGIS geometry, geography, and raster spatial types and functions.';

CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public;
COMMENT ON EXTENSION "uuid-ossp" IS 'Generate universally unique identifiers (UUIDs).';

CREATE EXTENSION IF NOT EXISTS "hstore" WITH SCHEMA public;
COMMENT ON EXTENSION "hstore" IS 'Data type for storing sets of (key, value) pairs.';


--
-- SCHEMAS
--

DROP SCHEMA IF EXISTS custom CASCADE;

CREATE SCHEMA custom;
COMMENT ON SCHEMA custom IS 'General (custom) functionality.';


--
-- FUNCTIONS
--


-- DROP FUNCTION IF EXISTS custom.array_hierarchy(anyarray, text) CASCADE;

CREATE OR REPLACE FUNCTION custom.array_hierarchy(anyarray, separator text DEFAULT '')
	RETURNS ANYARRAY
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	_ret text[];

BEGIN

	WITH

--
-- Generate subscripts of array, include the array for each tuple
--

		cte1 AS(
			SELECT
				generate_subscripts($1, 1) AS _sub,
				$1 AS _val
		),

--
-- Extract first one element, then two elements and so on of the array
--

		cte2 AS(
			SELECT
				_sub,
				_val[1:_sub] AS _val
			FROM cte1
		),

--
-- UNNEST WITH ORDINALITY to keep order
--

		cte3 AS(
			SELECT
				_sub,
				t._val,
				t._order
			FROM cte2, UNNEST(_val) WITH ORDINALITY AS t(_val, _order)
		),



		cte4 AS(
			SELECT
				_sub,
				string_agg(_val::text, separator ORDER BY _order) AS _val
			FROM cte3
			GROUP BY _sub
		)

	SELECT
		array_agg(_val ORDER BY _sub)
	FROM cte4
	INTO _ret;


	RETURN _ret;

END $BODY$;

COMMENT ON FUNCTION custom.array_hierarchy(anyarray, text) IS 'Generate array hierarchies.';


-- DROP FUNCTION IF EXISTS custom.array_trim(anyarray, integer) CASCADE;

CREATE OR REPLACE FUNCTION custom.array_trim(anyarray, integer)
	RETURNS ANYARRAY
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	_ret text[];

BEGIN

	SELECT
		array_agg(val::text)
	FROM UNNEST($1) WITH ORDINALITY AS t(val, num)
	WHERE num <= $2 AND cardinality($1) >= $2
	INTO _ret;

	RETURN _ret;

END $BODY$;

COMMENT ON FUNCTION custom.array_trim(anyarray, integer) IS 'Trim an array to a number of elements';


-- DROP FUNCTION IF EXISTS custom.check_tbl(schema_name text, table_name text, column_name text) CASCADE;

CREATE OR REPLACE FUNCTION custom.check_tbl(schema_name text DEFAULT NULL, table_name text DEFAULT NULL, column_name text DEFAULT NULL)
	RETURNS BOOLEAN
	LANGUAGE plpgsql AS
$$

BEGIN

--
-- If name of column has been specified
-- Check for column
--

	IF
		$1 IS NOT NULL AND
		$2 IS NOT NULL AND
		$3 IS NOT NULL
	THEN

		IF EXISTS(SELECT
					'1'
				FROM pg_catalog.pg_attribute a
				LEFT JOIN pg_catalog.pg_class b ON a.attrelid = b.oid
				LEFT JOIN pg_catalog.pg_namespace c ON b.relnamespace = c.oid
				WHERE a.attnum > 0 AND c.nspname = $1 AND b.relname = $2 AND a.attname = $3
		) THEN

			RETURN TRUE;

		ELSE

			RETURN NULL;

		END IF;

--
-- If only name of schema and table has been specified
-- Check for table
--

	ELSIF
		$1 IS NOT NULL AND
		$2 IS NOT NULL
	THEN

		IF EXISTS(SELECT
					'1'
				FROM pg_catalog.pg_class a
				LEFT JOIN pg_catalog.pg_namespace b ON a.relnamespace = b.oid
				WHERE b.nspname = $1 AND a.relname = $2
		) THEN

			RETURN TRUE;

		ELSE

			RETURN NULL;

		END IF;

--
-- If only name of schema has been specified
-- Check for schema
--

	ELSIF $1 IS NOT NULL THEN

		IF EXISTS(SELECT
					'1'
				FROM pg_catalog.pg_namespace
				WHERE nspname = $1
		) THEN

			RETURN TRUE;

		ELSE

			RETURN NULL;

		END IF;

	ELSE

		RETURN NULL;

	END IF;

END $$;

COMMENT ON FUNCTION custom.check_tbl(schema_name text, table_name text, column_name text) IS 'Checks for existing objects:
If only schema is specified then schema,
if table.. then table and
if column.. then column.
Returns NULL if the object doesn''t exist';


-- DROP FUNCTION IF EXISTS custom.create_geom_value_trigger(target_schema text, taget_table text, target_column text, source_schema text, source_table text, source_column text, target_where_clause text, source_where_clause text, range_limit text) CASCADE;

CREATE OR REPLACE FUNCTION custom.create_geom_value_trigger(target_schema text, taget_table text, target_column text, source_schema text, source_table text, source_column text, target_where_clause text DEFAULT NULL, source_where_clause text DEFAULT NULL, range_limit text DEFAULT NULL)
	RETURNS VOID
	LANGUAGE plpgsql AS
$$

BEGIN

	EXECUTE FORMAT(
		$qt$
			DROP TRIGGER IF EXISTS a_get_val_from_%5$s__%6$s__%7$s_iu ON %1$I.%2$I; -- Drop trigger on target table


			DROP TRIGGER IF EXISTS a_ref_val_on_%1$s__%2$s__%3$s_a_iud ON %5$I.%6$I; -- Drop trigger on source table


			CREATE TRIGGER a_get_val_from_%5$s__%6$s__%7$s_iu BEFORE INSERT OR UPDATE -- Trigger name: f_get_val_from_'Name of source schema'__'Name of source table'__'Name of source column'
				ON %1$I.%2$I
				FOR EACH ROW EXECUTE PROCEDURE custom.geom_assign_value('%3$s', '%5$s', '%6$s', '%7$s' %8$s %4$s %9$s);


			CREATE TRIGGER a_ref_val_on_%1$s__%2$s__%3$s_a_iud AFTER INSERT OR UPDATE OR DELETE -- Trigger name: f_ref_val_on_'Name of target schema'__'Name of target table'__'Name of target column'
				ON %5$I.%6$I
				FOR EACH ROW EXECUTE PROCEDURE custom.geom_refresh_value('%7$s', '%1$s', '%2$s', '%3$s' %4$s %8$s %9$s);

			$qt$,
			$1, -- Target schema
			$2, -- Target table
			$3, -- Target column
			COALESCE(',''' || $7 || '''', ', NULL'), -- Target WHERE CLAUSE
			$4, -- Source schema
			$5, -- Source table
			$6, -- Source column
			COALESCE(',''' || $8 || '''', ', NULL'), -- Source WHERE CLAUSE
			COALESCE(',''' || $9 || '''', ', NULL') -- Range limit
	);

END $$;

COMMENT ON FUNCTION custom.create_geom_value_trigger(target_schema text, taget_table text, target_column text, target_where_clause text, source_schema text, source_table text, source_column text, source_where_clause text, range_limit text) IS 'Creates triggers for auto assign values based on geometry.';


-- DROP FUNCTION IF EXISTS custom.data_type_of(schema_name text, table_name text, column_name text) CASCADE;

CREATE OR REPLACE FUNCTION custom.data_type_of(schema_name text, table_name text, column_name text)
	RETURNS text
	LANGUAGE plpgsql AS
$$

DECLARE

	_ret text;

BEGIN

SELECT
	format_type(a.atttypid, a.atttypmod) AS typname
FROM pg_catalog.pg_attribute a
LEFT JOIN pg_catalog.pg_class b ON a.attrelid = b.oid
LEFT JOIN pg_catalog.pg_namespace c ON b.relnamespace = c.oid
WHERE (c.nspname, b.relname, a.attname) = ($1, $2, $3)
INTO _ret;

	RETURN _ret;

END $$;

COMMENT ON FUNCTION custom.data_type_of(schema_name text, table_name text, column_name text) IS 'Returns data type of a specified column.';


-- DROP FUNCTION IF EXISTS custom.frame_scale(public.geometry, nearest integer, frame_width numeric, frame_height numeric, margin numeric) CASCADE;

CREATE OR REPLACE FUNCTION custom.frame_scale(public.geometry, nearest integer, frame_width numeric, frame_height numeric, margin numeric)
	RETURNS INTEGER
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	_ret integer;

BEGIN

	WITH

	cte1 AS(
		SELECT
			ST_Length(
				ST_MakeLine(
					ST_MakePoint(
						ST_XMin(
							ST_Envelope($1)
						),
						ST_YMin(
							ST_Envelope($1)
						)
					), ST_MakePoint(
						ST_XMax(
							ST_Envelope($1)
						),
						ST_YMin(
							ST_Envelope($1)
						)
					)
				)
			) AS width,
			ST_Length(
				ST_MakeLine(
					ST_MakePoint(
						ST_XMin(
							ST_Envelope($1)
						),
						ST_YMin(
							ST_Envelope($1)
						)
					), ST_MakePoint(
						ST_XMin(
							ST_Envelope($1)
						),
						ST_YMax(
							ST_Envelope($1)
						)
					)
				)
			) AS height
	)

	SELECT
		CASE -- Frame to scale times margin, subtract remaining amount from 'nearest', add 'nearest'
			WHEN a.width / frame_width > a.height / frame_height
			THEN ((a.width * 1000 / frame_width) * (1 + margin / 100)) + nearest - (((a.width * 1000 / frame_width) * (1 + margin / 100))::numeric % nearest)
			ELSE ((a.height * 1000 / frame_height) * (1 + margin / 100)) - (((a.height * 1000 / frame_height) * (1 + margin / 100))::numeric % nearest) + nearest
		END
	FROM cte1 a
	INTO _ret;

	RETURN _ret;

END $BODY$;

COMMENT ON FUNCTION custom.frame_scale(public.geometry, nearest integer, frame_width numeric, frame_height numeric, margin numeric) IS 'Generates ''best fit'' sclae for atlas.';


-- DROP FUNCTION IF EXISTS custom.geometry_of(schema_name text, table_name text) CASCADE;

CREATE OR REPLACE FUNCTION custom.geometry_of(schema_name text, table_name text)
	RETURNS TABLE (
		geom_column text,
		srid integer,
		type text,
		geom_type integer
	)
	LANGUAGE plpgsql AS
$$

DECLARE

	_schema_postgis text;

BEGIN

--
-- Find schema of extension PostGIS
--

	SELECT
		b.nspname::text
	FROM pg_catalog.pg_extension a
	LEFT JOIN pg_catalog.pg_namespace b ON a.extnamespace = b.oid
	WHERE extname = 'postgis'
	INTO _schema_postgis;

--
-- Select information for the given table from metadata-table
--

	RETURN QUERY
	EXECUTE FORMAT(
		$qt$
			SELECT
				f_geometry_column::text,
				srid,
				type::text,
				CASE
					WHEN type ILIKE '%%POLYGON'
					THEN 2
					WHEN type ILIKE '%%LINESTRING'
					THEN 1
					WHEN type ILIKE '%%POINT'
					THEN 0
				END AS geom_type
			FROM %s.geometry_columns
			WHERE f_table_schema = '%s' AND f_table_name = '%s'
		$qt$, _schema_postgis, $1, $2
	);

END $$;

COMMENT ON FUNCTION custom.geometry_of(schema_name text, table_name text) IS 'Finds geometry information for a given table.';


-- DROP FUNCTION IF EXISTS custom.multiply_f(float, float) CASCADE;

CREATE FUNCTION custom.multiply_f(float, float)
	RETURNS float 
	LANGUAGE sql
	IMMUTABLE STRICT AS
$$

	SELECT $1 * $2;

$$;

CREATE AGGREGATE custom.multiply (basetype = float, sfunc = custom.multiply_f, stype = float, initcond = 1);


-- DROP FUNCTION IF EXISTS custom.prefix_col(_input text, schema_name text, table_name text, _prefix text, separator text) CASCADE;

CREATE OR REPLACE FUNCTION custom.prefix_col(_input text, schema_name text, table_name text, _prefix text, separator text DEFAULT ' ')
	RETURNS text
	LANGUAGE plpgsql AS
$$

DECLARE

	_ret text;

BEGIN

	WITH

		cte1 AS(
			SELECT
				COALESCE(SUBSTRING(val, '^"(.*)"$'), val) AS val, -- Removing quotes if they appear both at the start and end
				num
			FROM regexp_split_to_table($1, $5) WITH ORDINALITY t(val, num)
		)

	SELECT
		string_agg(
			CASE
				WHEN custom.check_tbl($2, $3, a.val)
				THEN $4 || quote_ident(val) -- Apply _prefix, column as identifier
				ELSE val
			END, $5 ORDER BY a.num
		)
	FROM cte1 a
	INTO _ret;

	RETURN _ret;

END $$;

COMMENT ON FUNCTION custom.prefix_col(_input text, schema_name text, table_name text, _prefix text, separator text) IS 'Find and apply prefix to columns in a text string,
e.g ''xx = aa AND yy = bb'' can become ''NEW.xx = aa AND NEW.yy = bb'' if xx and yy are columns found in a specified table.';


-- DROP FUNCTION IF EXISTS custom.rem_parts(public.geometry, _size numeric) CASCADE;

CREATE OR REPLACE FUNCTION custom.rem_parts(public.geometry, _size numeric)
	RETURNS GEOMETRY
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	_ret public.geometry;

BEGIN

	WITH

		cte1 AS(
			SELECT
				(public.ST_Dump($1)).geom
		)

	SELECT
		ST_Multi(
			ST_Union(geom)
		)
	FROM cte1
	WHERE public.ST_Area(geom) >= _size
	INTO _ret;

	RETURN _ret;

END $BODY$;

COMMENT ON FUNCTION custom.rem_parts(public.geometry, _size numeric) IS 'Removes parts of a geometry where the area is less than a certain value.';


-- DROP FUNCTION IF EXISTS custom.auto_delete(schema_name text, table_name text) CASCADE;

CREATE OR REPLACE FUNCTION custom.auto_delete(schema_name text, table_name text)
	RETURNS text
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	_ret text;

BEGIN

	SELECT
		FORMAT(
			$$
				DELETE
					FROM %1$I.%2$I
				WHERE (%3$s) = (%4$s)
			$$,
			schema_name,
			table_name,
			custom.primary_key(schema_name, table_name, _sufix := '::text'), -- Casting columns to text for comparability
			custom.primary_key(schema_name, table_name, '$1.', '::text')
		)
	INTO _ret;

	RETURN _ret;

END $BODY$;

COMMENT ON FUNCTION custom.auto_delete(schema_name text, table_name text) IS 'SQL: DELETE from table.';


-- DROP FUNCTION IF EXISTS custom.auto_insert(into_schema text, into_table text, from_schema text, from_table text) CASCADE;

CREATE OR REPLACE FUNCTION custom.auto_insert(into_schema text, into_table text, from_schema text DEFAULT NULL, from_table text DEFAULT NULL)
	RETURNS text
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	_ret text;

BEGIN

	SELECT
		FORMAT(
			$$
				INSERT INTO %I.%I(
					%s
				) VALUES(
					%s
				);
			$$,
			into_schema,
			into_table,
			string_agg(attname, E',\n\t\t\t\t\t' ORDER BY attnum), -- Tabs for readability when executing
			string_agg(
				COALESCE(
					'COALESCE($1.' || attname || ', ' || adsrc || ')', '$1.' || attname -- Using DEFAULT-values if NULL
				), E',\n\t\t\t\t\t' ORDER BY attnum
			)
		)
	FROM custom.common_columns(into_schema, into_table, COALESCE(from_schema, into_schema), COALESCE(from_table, into_table))
	INTO _ret;

	RETURN _ret;

END $BODY$;

COMMENT ON FUNCTION custom.auto_insert(into_schema text, into_table text, from_schema text, from_table text) IS 'SQL: INSERT into table.';


-- DROP FUNCTION IF EXISTS custom.auto_update(schema_name text, table_name text, _columns text[]) CASCADE;

CREATE OR REPLACE FUNCTION custom.auto_update(schema_name text, table_name text, _columns text[])
	RETURNS text
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	_ret text;

BEGIN

--
-- Update columns based on input from comparison
--

	SELECT
		FORMAT(
			$$
				UPDATE %1$I.%2$I
					SET
						%3$s
				WHERE (%4$s) = (%5$s)
			$$,
			schema_name,
			table_name,
			string_agg(_col || ' = $1.' || _col, E',\n\t\t\t\t\t\t'), -- Tabs for readability when executing
			custom.primary_key(schema_name, table_name, _sufix := '::text'), -- Casting columns to text for comparability
			custom.primary_key(schema_name, table_name, '$2.', '::text')
		)
	FROM UNNEST(_columns) AS t(_col)
	INTO _ret;

	RETURN _ret;

END $BODY$;

COMMENT ON FUNCTION custom.auto_update(schema_name text, table_name text, _columns text[]) IS 'SQL: UPDATE columns.';


-- DROP FUNCTION IF EXISTS custom.auto_update_columns(into_schema text, into_table text, from_schema text, from_table text) CASCADE;

CREATE OR REPLACE FUNCTION custom.auto_update_columns(into_schema text, into_table text, from_schema text DEFAULT NULL, from_table text DEFAULT NULL)
	RETURNS TEXT
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	_ret text;

BEGIN

	SELECT
		FORMAT(
			$$
				WITH

--
-- Convert OLD/NEW values into rows
-- As well as the actual column names
--

					cte1 AS(
						SELECT
							UNNEST(ARRAY[
								%1$s
							]) AS _old,
							UNNEST(ARRAY[
								%2$s
							]) AS _new,
							UNNEST(ARRAY[
								%3$s
							]) AS _col
					)

--
-- Aggregate column names of any columns where OLD/NEW doesn't match up
-- DISTINCT is used due to NULL-values
--

				SELECT
					array_agg(_col)
				FROM cte1
				WHERE _old IS DISTINCT FROM _new
			$$,
			string_agg('$2.' || attname || '::text', E',\n\t\t\t\t\t\t\t\t' ORDER BY attnum), -- Tabs for readability when executing
			string_agg('$1.' || attname || '::text', E',\n\t\t\t\t\t\t\t\t' ORDER BY attnum),
			string_agg(E'''' || attname || '''::text', E',\n\t\t\t\t\t\t\t\t' ORDER BY attnum)
		)
	FROM custom.common_columns(into_schema, into_table, COALESCE(from_schema, into_schema), COALESCE(from_table, into_table))
	INTO _ret;

	RETURN _ret;

END $BODY$;

COMMENT ON FUNCTION custom.auto_update_columns(into_schema text, into_table text, from_schema text, from_table text) IS 'SQL: Find columns to update.';


-- DROP FUNCTION IF EXISTS custom.common_columns(base_schema text, base_table text, compare_schema text, compare_table text) CASCADE;

CREATE OR REPLACE FUNCTION custom.common_columns(base_schema text, base_table text, compare_schema text, compare_table text)
	RETURNS TABLE(
		attnum int,
		attname text,
		adsrc text
	)
	LANGUAGE plpgsql AS
$BODY$

BEGIN

	RETURN QUERY
	SELECT
		a.attnum::int,
		quote_ident(a.attname) AS attname,
		COALESCE(d.adsrc, e.typdefault) AS adsrc -- Column default (if any), type default (if any), e.g. domains
	FROM pg_catalog.pg_attribute a
	LEFT JOIN pg_catalog.pg_class b ON a.attrelid = b.oid
	LEFT JOIN pg_catalog.pg_namespace c ON b.relnamespace = c.oid
	LEFT JOIN pg_catalog.pg_attrdef d ON (d.adrelid, d.adnum) = (b.oid, a.attnum)
	LEFT JOIN pg_catalog.pg_type e ON a.atttypid = e.oid
	WHERE
		a.attnum > 0 AND -- Actual columns
		a.attisdropped IS FALSE AND -- Rows still appear even when individual columns has been dropped
		(c.nspname, b.relname) = ($1, $2) AND -- Schema/Table-specific
		a.attname IN( -- Intersection with columns from another table
			SELECT
				a.attname
			FROM pg_catalog.pg_attribute a
			LEFT JOIN pg_catalog.pg_class b ON a.attrelid = b.oid
			LEFT JOIN pg_catalog.pg_namespace c ON b.relnamespace = c.oid
			WHERE a.attnum > 0 AND
			a.attisdropped IS FALSE AND
			(c.nspname, b.relname) = ($3, $4)
		)
	ORDER BY a.attnum;

END $BODY$;

COMMENT ON FUNCTION custom.common_columns(base_schema text, base_table text, compare_schema text, compare_table text) IS 'Columns in common between tables/views based on the name of the column';


-- DROP FUNCTION IF EXISTS custom.create_auto_trigger(view_schema text, view_name text, table_schema text, table_name text) CASCADE;

CREATE OR REPLACE FUNCTION custom.create_auto_trigger(view_schema text, view_name text, table_schema text, table_name text)
	RETURNS VOID
	LANGUAGE plpgsql AS
$BODY$

BEGIN

	EXECUTE FORMAT(
		$$
			CREATE TRIGGER zzz_%2$s_iud INSTEAD OF INSERT OR DELETE OR UPDATE
			ON %1$I.%2$I
			FOR EACH ROW
			EXECUTE PROCEDURE custom.auto_update(%3$s, %4$s);
		$$, $1, $2, $3, $4
	);

END $BODY$;

COMMENT ON FUNCTION custom.create_auto_trigger(view_schema text, view_name text, table_schema text, table_name text) IS 'Creates a trigger with the necessary parameters to enable automatic update of views.';


-- DROP FUNCTION IF EXISTS custom.primary_key(schema_name text, table_name text, _prefix text, _sufix text) CASCADE;

CREATE OR REPLACE FUNCTION custom.primary_key(schema_name text, table_name text, _prefix text DEFAULT '', _sufix text DEFAULT '')
	RETURNS text
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	_ret text;

BEGIN

	WITH

		cte1 AS(
			SELECT
				UNNEST(a.conkey) AS conkey
			FROM pg_catalog.pg_constraint a
			LEFT JOIN pg_catalog.pg_class b ON a.conrelid = b.oid
			LEFT JOIN pg_catalog.pg_namespace c ON b.relnamespace = c.oid
			WHERE (c.nspname, b.relname, a.contype) = ($1, $2, 'p')
		)

	SELECT
		string_agg(_prefix || quote_ident(a.attname) || _sufix, ',')
	FROM pg_catalog.pg_attribute a
	LEFT JOIN pg_catalog.pg_class b ON a.attrelid = b.oid
	LEFT JOIN pg_catalog.pg_namespace c ON b.relnamespace = c.oid
	WHERE (c.nspname, b.relname) = ($1, $2) AND attnum IN(SELECT conkey FROM cte1)
	INTO _ret;

	RETURN _ret;

END $BODY$;

COMMENT ON FUNCTION custom.primary_key(schema_name text, table_name text, _prefix text, _sufix text) IS 'Lists PK column names of the specified table.';


-- DROP FUNCTION IF EXISTS custom.convert_hex_rgb(hex_code text) CASCADE;

CREATE OR REPLACE FUNCTION custom.convert_hex_rgb(hex_code text)
	RETURNS TEXT
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	_ret text;

BEGIN

	SELECT
		('x'||substr(hex_code,2,2))::bit(8)::int ||','||
		('x'||substr(hex_code,4,2))::bit(8)::int ||','||
		('x'||substr(hex_code,6,2))::bit(8)::int
	INTO _ret;

	RETURN _ret;

END $BODY$;

COMMENT ON FUNCTION custom.convert_hex_rgb(hex_code text) IS 'Converts hex colour (#0000FF) to RGB (0,0,255).';


-- DROP FUNCTION IF EXISTS custom.convert_rgb_hex(rgb text) CASCADE;

CREATE OR REPLACE FUNCTION custom.convert_rgb_hex(rgb text)
	RETURNS TEXT
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	_ret text;

BEGIN

	SELECT
		'#' ||
		to_hex(SUBSTRING($1, '(.+),.+,.+')) ||
		to_hex(SUBSTRING($1, '.+,(.+),.+')) ||
		to_hex(SUBSTRING($1, '.+,.+,(.+)'))
	INTO _ret;

	RETURN _ret;

END $BODY$;

COMMENT ON FUNCTION custom.convert_rgb_hex(rgb text) IS 'Converts RGB (0,0,255) to hex colour (#0000FF)).';


-- DROP FUNCTION IF EXISTS custom.random_hex() CASCADE;

CREATE OR REPLACE FUNCTION custom.random_hex()
	RETURNS TEXT
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	_ret text;

BEGIN

	WITH
		conversion(val, code) AS( -- Conversion af numbers from 10-15 to corresponding letters
			VALUES
				(10, 'A'),
				(11, 'B'),
				(12, 'C'),
				(13, 'D'),
				(14, 'E'),
				(15, 'F')
		),

		cte1 AS( -- Create six random numbers from 0-15
			SELECT
				ROUND(random()::numeric * 15) AS hex1,
				ROUND(random()::numeric * 15) AS hex2,
				ROUND(random()::numeric * 15) AS hex3,
				ROUND(random()::numeric * 15) AS hex4,
				ROUND(random()::numeric * 15) AS hex5,
				ROUND(random()::numeric * 15) AS hex6
		)

		SELECT
			'#' ||
			CASE
				WHEN hex1 >= 10
				THEN (SELECT code FROM conversion WHERE hex1 = val)
				ELSE hex1::text
			END ||
			CASE
				WHEN hex2 >= 10
				THEN (SELECT code FROM conversion WHERE hex2 = val)
				ELSE hex2::text
			END ||
			CASE
				WHEN hex3 >= 10
				THEN (SELECT code FROM conversion WHERE hex3 = val)
				ELSE hex3::text
			END ||
			CASE
				WHEN hex4 >= 10
				THEN (SELECT code FROM conversion WHERE hex4 = val)
				ELSE hex4::text
			END ||
			CASE
				WHEN hex5 >= 10
				THEN (SELECT code FROM conversion WHERE hex5 = val)
				ELSE hex5::text
			END ||
			CASE
				WHEN hex6 >= 10
				THEN (SELECT code FROM conversion WHERE hex6 = val)
				ELSE hex6::text
			END
		FROM cte1
	INTO _ret;

	RETURN _ret;

END $BODY$;

COMMENT ON FUNCTION custom.random_hex() IS 'Generate a random hex colour (#0000FF).';


-- DROP FUNCTION IF EXISTS custom.random_rgb() CASCADE;

CREATE OR REPLACE FUNCTION custom.random_rgb()
	RETURNS TEXT
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	_ret text;

BEGIN

	SELECT
		ROUND(random()::numeric * 255) || ',' ||
		ROUND(random()::numeric * 255) || ',' ||
		ROUND(random()::numeric * 255)
	INTO _ret;

	RETURN _ret;

END $BODY$;

COMMENT ON FUNCTION custom.random_rgb() IS 'Generate a random RGB colour (0,0,255).';


-- DROP FUNCTION IF EXISTS custom.log(schema_name text, table_name text, _year integer, description text, _join text) CASCADE;

CREATE OR REPLACE FUNCTION custom.log(schema_name text, table_name text, _year integer DEFAULT EXTRACT(YEAR FROM current_date), description text DEFAULT NULL, _join text DEFAULT NULL)
	RETURNS TABLE(
		versions_id uuid,
		objekt_id uuid,
		_operation text,
		bruger_id text,
		dato timestamp without time zone,
		beskrivelse text,
		aendringer text
	)
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	_column_name text;
	_column text;
	_col_order text;

BEGIN


	WITH

--
-- Exclude specific columns regarding history
--

		exclude_cl(_column) AS(
			VALUES ('versions_id'), ('objekt_id'), ('oprettet'), ('systid_fra'), ('systid_til'), ('bruger_id_start'), ('bruger_id_slut')
		)

--
-- Array of column names and array of values
--

	SELECT
		ARRAY_AGG(a.attname::text ORDER BY a.attnum)::text AS _column_name,
		'ARRAY[' || string_agg(a.attname::text || '::text', ',' ORDER BY a.attnum) || ']' AS _column,
		ARRAY_AGG(a.attnum::text ORDER BY a.attnum) AS _col_order
	FROM pg_catalog.pg_attribute a
	LEFT JOIN pg_catalog.pg_class b ON a.attrelid = b.oid
	LEFT JOIN pg_catalog.pg_namespace c ON b.relnamespace = c.oid
	WHERE a.attnum > 0
		AND attisdropped IS FALSE
		AND (c.nspname, b.relname) = (schema_name, table_name)
		AND a.attname NOT IN(SELECT a._column FROM exclude_cl a)
	INTO _column_name, _column, _col_order;

	RETURN QUERY
	EXECUTE FORMAT(
		$$
			WITH

--
-- Retrieve records of selected rows within a given year
--

				cte1 AS(
					SELECT
						versions_id,
						objekt_id,
						systid_fra,
						systid_til,
						$a$%6$s$a$::int[] AS _order,
						$a$%7$s$a$::text[] AS _names,
						%8$s AS _values
					FROM %1$I.%2$I
					WHERE EXTRACT(YEAR FROM systid_fra) = %3$s OR EXTRACT(YEAR FROM systid_til) = %3$s
				),

--
-- Compare records
-- And aggregate
--

				cte2 AS(
					SELECT
						b.versions_id, -- Primary key of NEW record
						UNNEST(a._order) AS _order,
						UNNEST(a._names) AS _names,
						UNNEST(a._values) AS _old,
						UNNEST(b._values) AS _new
					FROM cte1 a
					LEFT JOIN cte1 b ON a.objekt_id = b.objekt_id AND CASE WHEN a.systid_til = b.systid_fra THEN TRUE END
					WHERE
						EXTRACT(YEAR FROM a.systid_til) = %3$s /*AND -- Systid_til equals year, i.e. no current records, because they are NULL
						CASE
							WHEN 
								a.objekt_id NOT IN(SELECT objekt_id FROM %1$I.%2$I WHERE systid_til IS NULL) AND -- If objekt_id is IN selection, then the object is still active, i.e. this is not deletion
								a.systid_til = (SELECT MAX(systid_til) FROM %1$I.%2$I d WHERE a.objekt_id = d.objekt_id) -- However if object_id doesn't exists IN selection and it is in fact the last in the object's lifetime it is a deletion
							THEN FALSE
							ELSE TRUE
						END*/
				),

				change AS(
					SELECT
						versions_id,
						string_agg(_names, ', ') AS aendringer
					FROM cte2
					WHERE _old IS DISTINCT FROM _new
					GROUP BY versions_id
				),

--
-- All records representing INSERT operations
--

				_history AS(

					(WITH

						_insert AS(
							SELECT
								*
							FROM %1$I.%2$I a
							WHERE EXTRACT(YEAR FROM a.systid_fra) = %3$s AND a.systid_fra = a.oprettet
						)

					SELECT
						a.versions_id,
						a.objekt_id,
						'Tilføjet' AS _operation,
						a.bruger_id_start AS bruger_id,
						a.systid_fra::timestamp(0) AS dato,
						%4$s AS beskrivelse,
						''::text AS aendringer
					FROM _insert a
					%5$s)

					UNION ALL

--
-- All records representing UPDATE operations
--

					(WITH

						_update AS(
							SELECT
								*
							FROM %1$I.%2$I a
							WHERE EXTRACT(YEAR FROM a.systid_fra) = %3$s AND a.systid_fra != a.oprettet
						)

					SELECT
						a.versions_id,
						a.objekt_id,
						'Ændret' AS _operation,
						a.bruger_id_start AS bruger_id,
						a.systid_fra::timestamp(0) AS dato,
						%4$s AS beskrivelse,
						change.aendringer
					FROM _update a
					LEFT JOIN change ON a.versions_id = change.versions_id
					%5$s)

					UNION ALL

--
-- All records representing DELETE operations
--

					(WITH

						_delete AS(
							SELECT
								*
							FROM %1$I.%2$I a
							WHERE EXTRACT(YEAR FROM a.systid_til) = %3$s
							AND CASE
									WHEN NOT EXISTS(SELECT '1' FROM %1$I.%2$I xyz WHERE xyz.systid_til IS NULL AND a.objekt_id = xyz.objekt_id)
										AND a.systid_til = (SELECT MAX(xyz.systid_til) FROM %1$I.%2$I xyz WHERE a.objekt_id = xyz.objekt_id) -- If objekt_id doesn't exist IN selection and it the last in the object's lifetime it is a deletion
									THEN TRUE
								END
						)

					SELECT
						a.versions_id,
						a.objekt_id,
						'Slettet' AS _operation,
						a.bruger_id_slut AS bruger_id,
						a.systid_til::timestamp(0) AS dato,
						%4$s AS beskrivelse,
						''::text AS aendringer
					FROM _delete a
					%5$s)
				)

				SELECT
					a.versions_id,
					a.objekt_id,
					a._operation::text,
					a.bruger_id::text,
					a.dato,
					a.beskrivelse::text,
					a.aendringer::text
				FROM _history a
				ORDER BY dato DESC
		$$,
		schema_name,
		table_name,
		_year,
		COALESCE(description, '''''::text'),
		COALESCE(_join, ''),
		_col_order,
		_column_name,
		_column
	);

END $BODY$;



-- DROP FUNCTION IF EXISTS custom.log_geom(schema_name text, table_name text, days integer, description text, _join text) CASCADE;

CREATE OR REPLACE FUNCTION custom.log_geom(schema_name text, table_name text, days integer, description text DEFAULT NULL, _join text DEFAULT NULL)
	RETURNS TABLE(
		versions_id uuid,
		objekt_id uuid,
		geom public.geometry,
		_operation text,
		bruger_id text,
		dato timestamp without time zone,
		beskrivelse text,
		aendringer text
	)
	LANGUAGE plpgsql AS
$$

DECLARE

	_geom text;
	_column_name text;
	_column text;
	_col_order text;

BEGIN

	SELECT
		geom_column
	FROM custom.geometry_of($1, $2)
	INTO _geom;

	WITH

--
-- Exclude specific columns regarding history
--

		exclude_cl(_column) AS(
			VALUES ('versions_id'), ('objekt_id'), ('oprettet'), ('systid_fra'), ('systid_til'), ('bruger_id_start'), ('bruger_id_slut')
		)

--
-- Array of column names and array of values
--

	SELECT
		ARRAY_AGG(a.attname::text ORDER BY a.attnum)::text AS _column_name,
		'ARRAY[' || string_agg(a.attname::text || '::text', ',' ORDER BY a.attnum) || ']' AS _column,
		ARRAY_AGG(a.attnum::text ORDER BY a.attnum) AS _col_order
	FROM pg_catalog.pg_attribute a
	LEFT JOIN pg_catalog.pg_class b ON a.attrelid = b.oid
	LEFT JOIN pg_catalog.pg_namespace c ON b.relnamespace = c.oid
	WHERE a.attnum > 0
		AND attisdropped IS FALSE
		AND (c.nspname, b.relname) = (schema_name, table_name)
		AND a.attname NOT IN(SELECT a._column FROM exclude_cl a)
	INTO _column_name, _column, _col_order;

	RETURN QUERY
	EXECUTE FORMAT(
		$qt$
			WITH

--
-- Retrieve records of selected rows within a given year
--

				cte1 AS(
					SELECT
						versions_id,
						objekt_id,
						systid_fra,
						systid_til,
						$a$%6$s$a$::int[] AS _order,
						$a$%7$s$a$::text[] AS _names,
						%8$s AS _values
					FROM %1$I.%2$I
					WHERE current_date - systid_til::date < %3$s OR current_date - systid_fra::date < %3$s
				),

--
-- Compare records
-- And aggregate
--

				cte2 AS(
					SELECT
						b.versions_id, -- Primary key of NEW record
						UNNEST(a._order) AS _order,
						UNNEST(a._names) AS _names,
						UNNEST(a._values) AS _old,
						UNNEST(b._values) AS _new
					FROM cte1 a
					LEFT JOIN cte1 b ON a.objekt_id = b.objekt_id AND a.systid_til = b.systid_fra
					WHERE
						current_date - a.systid_til::date < %3$s /*AND -- Systid_til equals year, i.e. no current records, because they are NULL
						CASE
							WHEN 
								a.objekt_id NOT IN(SELECT objekt_id FROM %1$I.%2$I WHERE systid_til IS NULL) AND -- If objekt_id is IN selection, then the object is still active, i.e. this is not deletion
								a.systid_til = (SELECT MAX(systid_til) FROM %1$I.%2$I d WHERE a.objekt_id = d.objekt_id) -- However if object_id doesn't exists IN selection and it is in fact the last in the object's lifetime it is a deletion
							THEN FALSE
							ELSE TRUE
						END*/
				),

				change AS(
					SELECT
						versions_id,
						string_agg(_names, ', ') AS aendringer
					FROM cte2
					WHERE _old IS DISTINCT FROM _new
					GROUP BY versions_id
				),

--
-- All records representing INSERT operations
--

				_history AS(
					SELECT
						a.versions_id,
						a.objekt_id,
						a.%9$s::public.geometry AS geom,
						'Tilføjet' AS _operation,
						a.bruger_id_start AS bruger_id,
						a.systid_fra::timestamp(0) AS dato,
						%4$s AS beskrivelse,
						''::text AS aendringer
					FROM %1$I.%2$I a
					%5$s
					WHERE current_date - a.systid_fra::date < %3$s AND CASE WHEN a.systid_fra = a.oprettet THEN TRUE END

					UNION ALL

--
-- All records representing UPDATE operations
--

					SELECT
						a.versions_id,
						a.objekt_id,
						a.%9$s::public.geometry AS geom,
						CASE
							WHEN current_date - a.oprettet::date < %3$s
							THEN 'Tilføjet og ændret'
							ELSE 'Ændret'
						END AS _operation,
						a.bruger_id_start AS bruger_id,
						a.systid_fra::timestamp(0) AS dato,
						%4$s AS beskrivelse,
						change.aendringer
					FROM %1$I.%2$I a
					LEFT JOIN change ON a.versions_id = change.versions_id
					%5$s
					WHERE current_date - a.systid_fra::date < %3$s AND a.systid_fra != a.oprettet

					UNION ALL

--
-- All records representing DELETE operations
--

					SELECT
						a.versions_id,
						a.objekt_id,
						a.%9$s::public.geometry AS geom,
						CASE
							WHEN current_date - a.oprettet::date < %3$s
							THEN 'Tilføjet og slettet'
							ELSE 'Slettet'
						END AS _operation,
						a.bruger_id_slut AS bruger_id,
						a.systid_til::timestamp(0) AS dato,
						%4$s AS beskrivelse,
						''::text AS aendringer
					FROM %1$I.%2$I a
					%5$s
					WHERE current_date - a.systid_til::date < %3$s AND
						CASE
							WHEN 
								a.objekt_id NOT IN(SELECT objekt_id FROM %1$I.%2$I WHERE systid_til IS NULL) AND
								a.systid_til = (SELECT MAX(xyz.systid_til) FROM %1$I.%2$I xyz WHERE a.objekt_id = xyz.objekt_id) -- If objekt_id doesn't exist IN selection and it the last in the object's lifetime it is a deletion
							THEN TRUE
						END
				),

				_dist AS(
					SELECT DISTINCT ON(a.objekt_id)
						a.versions_id,
						a.objekt_id,
						a.geom,
						a._operation::text,
						a.bruger_id::text,
						a.dato,
						a.beskrivelse::text,
						a.aendringer::text
					FROM _history a
					ORDER BY a.objekt_id, a.dato DESC
				)

				SELECT
					*
				FROM _dist
				ORDER BY dato DESC
		$qt$,
		schema_name, 
		table_name,
		days,
		COALESCE(description, '''''::text'),
		COALESCE(_join, ''),
		_col_order,
		_column_name,
		_column,
		_geom
	);

END $$;



--
-- TRIGGER FUNCTIONS
--


-- DROP FUNCTION IF EXISTS custom.auto_update() CASCADE;

CREATE OR REPLACE FUNCTION custom.auto_update()
	RETURNS trigger
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	schema_name text := TG_ARGV[0];
	table_name text := TG_ARGV[1];
	_col text[];

BEGIN

--
-- Check if arguments necessary are present
--

	IF TG_ARGV[0] IS NULL OR TG_ARGV[1] IS NULL THEN

		RAISE EXCEPTION 'Arguments for TRIGGER FUNCTION are missing!';

	END IF;

--
-- DELETE
--

	IF (TG_OP = 'DELETE') THEN

		EXECUTE FORMAT(
			'%s', custom.auto_delete(schema_name, table_name)
		)
		USING OLD;

		RETURN NULL;

--
-- UPDATE
--

	ELSIF (TG_OP = 'UPDATE') THEN

--
-- Find column values that has changed
--

		EXECUTE FORMAT(
			'%s', custom.auto_update_columns(schema_name, table_name, TG_TABLE_SCHEMA, TG_TABLE_NAME)
		)
		USING NEW, OLD
		INTO _col;

--
-- If any has changed
-- Update specific columns
--

		IF cardinality(_col) > 0 THEN

			EXECUTE FORMAT(
				'%s', custom.auto_update(schema_name, table_name, _col)
			)
			USING NEW, OLD;

		END IF;

		RETURN NULL;

--
-- INSERT
--

	ELSIF (TG_OP = 'INSERT') THEN

		EXECUTE FORMAT(
			'%s', custom.auto_insert(schema_name, table_name, TG_TABLE_SCHEMA, TG_TABLE_NAME)
		)
		USING NEW;

		RETURN NULL;

	END IF;

END $BODY$;


-- DROP FUNCTION IF EXISTS custom.geom_check_aggressive() CASCADE;

CREATE OR REPLACE FUNCTION custom.geom_check_aggressive()
	RETURNS trigger
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	boolean_var text;
	where_clause text;
	where_clause_new text;
	_check text;
	_geom text;
	_col text;


BEGIN

--
-- Get name of geometry column as identifier
--

	SELECT
		quote_ident((custom.geometry_of(TG_TABLE_SCHEMA, TG_TABLE_NAME)).geom_column)
	INTO _col;

--
-- Generate WHERE CLAUSE arguments for queries
--

	where_clause := COALESCE('AND ' || TG_ARGV[0], ''); -- WHERE CLAUSE, e.g. systid_til IS NULL

	_check := COALESCE('AND ' || TG_ARGV[1], ''); -- Use this to turn ON/OFF the check

--
-- Edit WHERE CLAUSE to NEW values, e.g 'xx = aa AND yy = bb' becomes 'NEW.xx = aa AND NEW.yy = bb'
-- And check whether the trigger should continue
--

	WITH

		cte1 AS(
			SELECT
				COALESCE(SUBSTRING(val, '^"(.*)"$'), val) AS val, -- Removing quotes if they appear both at the start and end
				num
			FROM regexp_split_to_table(where_clause, '\s') WITH ORDINALITY t(val, num) -- Ordinality to get same order when aggregating in next query
		)

	SELECT
		string_agg(
			CASE
				WHEN custom.check_tbl(TG_TABLE_SCHEMA, TG_TABLE_NAME, a.val)
				THEN '$1.' || quote_ident(val) -- Apply $1 for NEW, quote column
				ELSE val
			END, ' ' ORDER BY a.num
		)
	FROM cte1 a
	INTO where_clause_new;

	EXECUTE FORMAT(
		$$
			SELECT
				'1'
			WHERE TRUE %1$s
		$$,
		where_clause_new
	)
	USING NEW
	INTO boolean_var;

	IF boolean_var THEN


		EXECUTE FORMAT( -- Geometry type: Polygon/Multipolygon
			$$
				SELECT
					'1'
				FROM (
					SELECT
						(custom.geometry_of('%s', '%s')).geom_type
				) a
				WHERE geom_type = 2 %s
			$$, TG_TABLE_SCHEMA, TG_TABLE_NAME, _check
		)
		INTO boolean_var;

		IF boolean_var THEN

			EXECUTE FORMAT(
				$$
					UPDATE %1$s.%2$s
						SET
							%3$s = ST_Multi(ST_CollectionExtract(ST_Difference(%3$s, $1.%3$s), 3))
					WHERE ST_Intersects(%3$s, $1.%3$s) %4$s
				$$, TG_TABLE_SCHEMA, TG_TABLE_NAME, _col, where_clause
			)
			USING NEW;

		END IF;

	END IF;

	RETURN NEW;

END $BODY$;


-- DROP FUNCTION IF EXISTS custom.geom_check_passive() CASCADE;

CREATE OR REPLACE FUNCTION custom.geom_check_passive()
	RETURNS trigger
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	boolean_var text;
	where_clause text;
	_check text;
	_geom geometry('MultiPolygon', 25832);
	_col text;
	_exclude text;

	qc_intersect text;
	qc_contain text;
	_contained text;
	_intersect text;
	_fit text;

BEGIN

--
-- Identify geometry column of the given table
--

	SELECT
		(custom.geometry_of(TG_TABLE_SCHEMA, TG_TABLE_NAME)).geom_column
	INTO _col;

--
-- Generate WHERE-clauses features to include and wether or not to even continue the check
--

	where_clause := COALESCE('AND ' || TG_ARGV[0], ''); -- WHERE CLAUSE, e.g. systid_til IS NULL

	_check := COALESCE('AND ' || TG_ARGV[1], ''); -- Use this to turn ON/OFF the check

--
-- Geometry check only happens if geometry type is of the type polygons
-- and if any other given conditions are met
--

	EXECUTE FORMAT( -- Geometry type: Polygon/Multipolygon
		$$
			SELECT
				'1'
			FROM custom.geometry_of('%s', '%s')
			WHERE geom_type = 2 %s
		$$, TG_TABLE_SCHEMA, TG_TABLE_NAME, _check
	)
	INTO boolean_var;

	IF boolean_var THEN

--
-- Generate specific part of WHERE CLAUSE based on operation
-- Exclude the record being updated on updates
--

		IF (TG_OP = 'UPDATE') THEN

			SELECT
				FORMAT(
					$$
						AND %1$I != $2.%1$I
					$$, custom.primary_key(TG_TABLE_SCHEMA, TG_TABLE_NAME)
				)
			INTO _exclude;

		ELSE

			_exclude := '';

		END IF;


--
-- Generate SQL for different checks to for both INSERT and UPDATE
--

-- Quick-check Bounding box intersection

		SELECT
			FORMAT(
				$$
					SELECT
						'1'
					FROM %1$I.%2$I
					WHERE %3$I && $1.%3$I %4$s %5$s;
				$$, TG_TABLE_SCHEMA, TG_TABLE_NAME, _col, _exclude, where_clause
			)
		INTO qc_intersect;

-- Quick-check Bounding box contained

		SELECT
			FORMAT(
				$$
					SELECT
						'1'
					WHERE $1.%3$I @ (
						SELECT
							ST_Buffer(
								ST_Buffer(
									ST_Union(%3$I), 0.0001, 'join=mitre'
								), -0.0001, 'join=mitre'
							)
						FROM %1$I.%2$I
						WHERE %3$I && $1.%3$I %4$s %5$s);
				$$, TG_TABLE_SCHEMA, TG_TABLE_NAME, _col, _exclude, where_clause
			)
		INTO qc_contain;

-- Contained

		SELECT
			FORMAT(
				$$
					SELECT
						'1'
					WHERE ST_Within($1.%3$I, (
						SELECT
							ST_Buffer(
								ST_Buffer(
									ST_Union(%3$I), 0.0001, 'join=mitre'
								), -0.0001, 'join=mitre'
							)
						FROM %1$I.%2$I
						WHERE %3$I && $1.%3$I %4$s %5$s)
					);
				$$, TG_TABLE_SCHEMA, TG_TABLE_NAME, _col, _exclude, where_clause
			)
		INTO _contained;

-- Intersections

		SELECT
			FORMAT(
				$$
					SELECT
						'1'
					FROM %1$I.%2$I
					WHERE public.ST_Intersects(%3$I, $1.%3$I) %4$s %5$s;
				$$, TG_TABLE_SCHEMA, TG_TABLE_NAME, _col, _exclude, where_clause
			)
		INTO _intersect;

-- Fit geometry

		SELECT
			FORMAT( -- Intersections with existing geometries are removed
				$$
					WITH

						cte1 AS(
							SELECT
								ST_Multi(
									ST_CollectionExtract( -- Only polygons
										ST_Difference($1.%3$I, (
											SELECT
												ST_Buffer(
													ST_Buffer(
														ST_Union(%3$I), 0.0001, 'join=mitre'
													), -0.0001, 'join=mitre'
												)
											FROM %1$I.%2$I
											WHERE ST_Intersects($1.%3$I, %3$I) %4$s %5$s
										)
										), 3
									)
								) AS geom
						),

						cte2 AS(
							SELECT
								(ST_Dump(geom)).geom
							FROM cte1
						)

					SELECT
						ST_Multi(
							ST_Union(geom)
						)
					FROM cte2
					WHERE ST_Area(geom) > 0.01; -- Minimum size
				$$, TG_TABLE_SCHEMA, TG_TABLE_NAME, _col, _exclude, where_clause
			)
		INTO _fit;

--
-- UPDATE
--

		IF (TG_OP = 'UPDATE') THEN

			EXECUTE FORMAT( -- Quick-check Bounding box intersection
				'%s', qc_intersect
			)
			USING NEW, OLD
			INTO boolean_var;

			IF boolean_var THEN

				EXECUTE FORMAT( -- Quick-check Bounding box contained
					'%s', qc_contain
				)
				USING NEW, OLD
				INTO boolean_var;

				IF boolean_var THEN

					EXECUTE FORMAT( -- Contained
						'%s', _contained
					)
					USING NEW, OLD
					INTO boolean_var;

					IF boolean_var THEN -- If contained check is TRUE

						RAISE EXCEPTION 'Geometrien befinder sig indenfor andre geometrier!';

					END IF;

				END IF;

				EXECUTE FORMAT( -- Intersections
					'%s', _intersect
				)
				USING NEW, OLD
				INTO boolean_var;

				IF boolean_var THEN

					EXECUTE FORMAT( -- Fit
						'%s', _fit
					)
					USING NEW, OLD
					INTO _geom;

					NEW := NEW #= hstore(_col, _geom); -- NEW.column is assigned the proper value

				END IF;

			END IF;

--
-- INSERT
--

		ELSIF (TG_OP = 'INSERT') THEN

			EXECUTE FORMAT( -- Quick-check Bounding box intersection
				'%s', qc_intersect
			)
			USING NEW
			INTO boolean_var;

			IF boolean_var THEN

				EXECUTE FORMAT( -- Quick-check Bounding box contained
					'%s', qc_contain
				)
				USING NEW
				INTO boolean_var;

				IF boolean_var THEN

					EXECUTE FORMAT( -- Contained
						'%s', _contained
					)
					USING NEW
					INTO boolean_var;

					IF boolean_var THEN -- If contained check is TRUE

						RAISE EXCEPTION 'Geometrien befinder sig indenfor andre geometrier!';

					END IF;

				END IF;

				EXECUTE FORMAT( -- Intersections
					'%s', _intersect
				)
				USING NEW
				INTO boolean_var;

				IF boolean_var THEN

					EXECUTE FORMAT( -- Fit
						'%s', _fit
					)
					USING NEW
					INTO _geom;

					NEW := NEW #= hstore(_col, _geom); -- NEW.column is assigned the proper value

				END IF;

			END IF;

		END IF;

	END IF;

	RETURN NEW;

END $BODY$;


-- DROP FUNCTION IF EXISTS custom.hierarchy() CASCADE;

CREATE OR REPLACE FUNCTION custom.hierarchy()
	RETURNS trigger
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	_col text := TG_ARGV[0];
	_cardinality int;
	boolean_var text;

BEGIN

--
-- UPDATE-check
--

	IF (TG_OP = 'UPDATE') THEN

--
-- No changes in column
--

		EXECUTE FORMAT(
			$$
				SELECT
					'1'
				WHERE $1.%1$I = $2.%1$I
			$$, _col
		)
		USING NEW, OLD
		INTO boolean_var;

--
-- If TRUE
--

		IF boolean_var THEN

			RETURN NEW;

		END IF;

	END IF;

--
-- DELETE / UPDATE
--

	IF (TG_OP = 'DELETE') OR (TG_OP = 'UPDATE') THEN

--
-- Cardinality
--

		EXECUTE FORMAT(
			$$
				SELECT
					cardinality($1.%1$I)
			$$, _col
		)
		USING OLD
		INTO _cardinality;

--
-- Check for lower levels in hierarchy
--

		EXECUTE FORMAT(
			$$
				SELECT
					'1'
				FROM %1$I.%2$I
				WHERE
					custom.array_trim(%3$I, %4$s) = $1.%3$I AND
					%3$I != $1.%3$I
			$$, TG_TABLE_SCHEMA, TG_TABLE_NAME, _col, _cardinality
		)
		USING OLD
		INTO boolean_var;

		IF boolean_var THEN

			RAISE EXCEPTION 'Hierarchy dependency found!';

		END IF;

		IF (TG_OP = 'DELETE') THEN

			RETURN OLD;

		END IF;

	END IF;

--
-- INSERT / UPDATE
--

	IF (TG_OP = 'INSERT') OR (TG_OP = 'UPDATE') THEN

--
-- Cardinality
--

		EXECUTE FORMAT(
			$$
				SELECT
					cardinality($1.%1$I)
			$$, _col
		)
		USING NEW
		INTO _cardinality;

--
-- Highest in the hierarchy, no checks
--

		IF _cardinality = 1 THEN

			RETURN NEW;

		END IF;

--
-- INSERT
--

		IF (TG_OP = 'INSERT') THEN


--
-- Check for existing value of one level above
--

			EXECUTE FORMAT(
				$$
					SELECT
						'1'
					FROM %1$I.%2$I
					WHERE %3$I = custom.array_trim($1.%3$I, %4$s)
				$$, TG_TABLE_SCHEMA, TG_TABLE_NAME, _col, _cardinality - 1
			)
			USING NEW
			INTO boolean_var;

		ELSIF (TG_OP = 'UPDATE') THEN

--
-- Check for existing value of one level above
--

			EXECUTE FORMAT(
				$$
					SELECT
						'1'
					FROM %1$I.%2$I
					WHERE %3$I = custom.array_trim($1.%3$I, %4$s) AND
						%3$I != $2.%3$I
				$$, TG_TABLE_SCHEMA, TG_TABLE_NAME, _col, _cardinality - 1
			)
			USING NEW, OLD
			INTO boolean_var;

		END IF;

--
-- If TRUE, return NEW
--

		IF boolean_var THEN

			RETURN NEW;

		END IF;

		RAISE EXCEPTION 'Hierarchy not found!';

	END IF;


END $BODY$;


-- DROP FUNCTION IF EXISTS custom.history() CASCADE;

CREATE OR REPLACE FUNCTION custom.history()
	RETURNS trigger
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	boolean_var text;

BEGIN

--
-- If record is a part of history
-- Ignore action completely
--

	IF TG_OP IN('DELETE', 'UPDATE') AND OLD.systid_til IS NOT NULL THEN

		RETURN NULL;

	END IF;

--
-- DELETE
-- Apply values for closing record
-- Insert into table and delete original
-- Deferrable primary key needed
--

	IF (TG_OP = 'DELETE') THEN

		OLD.systid_til = current_timestamp;
		OLD.bruger_id_slut = current_user;

		EXECUTE FORMAT(
			$$
				INSERT INTO %I.%I
					VALUES ($1.*)
			$$, TG_TABLE_SCHEMA, TG_TABLE_NAME
		)
		USING OLD;

		RETURN OLD;

--
-- UPDATE
--

	ELSIF (TG_OP = 'UPDATE') THEN

		IF OLD.systid_fra != current_timestamp THEN -- If systid_fra = current_timestamp ie. systid_til it is due to several changes happening at once in QGIS, creating a lot of trigger-action, this should be ignored

--
-- Apply vlues for updated record
--

			NEW.versions_id = public.uuid_generate_v1(); -- UUID
			NEW.objekt_id = OLD.objekt_id; -- Overwrites potential changes from user
			NEW.oprettet = OLD.oprettet; -- Overwrites potential changes from user
			NEW.systid_fra = current_timestamp; -- Timestamp
			NEW.systid_til = NULL; -- Overwrites potential changes from user
			NEW.bruger_id_start = current_user; -- User responsible
			NEW.bruger_id_slut = NULL; -- Overwrites potential changes from user

--
-- Apply values for closing record
-- Insert into table and update original
-- Deferrable primary key needed
--

			OLD.systid_til = current_timestamp;
			OLD.bruger_id_slut = current_user;

			EXECUTE FORMAT(
				$$
					INSERT INTO %I.%I
						VALUES ($1.*)
				$$, TG_TABLE_SCHEMA, TG_TABLE_NAME
			)
			USING OLD;

		END IF;

		RETURN NEW;

--
-- INSERT
--

	ELSIF (TG_OP = 'INSERT') THEN

--
-- Ignore closed records and return NEW
--

		IF NEW.systid_til = current_timestamp THEN

			RETURN NEW;

		END IF;

--
-- Apply information
--

		NEW.versions_id = public.uuid_generate_v1(); -- UUID
		NEW.objekt_id = NEW.versions_id; -- UUID as versions_id
		NEW.oprettet = current_timestamp; -- Timestamp
		NEW.systid_fra = NEW.oprettet; -- Timestamp as oprettet
		NEW.systid_til = NULL; -- Overwrites potential changes from user
		NEW.bruger_id_start = current_user; -- User responsible
		NEW.bruger_id_slut = NULL; -- Overwrites potential changes from user

		RETURN NEW;

	END IF;


END $BODY$;


--
-- TABLES
--


--
-- VIEWS
--


--
-- INDEXES
--


--
-- TRIGGERS
--


--
-- INSERTS
--





--
-- Filtering module for PostgreSQL 10
--
--
--


--
-- DROP SCHEMAS AND MISC
--

DROP SCHEMA IF EXISTS filter CASCADE;


--
-- SCHEMAS
--

CREATE SCHEMA filter;

COMMENT ON SCHEMA filter IS 'User-based filtering of data.';


--
-- DOMAINS
--

CREATE DOMAIN filter.text text[];

COMMENT ON DOMAIN filter.text IS 'Simple text domain used to distinguish IN-filters from relational filters.';


--
-- FUNCTIONS
--


-- DROP FUNCTION IF EXISTS filter._value(column_name text) CASCADE;

CREATE OR REPLACE FUNCTION filter._value(column_name text)
	RETURNS text
	LANGUAGE plpgsql AS
$$

DECLARE

	_ret text;

BEGIN

	EXECUTE FORMAT(
		$qt$
			SELECT
				%1$I::text
			FROM filter.settings
			WHERE rolname = current_user
		$qt$, $1
	)
	INTO _ret;

	RETURN _ret;

END $$;

COMMENT ON FUNCTION filter._value(column_name text) IS 'Find values in settings table with relating to filter.v_settings.';


-- DROP FUNCTION IF EXISTS filter.check_tbl(schema_name text, table_name text, column_name text) CASCADE;

CREATE OR REPLACE FUNCTION filter.check_tbl(schema_name text DEFAULT NULL, table_name text DEFAULT NULL, column_name text DEFAULT NULL)
	RETURNS BOOLEAN
	LANGUAGE plpgsql AS
$$

BEGIN

--
-- If name of column has been specified
-- Check for column
--

	IF
		$1 IS NOT NULL AND
		$2 IS NOT NULL AND
		$3 IS NOT NULL
	THEN

		IF EXISTS(SELECT
					'1'
				FROM pg_catalog.pg_attribute a
				LEFT JOIN pg_catalog.pg_class b ON a.attrelid = b.oid
				LEFT JOIN pg_catalog.pg_namespace c ON b.relnamespace = c.oid
				WHERE a.attnum > 0 AND c.nspname = $1 AND b.relname = $2 AND a.attname = $3
		) THEN

			RETURN TRUE;

		ELSE

			RETURN NULL;

		END IF;

--
-- If only name of schema and table has been specified
-- Check for table
--

	ELSIF
		$1 IS NOT NULL AND
		$2 IS NOT NULL
	THEN

		IF EXISTS(SELECT
					'1'
				FROM pg_catalog.pg_class a
				LEFT JOIN pg_catalog.pg_namespace b ON a.relnamespace = b.oid
				WHERE b.nspname = $1 AND a.relname = $2
		) THEN

			RETURN TRUE;

		ELSE

			RETURN NULL;

		END IF;

--
-- If only name of schema has been specified
-- Check for schema
--

	ELSIF $1 IS NOT NULL THEN

		IF EXISTS(SELECT
					'1'
				FROM pg_catalog.pg_namespace
				WHERE nspname = $1
		) THEN

			RETURN TRUE;

		ELSE

			RETURN NULL;

		END IF;

	ELSE

		RETURN NULL;

	END IF;

END $$;

COMMENT ON FUNCTION filter.check_tbl(schema_name text, table_name text, column_name text) IS 'Checks for existing objects:
If only schema is specified then schema,
if table.. then table and
if column.. then column.
Returns NULL if the object doesn''t exist';


-- DROP FUNCTION IF EXISTS filter.col_name(filter_key_column text, filter_id int) CASCADE;

CREATE OR REPLACE FUNCTION filter.col_name(filter_key_column text, filter_id int)
	RETURNS text
	LANGUAGE plpgsql AS
$$

DECLARE

	ret text;

BEGIN

	SELECT
		FORMAT(
			'%1$s_%2$s',
			LEFT(
				filter.rem_alias($1), 19 - LENGTH($2::text) -- Remove aliases and reduce length to make room for _##
			), $2
		)
	INTO ret;

	RETURN ret;

END $$;

COMMENT ON FUNCTION filter.col_name(filter_key_column text, filter_id int) IS 'Converts text in column into a column name for filter.settings.';


-- DROP FUNCTION IF EXISTS filter.current_values(filter_id integer)

CREATE OR REPLACE FUNCTION filter.current_values(filter_id integer)
	RETURNS text
	LANGUAGE plpgsql AS
$$

DECLARE

	filter_schema text;
	filter_table text;
	filter_key_column text;
	filter_value_column text;
	ret text;

BEGIN

--
-- IN-filter
--

	IF (SELECT
			a.key_relation
		FROM filter.filters a
		WHERE a.filter_id = $1) IS NULL
	THEN

--
-- Select inputs as variables
--

		SELECT
			a.filter_schema,
			a.filter_table,
			a.filter_key_column,
			a.filter_value_column
		FROM filter.filters a
		WHERE a.filter_id = $1
		INTO
			filter_schema,
			filter_table,
			filter_key_column,
			filter_value_column;

--
-- Select aggregate of values based on user inputs
--

		EXECUTE FORMAT(
			$qt$
				SELECT
					string_agg(%5$s, ', ' ORDER BY %3$s) AS agg_
				FROM %1$I.%2$I
				WHERE %3$s::text = ANY(filter._value('%4$s')::text[])
			$qt$,
			filter_schema,
			filter_table,
			filter.rem_alias(filter_key_column),
			filter.col_name(filter_key_column, $1),
			filter_value_column
		)
		INTO ret;

	ELSE

		SELECT
			a.filter_key_column
		FROM filter.filters a
		WHERE a.filter_id = $1
		INTO filter_key_column;

		EXECUTE FORMAT(
			$qt$
				SELECT
					filter._value('%s')::text
			$qt$, filter.col_name(filter_key_column, $1)
		)
		INTO ret;

	END IF;

	RETURN ret;

END $$;

COMMENT ON FUNCTION filter.current_values(filter_id integer) IS 'Find current filtering values for the current user for both IN- and relation filters.';


-- DROP FUNCTION IF EXISTS filter.data_type_of(schema_name text, table_name text, column_name text) CASCADE;

CREATE OR REPLACE FUNCTION filter.data_type_of(schema_name text, table_name text, column_name text)
	RETURNS text
	LANGUAGE plpgsql AS
$$

DECLARE

	_ret text;

BEGIN

SELECT
	format_type(a.atttypid, a.atttypmod) AS typname
FROM pg_catalog.pg_attribute a
LEFT JOIN pg_catalog.pg_class b ON a.attrelid = b.oid
LEFT JOIN pg_catalog.pg_namespace c ON b.relnamespace = c.oid
WHERE (c.nspname, b.relname, a.attname) = ($1, $2, $3)
INTO _ret;

	RETURN _ret;

END $$;

COMMENT ON FUNCTION filter.data_type_of(schema_name text, table_name text, column_name text) IS 'Returns data type of a specified column.';


-- DROP FUNCTION IF EXISTS filter.filter(schema_name text, table_name text) CASCADE;

CREATE OR REPLACE FUNCTION filter.filter(schema_name text, table_name text)
	RETURNS SETOF record
	LANGUAGE plpgsql AS
$$

DECLARE

	_rec record;

BEGIN

--
-- If no filtering parameters are listed,
-- One query will check both the table and the filters
--

	IF filter._value('filter')::bool IS FALSE OR NOT EXISTS(
		WITH

			cte1 AS(
				SELECT
					UNNEST(table_id) AS table_id
				FROM filter.filters
				WHERE 
					CASE
						WHEN key_relation IS NULL
						THEN filter.check_tbl(filter_schema, filter_table, filter.rem_alias(filter_key_column))
						ELSE TRUE
					END
			)

		SELECT
			'1'
		FROM filter.tables a
		INNER JOIN cte1 b ON a.table_id = b.table_id -- If no tables or filters are present, zero rows will be selected
		WHERE (a.schema_name, a.table_name) = ($1, $2)
	) THEN -- If zero rows

		RETURN QUERY
		EXECUTE FORMAT(
			'%s', -- Simply select the given primary key
			(SELECT
				FORMAT(
					$qt$
						SELECT
							%s
						FROM %s.%s
					$qt$,
					COALESCE( -- Either use value from pk_columns or find in metadata
						(SELECT
							pk_columns
						FROM filter.tables a
						WHERE (a.schema_name, a.table_name) = ($1, $2)), filter.primary_key($1, $2)
					), $1,$2
				)
			)
		);

	ELSE

		RETURN QUERY
		EXECUTE FORMAT('%s',
			(WITH

--
-- UNNEST table_id
-- Check if the specified filtering column even exists
--

				cte1 AS(
					SELECT
						a.filter_id,
						UNNEST(a.table_id) AS table_id,
						a.filter_key_column,
						a.key_relation,
						a.target_key_column
					FROM filter.filters a
					WHERE CASE
						WHEN key_relation IS NULL
						THEN filter.check_tbl(a.filter_schema, a.filter_table, filter.rem_alias(a.filter_key_column))
						ELSE TRUE
					END
				),

--
-- Combine filters the the table information for the specified table
--

				cte2 AS(
					SELECT
						a.filter_id,
						a.table_id,
						a.filter_key_column,
						a.key_relation,
						a.target_key_column,
						b.schema_name,
						b.table_name,
						b.pk_columns,
						b.filter_join
					FROM cte1 a
					LEFT JOIN filter.tables b ON a.table_id = b.table_id
					WHERE (b.schema_name, b.table_name) = ($1, $2) AND filter.check_tbl(b.schema_name, b.table_name)
				),

--
-- Generate the WHERE-clause that is the filter
--

				cte3 AS(
					SELECT
						a.schema_name,
						a.table_name,
						a.pk_columns,
						a.filter_join,
						COALESCE( -- If fail-safe WHERE TRUE
							string_agg( -- Aggregate the different filters
								CASE
									WHEN a.key_relation IS NOT NULL -- Relational filters =, >, <
									THEN FORMAT(
										$qt$
											CASE
												WHEN (SELECT filter._value('%1$s')) IS NOT NULL
												THEN %2$s %3$s (SELECT filter._value('%1$s')::%4$s)
												ELSE TRUE
											END
										$qt$, filter.col_name(a.filter_key_column, a.filter_id), a.filter_key_column, a.key_relation, filter.data_type_of('filter', 'settings', filter.col_name(a.filter_key_column, a.filter_id))
									)
									ELSE FORMAT( -- IN-filters, "based on other tables"
										$qt$
											CASE
												WHEN (SELECT filter._value('%1$s')::text) != '{}'
												THEN %2$s::text = ANY(filter._value('%1$s')::text[])
												ELSE TRUE
											END
										$qt$, filter.col_name(a.filter_key_column, a.filter_id), COALESCE(a.target_key_column, a.filter_key_column)
									)
								END, ' AND '
							), 'TRUE'
						) AS _body
					FROM cte2 a
					GROUP BY a.schema_name, a.table_name, a.pk_columns, a.filter_join
				)

--
-- Generate the SQL
--

			SELECT
				FORMAT(
					$qt$
						SELECT
							%s
						FROM %I.%I a
						%s
						WHERE %s
					$qt$, 'a.' || regexp_replace(COALESCE(a.pk_columns, filter.primary_key($1, $2)), ',', ',a.', 'g'), a.schema_name, a.table_name, a.filter_join, a._body
				)
			FROM cte3 a)
		);

	END IF;

END $$;

COMMENT ON FUNCTION filter.filter(schema_name text, table_name text) IS 'Returns a record with primary key-values based on filters.
To select from function a column definition list is required, e.g. "SELECT a FROM filter.filter(''xx'', ''yy'') a(versions_id uuid)" would return a record with one value. "SELECT versions_id FROM filter.filter(''xx'', ''yy'') a(versions_id uuid)" would return the actual column as UUID.';


-- DROP FUNCTION IF EXISTS filter.label() CASCADE;

CREATE OR REPLACE FUNCTION filter.label()
	RETURNS text
	LANGUAGE plpgsql AS
$$

DECLARE

	ret text;

BEGIN

	IF filter._value('filter')::boolean IS TRUE THEN

		WITH

			cte1 AS(
				SELECT
					a.label_level,
					CASE
						WHEN a.key_relation IS NULL
						THEN a.label || ': ' || filter.current_values(a.filter_id)
						ELSE a.label || ' ' || a.key_relation || ' ' || filter.current_values(a.filter_id)
					END AS _body
				FROM filter.filters a
				WHERE filter.current_values(a.filter_id) IS NOT NULL AND label_level IS NOT NULL
			),

			cte2 AS(
				SELECT
					a.label_level,
					string_agg(a._body, E'\n' ORDER BY a._body) AS _body
				FROM cte1 a
				GROUP BY a.label_level
			)

		SELECT
			COALESCE(E'Filtre:\n' || string_agg(a._body, E'\n\n' ORDER BY a.label_level DESC), '')
		FROM cte2 a
		INTO ret;

		RETURN ret;

	ELSE

		RETURN '';

	END IF;

END $$;

COMMENT ON FUNCTION filter.label() IS 'Returns filtering values as an overview, can be used in combination with other information to show as a label.';


-- DROP FUNCTION IF EXISTS filter.rem_alias(filter_key_column text) CASCADE;

CREATE OR REPLACE FUNCTION filter.rem_alias(filter_key_column text)
	RETURNS text
	LANGUAGE plpgsql AS
$$

DECLARE

	ret text;

BEGIN

	SELECT
		FORMAT(
			'%1$s',
			regexp_replace($1, '.*\.', '')
		)
	INTO ret;

	RETURN ret;

END $$;

COMMENT ON FUNCTION filter.rem_alias(filter_key_column text) IS 'Removes aliases from column names, i.e. a.id becomes id.';


-- DROP FUNCTION IF EXISTS filter.settings() CASCADE;

CREATE OR REPLACE FUNCTION filter.settings()
	RETURNS text
	LANGUAGE plpgsql AS
$$

DECLARE

	ret text;

BEGIN

	SELECT
		'DROP VIEW IF EXISTS filter.v_settings;

		CREATE VIEW filter.v_settings AS

		SELECT
			1 AS id, -- Primary key, easy relation to table in QGIS, value will always be 1 regardless of user
			rolname' || 
			COALESCE(
				(SELECT ',' || sql_storage FROM filter.view_storage), '' -- If any additional SQL has been specified in filter.view_storage
			) ||
			COALESCE(',' || -- If any filters has been made
				string_agg(
					CASE
						WHEN e.nspname = 'filter' -- If namespace of the data type is filter, i.e. domain is used, it's an IN-filter
						THEN 'COALESCE(' ||a.attname || ', ''{}'') AS ' || a.attname
						ELSE a.attname
					END, ',' ORDER BY a.attnum
				), ''
			) ||
		' FROM filter.settings
		WHERE rolname = current_user;

		CREATE TRIGGER v_settings_trg_iud INSTEAD OF INSERT OR UPDATE ON filter.v_settings FOR EACH ROW EXECUTE PROCEDURE filter.auto_update_settings(''filter'', ''settings'');

		COMMENT ON VIEW filter.v_settings IS ''Don''''t make any other queries depend on this view. Use function filter._value(column_name text) instead!;'''
		FROM pg_catalog.pg_attribute a
		LEFT JOIN pg_catalog.pg_class b ON a.attrelid = b.oid
		LEFT JOIN pg_catalog.pg_namespace c ON b.relnamespace = c.oid
		LEFT JOIN pg_catalog.pg_type d ON a.atttypid = d.oid
		LEFT JOIN pg_catalog.pg_namespace e ON d.typnamespace = e.oid
		WHERE a.attname != 'rolname' AND a.attnum > 0 AND a.attisdropped IS FALSE AND c.nspname = 'filter' AND b.relname = 'settings'
	INTO ret;

	RETURN ret;

END $$;

COMMENT ON FUNCTION filter.settings() IS 'Returns the SQL used to create filter.v_settings.';


-- DROP FUNCTION IF EXISTS filter.auto_check_update(from_schema_name text, from_table_name text, into_schema_name text, into_table_name text) CASCADE;

CREATE OR REPLACE FUNCTION filter.auto_check_update(from_schema_name text, from_table_name text, into_schema_name text DEFAULT NULL, into_table_name text DEFAULT NULL)
	RETURNS text
	LANGUAGE plpgsql AS
$$

DECLARE

	_ret text;

BEGIN

	WITH

		cte1 AS(
			SELECT
				quote_ident(a.attname) AS attname,
				a.attnum
			FROM pg_catalog.pg_attribute a
			LEFT JOIN pg_catalog.pg_class b ON a.attrelid = b.oid
			LEFT JOIN pg_catalog.pg_namespace c ON b.relnamespace = c.oid
			WHERE
				attnum > 0 AND
				(c.nspname, b.relname) = (COALESCE($3, $1), COALESCE($4, $2)) AND
				attname IN(SELECT filter.common_columns($1, $2, COALESCE($3, $1), COALESCE($4, $2)))
			ORDER BY a.attnum
		)

	SELECT
		FORMAT(
			$qt$
SELECT
	'1'
WHERE %s;
			$qt$, E'(\n\t' || string_agg('$1.' || attname, E',\n\t' ORDER BY attnum) || E'\n) = (\n\t' || string_agg('$2.' || attname, E',\n\t' ORDER BY attnum) || E'\n)'
		)
	FROM cte1
	INTO _ret;

	RETURN _ret;

-------------------
/*
	SELECT
		FORMAT(
			$qt$
SELECT
	'1'
WHERE %s;
			$qt$, E'(\n\t' || string_agg('$1.' || attname || ' = $2.' || attname, E' AND\n\t' ORDER BY attnum) || E'\n)'
		)
	FROM cte1
	INTO _ret;

	RETURN _ret;
*/
-------------------

END $$;

COMMENT ON FUNCTION filter.auto_check_update(from_schema_name text, from_table_name text, into_schema_name text, into_table_name text) IS 'SQL: SELECT ''1'' if all NEW values are equal all OLD values for all common columns.';


-- DROP FUNCTION IF EXISTS filter.auto_delete(schema_name text, table_name text) CASCADE;

CREATE OR REPLACE FUNCTION filter.auto_delete(schema_name text, table_name text)
	RETURNS text
	LANGUAGE plpgsql AS
$$

DECLARE

	_ret text;

BEGIN

	SELECT
		FORMAT(
			$qt$
DELETE FROM
	%1$I.%2$I
WHERE %3$s = $1.%3$s;
			$qt$, $1, $2, filter.primary_key($1, $2)
		)
	INTO _ret;

	RETURN _ret;

END $$;

COMMENT ON FUNCTION filter.auto_delete(schema_name text, table_name text) IS 'SQL: DELETE from table where PK equals OLD PK. Only one column as PK.';


-- DROP FUNCTION IF EXISTS filter.auto_insert(from_schema_name text, from_table_name text, into_schema_name text, into_table_name text) CASCADE;

CREATE OR REPLACE FUNCTION filter.auto_insert(from_schema_name text, from_table_name text, into_schema_name text DEFAULT NULL, into_table_name text DEFAULT NULL)
	RETURNS text
	LANGUAGE plpgsql AS
$$

DECLARE

	_ret text;

BEGIN

	WITH

		cte1 AS( -- Get columns and default values
			SELECT
				quote_ident(a.attname) AS attname,
				a.attnum,
				COALESCE(d.adsrc, e.typdefault) AS adsrc
			FROM pg_catalog.pg_attribute a
			LEFT JOIN pg_catalog.pg_class b ON a.attrelid = b.oid
			LEFT JOIN pg_catalog.pg_namespace c ON b.relnamespace = c.oid
			LEFT JOIN pg_catalog.pg_attrdef d ON (d.adrelid, d.adnum) = (b.oid, a.attnum)
			LEFT JOIN pg_catalog.pg_type e ON a.atttypid = e.oid
			WHERE
				attnum > 0 AND
				(c.nspname, b.relname) = (COALESCE($3, $1), COALESCE($4, $2)) AND
				attname IN(SELECT filter.common_columns($1, $2, COALESCE($3, $1), COALESCE($4, $2)))
			ORDER BY a.attnum
		)

	SELECT
		FORMAT(
			$qt$
INSERT INTO %I.%I(
	%s
) VALUES(
	%s
);
			$qt$, COALESCE($3, $1), COALESCE($4, $2), string_agg(attname, E',\n\t' ORDER BY attnum), string_agg(COALESCE('COALESCE($1.' || attname || ',' || adsrc || ')', '$1.' || attname), E',\n\t' ORDER BY attnum)
		)
	FROM cte1
	INTO _ret;

	RETURN _ret;

END $$;

COMMENT ON FUNCTION filter.auto_insert(from_schema_name text, from_table_name text, into_schema_name text, into_table_name text) IS 'SQL: INSERT common columns into table.';


-- DROP FUNCTION IF EXISTS filter.auto_update(from_schema_name text, from_table_name text, into_schema_name text, into_table_name text) CASCADE;

CREATE OR REPLACE FUNCTION filter.auto_update(from_schema_name text, from_table_name text, into_schema_name text DEFAULT NULL, into_table_name text DEFAULT NULL)
	RETURNS text
	LANGUAGE plpgsql AS
$$

DECLARE

	_ret text;

BEGIN

	WITH

		cte1 AS( -- Get columns and default values
			SELECT
				quote_ident(a.attname) AS attname,
				a.attnum,
				d.adsrc
			FROM pg_catalog.pg_attribute a
			LEFT JOIN pg_catalog.pg_class b ON a.attrelid = b.oid
			LEFT JOIN pg_catalog.pg_namespace c ON b.relnamespace = c.oid
			LEFT JOIN pg_catalog.pg_attrdef d ON (d.adrelid, d.adnum) = (b.oid, a.attnum)
			WHERE
				attnum > 0 AND
				(c.nspname, b.relname) = (COALESCE($3, $1), COALESCE($4, $2)) AND
				attname IN(SELECT filter.common_columns($1, $2, COALESCE($3, $1), COALESCE($4, $2)))
			ORDER BY a.attnum
		)

	SELECT
		FORMAT(
			$qt$
UPDATE %1$I.%2$I
	SET
		%3$s
WHERE %4$I = $2.%4$I;
			$qt$,  COALESCE($3, $1), COALESCE($4, $2), string_agg(attname || ' = $1.' || attname, E',\n\t\t' ORDER BY attnum), filter.primary_key(COALESCE($3, $1), COALESCE($4, $2))
		)
	FROM cte1
	INTO _ret;

	RETURN _ret;

END $$;

COMMENT ON FUNCTION filter.auto_update(from_schema_name text, from_table_name text, into_schema_name text, into_table_name text) IS 'SQL: UPDATE common columns to NEW values where PK equals OLD PK. Only one column as PK.';


-- DROP FUNCTION IF EXISTS filter.common_columns(schema_name_1 text, table_name_1 text, schema_name_2 text, table_name_2 text) CASCADE;

CREATE OR REPLACE FUNCTION filter.common_columns(schema_name_1 text, table_name_1 text, schema_name_2 text, table_name_2 text)
	RETURNS TABLE (
		column_name text
	)
	LANGUAGE plpgsql AS
$$

BEGIN

	RETURN QUERY
	SELECT
		a.attname::text
	FROM (
		SELECT
			a.attname
		FROM pg_catalog.pg_attribute a
		LEFT JOIN pg_catalog.pg_class b ON a.attrelid = b.oid
		LEFT JOIN pg_catalog.pg_namespace c ON b.relnamespace = c.oid
		WHERE attnum > 0 AND (c.nspname, b.relname) = ($1, $2)

		INTERSECT

		SELECT
			a.attname
		FROM pg_catalog.pg_attribute a
		LEFT JOIN pg_catalog.pg_class b ON a.attrelid = b.oid
		LEFT JOIN pg_catalog.pg_namespace c ON b.relnamespace = c.oid
		WHERE attnum > 0 AND (c.nspname, b.relname) = ($3, $4)
	) a
	LEFT JOIN (
		SELECT
			a.attname,
			a.attnum
		FROM pg_catalog.pg_attribute a
		LEFT JOIN pg_catalog.pg_class b ON a.attrelid = b.oid
		LEFT JOIN pg_catalog.pg_namespace c ON b.relnamespace = c.oid
		WHERE (c.nspname, b.relname) = ($3, $4)
	) b ON a.attname = b.attname
	ORDER BY b.attnum;

END $$;

COMMENT ON FUNCTION filter.common_columns(schema_name_1 text, table_name_1 text, schema_name_2 text, table_name_2 text) IS 'Finds common columns between tables/views based on the name of the column';


-- DROP FUNCTION IF EXISTS filter.create_auto_trigger(view_schema text, view_name text, table_schema text, table_name text) CASCADE;

CREATE OR REPLACE FUNCTION filter.create_auto_trigger(view_schema text, view_name text, table_schema text, table_name text)
	RETURNS VOID
	LANGUAGE plpgsql AS
$$

BEGIN

	EXECUTE FORMAT(
		$qt$
			CREATE TRIGGER %2$s_iud INSTEAD OF INSERT OR DELETE OR UPDATE ON %1$I.%2$I FOR EACH ROW EXECUTE PROCEDURE filter.auto_update(%3$s, %4$s);
		$qt$, $1, $2, $3, $4
	);

END $$;

COMMENT ON FUNCTION filter.create_auto_trigger(view_schema text, view_name text, table_schema text, table_name text) IS 'Creates a trigger with the necessary parameters to enable automatic update of views.';


-- DROP FUNCTION IF EXISTS filter.primary_key(schema_name text, table_name text, _prefix text) CASCADE;

CREATE OR REPLACE FUNCTION filter.primary_key(schema_name text, table_name text, _prefix text DEFAULT '')
	RETURNS text
	LANGUAGE plpgsql AS
$$

DECLARE

	_ret text;

BEGIN

	WITH

		cte1 AS(
			SELECT
				UNNEST(a.conkey) AS conkey
			FROM pg_catalog.pg_constraint a
			LEFT JOIN pg_catalog.pg_class b ON a.conrelid = b.oid
			LEFT JOIN pg_catalog.pg_namespace c ON b.relnamespace = c.oid
			WHERE (c.nspname, b.relname, a.contype) = ($1, $2, 'p')
		)

	SELECT
		string_agg(_prefix || a.attname, ',')
	FROM pg_catalog.pg_attribute a
	LEFT JOIN pg_catalog.pg_class b ON a.attrelid = b.oid
	LEFT JOIN pg_catalog.pg_namespace c ON b.relnamespace = c.oid
	WHERE (c.nspname, b.relname) = ($1, $2) AND attnum IN(SELECT conkey FROM cte1)
	INTO _ret;

	RETURN _ret;

END $$;

COMMENT ON FUNCTION filter.primary_key(schema_name text, table_name text, _prefix text) IS 'Lists PK column names of the specified table.';


--
-- TRIGGER FUNCTIONS
--


-- DROP FUNCTION IF EXISTS filter.filters_trg_iud();

CREATE OR REPLACE FUNCTION filter.filters_trg_iud()
	RETURNS trigger
	LANGUAGE plpgsql AS
$$

DECLARE

	d_type text;

BEGIN

--
-- DELETE
--

	IF (TG_OP = 'DELETE') THEN

--
-- Remove the related column in settings when the filter is removed
--

		EXECUTE FORMAT(
			$qt$
				ALTER TABLE filter.settings
					DROP COLUMN %s CASCADE -- Cascade because the column is used in filter.v_settings
			$qt$,
			filter.col_name(OLD.filter_key_column, OLD.filter_id)
		);

--
-- UPDATE
--

	ELSIF (TG_OP = 'UPDATE') THEN

--
-- If the value in filter_key_column has been changed (alias not included)
--

		IF filter.rem_alias(NEW.filter_key_column) != filter.rem_alias(OLD.filter_key_column) THEN

--
-- Rename column name in filter.settings to match
--

			EXECUTE FORMAT(
				$qt$
					ALTER TABLE filter.settings
						RENAME COLUMN %s TO %s
				$qt$,
				filter.col_name(OLD.filter_key_column, OLD.filter_id),
				filter.col_name(NEW.filter_key_column, NEW.filter_id)
			);

		END IF;

--
-- INSERT
--

	ELSIF (TG_OP = 'INSERT') THEN

--
-- IN-filters
-- Domain is used as data type to distinguish
--

		IF NEW.key_relation IS NULL THEN

			EXECUTE FORMAT(
				$qt$
					ALTER TABLE filter.settings
						ADD COLUMN %s filter.text
				$qt$,
				filter.col_name(NEW.filter_key_column, NEW.filter_id)
			);

--
-- Relational filter
-- Find data type of one of the columns the filter is applied on
--

		ELSE

			WITH

--
-- Find relation to one table (shouldn't matter which one)
--

				cte1 AS(
					SELECT
						UNNEST(NEW.table_id) AS table_id,
						filter.rem_alias(COALESCE(NEW.target_key_column, NEW.filter_key_column)) AS filter_key_column
					LIMIT 1
				)

--
-- Find data type of specified column
--

			SELECT
				f.typname::text
			FROM cte1 a
			LEFT JOIN filter.tables b ON a.table_id = b.table_id
			LEFT JOIN pg_catalog.pg_namespace c ON b.schema_name = c.nspname
			LEFT JOIN pg_catalog.pg_class d ON (b.table_name, c.oid) = (d.relname, d.relnamespace)
			LEFT JOIN pg_catalog.pg_attribute e ON e.attrelid = d.oid AND e.attname = a.filter_key_column
			LEFT JOIN pg_catalog.pg_type f ON f.oid = e.atttypid
			INTO d_type;

--
-- Add column
--

			EXECUTE FORMAT(
				$qt$
					ALTER TABLE filter.settings
						ADD COLUMN %s %s
				$qt$,
				filter.col_name(NEW.filter_key_column, NEW.filter_id), d_type
			);

		END IF;

	END IF;


	IF (
		CASE
			WHEN (TG_OP = 'UPDATE')
			THEN filter.rem_alias(NEW.filter_key_column) != filter.rem_alias(OLD.filter_key_column)
			ELSE TRUE
		END
	) THEN

--
-- Remake filter.v_settings
--

			EXECUTE FORMAT('%s',
				(SELECT filter.settings())
			);

	END IF;

	IF (TG_OP = 'DELETE') THEN

		RETURN OLD;

	ELSE

		RETURN NEW;

	END IF;

END $$;


-- DROP FUNCTION IF EXISTS filter.auto_update() CASCADE;

CREATE OR REPLACE FUNCTION filter.auto_update()
	RETURNS trigger
	LANGUAGE plpgsql AS
$$

DECLARE

	schema_name text := TG_ARGV[0];
	table_name text := TG_ARGV[1];
	boolean_var text;

BEGIN

	IF TG_ARGV[0] IS NULL OR TG_ARGV[1] IS NULL THEN

		RAISE EXCEPTION 'Arguments for trigger function are missing!';

	END IF;

	IF (TG_OP = 'DELETE') THEN

		EXECUTE FORMAT(
			'%s', filter.auto_delete(schema_name, table_name)
		)
		USING OLD;

		RETURN NULL;

	ELSIF (TG_OP = 'UPDATE') THEN

		EXECUTE FORMAT(
			'%s', filter.auto_check_update(TG_TABLE_SCHEMA, TG_TABLE_NAME, schema_name, table_name)
		)
		USING NEW, OLD
		INTO boolean_var;

		IF boolean_var = '1' THEN

			RETURN NULL;

		END IF;

		EXECUTE FORMAT(
			'%s', filter.auto_update(TG_TABLE_SCHEMA, TG_TABLE_NAME, schema_name, table_name)
		)
		USING NEW, OLD;

		RETURN NULL;

	ELSIF (TG_OP = 'INSERT') THEN

		EXECUTE FORMAT(
			'%s', filter.auto_insert(TG_TABLE_SCHEMA, TG_TABLE_NAME, schema_name, table_name)
		)
		USING NEW;

		RETURN NULL;

	END IF;

END $$;


-- DROP FUNCTION IF EXISTS filter.auto_update_settings() CASCADE;

CREATE OR REPLACE FUNCTION filter.auto_update_settings()
	RETURNS trigger
	LANGUAGE plpgsql AS
$$

DECLARE

	schema_name text := TG_ARGV[0];
	table_name text := TG_ARGV[1];
	boolean_var text;

BEGIN

	IF TG_ARGV[0] IS NULL OR TG_ARGV[1] IS NULL THEN

		RAISE EXCEPTION 'Arguments for trigger function are missing!';

	END IF;

	IF (TG_OP = 'DELETE') THEN

		RETURN NULL;

	END IF;

--
-- Has the user been created in the settings table
--

	IF EXISTS(
		SELECT
			'1'
		FROM filter.settings
		WHERE rolname = current_user
	) THEN

		EXECUTE FORMAT(
			'%s',
				regexp_replace(
					regexp_replace(
						filter.auto_update(TG_TABLE_SCHEMA, TG_TABLE_NAME, schema_name, table_name), E'***=rolname = $1.rolname,\n\t\t', '' -- Remove update of rolname
					), E'***=$2.rolname', 'current_user' -- Change OLD.rolname to current_user, so it works with INSERT as well
				)
		)
		USING NEW;

--
-- Insert row instead
--

	ELSE

		EXECUTE FORMAT(
			'%s', filter.auto_insert(TG_TABLE_SCHEMA, TG_TABLE_NAME, schema_name, table_name)
		)
		USING NEW;

	END IF;

	RETURN NULL;

END $$;


--
-- TABLES
--


-- DROP TABLE IF EXISTS filter.view_storage CASCADE;

CREATE TABLE filter.view_storage(
	sql_storage text NOT NULL,
	CONSTRAINT view_storage_pk PRIMARY KEY (sql_storage) WITH (fillfactor='10')
);

COMMENT ON TABLE filter.view_storage IS 'SQL for additional columns in filter.v_settings view. Example:

CASE
	WHEN geometri_tjek IS FALSE AND geometri_date = current_date
	THEN geometri_tjek
	ELSE TRUE
END AS geometri_tjek_2,
CASE
	WHEN geometri_tjek IS FALSE AND geometri_date = current_date
	THEN geometri_date
	ELSE NULL::date
END AS geometri_date_2

The example adds two dditional columns to the v_settings view every time it is updated.';


-- DROP TABLE IF EXISTS filter.settings CASCADE;

-- Feel free to add more settings to be applied on a user level

CREATE TABLE filter.settings(
	rolname text NOT NULL,
	filter boolean NOT NULL DEFAULT FALSE,
	CONSTRAINT settings_pk PRIMARY KEY (rolname) WITH (fillfactor='10')
);

COMMENT ON TABLE filter.settings IS 'Settings to be applied on a user level.';


-- DROP TABLE IF EXISTS filter.tables CASCADE;

CREATE TABLE filter.tables(
	table_id serial NOT NULL,
	schema_name text NOT NULL,
	table_name text NOT NULL,
	pk_columns text, -- csv
	filter_join text, -- JOINS for indirect relations
	CONSTRAINT tables_pk PRIMARY KEY (table_id) WITH (fillfactor='10')
);

COMMENT ON TABLE filter.tables IS 'Tables to be filtered.';
COMMENT ON COLUMN filter.tables.pk_columns IS 'Primary key columns as CSV (No spaces). If left out, metadata will be searched to find it when applying filters. Will fail if primary key can''t be found in metadata, i.e. views.';
COMMENT ON COLUMN filter.tables.filter_join IS 'LEFT JOIN-SQL to expand table to include columns in indirect relations. The table in focus will be a as alias.';


-- DROP TABLE IF EXISTS filter.filters CASCADE;

CREATE TABLE filter.filters(
	filter_id serial NOT NULL,
	table_id integer[], -- Array for tables to apply filter
	label text NOT NULL, -- For overview
	label_level integer NOT NULL,
	filter_schema text,
	filter_table text,
	filter_key_column text NOT NULL, -- Code
	filter_value_column text, -- Description
	key_relation text,
	target_key_column text,
	CONSTRAINT filters_pk PRIMARY KEY (filter_id) WITH (fillfactor='10')
);

COMMENT ON TABLE filter.filters IS 'Filters to be applied.';
COMMENT ON COLUMN filter.filters.table_id IS 'Integer-array to which tables the filter is applied.';
COMMENT ON COLUMN filter.filters.label IS 'Label to define the individual filter.';
COMMENT ON COLUMN filter.filters.label_level IS 'Grouping of the labels. 1 being ''closest to the data set''';
COMMENT ON COLUMN filter.filters.filter_key_column IS 'Column to filter on the table. Use alias when filtering columns related to a filter.';
COMMENT ON COLUMN filter.filters.filter_value_column IS 'Values for label, can be SQL concatenation of several columns.';
COMMENT ON COLUMN filter.filters.key_relation IS 'Use other than a table to define filter, relational filter. E.g. = and >';
COMMENT ON COLUMN filter.filters.target_key_column IS 'Use this column if the column names doesnt''t match up.';

CREATE TRIGGER filters_trg_iud BEFORE INSERT OR UPDATE OR DELETE ON filter.filters FOR EACH ROW EXECUTE PROCEDURE filter.filters_trg_iud();


--
-- VIEWS
--


-- DROP VIEW IF EXISTS filter.v_settings CASCADE;

DO $$

BEGIN

	EXECUTE FORMAT('%s',
		(SELECT filter.settings())
	);

END $$;


-- DROP VIEW IF EXISTS filter.v_tables CASCADE;

CREATE VIEW filter.v_tables AS

SELECT
	*,
	schema_name || '.' || table_name AS _table
FROM filter.tables;

CREATE TRIGGER v_tables_trg_iud INSTEAD OF INSERT OR DELETE OR UPDATE ON filter.v_tables FOR EACH ROW EXECUTE PROCEDURE filter.auto_update('filter', 'tables');





--
-- MODIFICATIONS TO MODULE FILTER
--


-- DROP FUNCTION IF EXISTS filter.auto_update_settings() CASCADE;

CREATE OR REPLACE FUNCTION filter.auto_update_settings()
	RETURNS trigger
	LANGUAGE plpgsql
	SECURITY DEFINER AS
$$

DECLARE

	schema_name text := TG_ARGV[0];
	table_name text := TG_ARGV[1];
	boolean_var text;

BEGIN

	IF TG_ARGV[0] IS NULL OR TG_ARGV[1] IS NULL THEN

		RAISE EXCEPTION 'Arguments for trigger function are missing!';

	END IF;

	IF (TG_OP = 'DELETE') THEN

		RETURN NULL;

	END IF;

	NEW.geometri_tjek = NEW.geometri_tjek_2;

	IF NEW.geometri_tjek IS FALSE THEN

		NEW.geometri_date = current_date;

	END IF;

--
-- Has the user been created in the settings table
--

	IF EXISTS(
		SELECT
			'1'
		FROM filter.settings
		WHERE rolname = session_user
	) THEN

		EXECUTE FORMAT(
			'%s',
				regexp_replace(
					regexp_replace(
						filter.auto_update(TG_TABLE_SCHEMA, TG_TABLE_NAME, schema_name, table_name), E'***=rolname = $1.rolname,\n\t\t', '' -- Remove update of rolname
					), E'***=$2.rolname', 'session_user' -- Change OLD.rolname to current_user, so it works with INSERT as well
				)
		)
		USING NEW;

--
-- Insert row instead
--

	ELSE

		EXECUTE FORMAT(
			'%s',
				regexp_replace(
					filter.auto_insert('filter', 'v_settings', 'filter', 'settings'), E'***=$1.rolname', 'session_user' -- Change OLD.rolname to current_user, so it works with INSERT as well
				)
		)
		USING NEW;

	END IF;

	RETURN NULL;

END $$;



INSERT INTO filter.view_storage
	VALUES ('CASE
	WHEN geometri_tjek IS FALSE AND geometri_date = current_date -- Only if geometri_tjek was ticked off on the current date will it be FALSE
	THEN geometri_tjek
	ELSE TRUE
END AS geometri_tjek_2,
CASE
	WHEN geometri_tjek IS FALSE AND geometri_date = current_date -- When geometri_tjek is ticked off the date will be shown
	THEN geometri_date
	ELSE NULL::date
END AS geometri_date_2,
(SELECT filter.label()) || (SELECT COALESCE(E''\n'' || repeat(''_'', LENGTH(''Login: '' || _value)) || E''\n'' || ''Login: '' || _value, '''') FROM filter._value(''rolname'')) AS label');


ALTER TABLE filter.settings
	ADD COLUMN navn character varying(128) NOT NULL,
	ADD COLUMN geometri_tjek boolean DEFAULT TRUE NOT NULL,
	ADD COLUMN geometri_date date,
	ADD COLUMN geometri_aggresive boolean DEFAULT FALSE NOT NULL,
	ADD COLUMN maengder boolean DEFAULT FALSE NOT NULL,
	ADD COLUMN historik timestamp without time zone DEFAULT current_timestamp(0)::timestamp without time zone NOT NULL,
	ADD COLUMN log_ integer DEFAULT EXTRACT(YEAR FROM current_date)::integer NOT NULL,
	ADD COLUMN aendringer integer DEFAULT 14,
	ADD COLUMN picture text DEFAULT 'logo\logo.gif' NOT NULL,
	ADD COLUMN composer text DEFAULT E'Kommunenavn\nAdresse\nPostnr' NOT NULL,
	ADD COLUMN cvr integer DEFAULT 29189129,
	ADD COLUMN oprind integer DEFAULT 0 NOT NULL,
	ADD COLUMN status integer DEFAULT 0 NOT NULL,
	ADD COLUMN off_ integer DEFAULT 1 NOT NULL,
	ADD COLUMN tilstand integer DEFAULT 9 NOT NULL,
	ADD COLUMN omr_red boolean DEFAULT TRUE;


INSERT INTO filter.settings(rolname, navn) VALUES
	('postgres', 'Administrator');

INSERT INTO filter.tables(schema_name, table_name, pk_columns) VALUES
	('greg', 'v_data_flader', 'versions_id'),
	('greg', 'v_data_linier', 'versions_id'),
	('greg', 'v_data_punkter', 'versions_id'),
	('basis', 'omraader', 'pg_distrikt_nr'),
	('basis', 'v_prep_delomraader', 'id'),
	('basis', 'v_afdelinger', 'afdeling_nr');

INSERT INTO filter.filters(table_id, label, label_level, filter_schema, filter_table, filter_key_column, filter_value_column, key_relation, target_key_column) VALUES
	('{1,2,3,4,5,6}', 'Afdeling', 3, 'basis', 'v_afdelinger', 'afdeling_nr', 'label', NULL, NULL),
	('{1,2,3,4,5}', 'Område', 2, 'basis', 'v_omraader', 'pg_distrikt_nr', 'omraade', NULL, NULL),
	('{1,2,3,4,5}', 'Distriktype', 2, 'basis', 'v_distrikt_type', 'pg_distrikt_type_kode', 'label', NULL, NULL),
	('{1,2,3,4,5}', 'Postnr.', 2, 'basis', 'v_postnr', 'postnr', 'label', NULL, NULL),
	('{1,2,3,4,5}', 'Udfører', 1, 'basis', 'v_udfoerer', 'udfoerer_kode', 'label', NULL, NULL),
	('{1,2,3}', 'Ansvarlig myndighed', 1, 'basis', 'v_ansvarlig_myndighed', 'cvr_kode', 'label', NULL, NULL),
	('{1,2,3}', 'Status', 1, 'basis', 'v_status', 'statuskode', 'label', NULL, NULL),
	('{1,2,3}', 'Offentlig', 1, 'basis', 'v_offentlig', 'off_kode', 'label', NULL, NULL),
	('{1,2,3}', 'Udførende entrep.', 1, 'basis', 'v_udfoerer_entrep', 'udfoerer_entrep_kode', 'label', NULL, NULL),
	('{1,2,3}', 'Element, niv. 1 mm.', 1, 'basis', 'v_elementer_1', 'element_kode_def', 'label', NULL, 'element_kode_1'),
	('{1,2,3}', 'Element, niv. 2 mm.', 1, 'basis', 'v_elementer_2', 'element_kode_def', 'label', NULL, 'element_kode_2'),
	('{1,2,3}', 'Element', 1, 'basis', 'v_elementer', 'element_kode_def', 'label', NULL, 'element_kode_3');


DO $$

BEGIN

	EXECUTE FORMAT('%s',
		(SELECT filter.settings())
	);

END $$;


-- Developed for PostgreSQL vers. 10 in combination with QGIS vers. 3
--
-- This script creates a schema, roles, in a PostgreSQL database.
--
-- The purpose of this script is to create an overview as well as manage role properties and relations on the database server and privileges in the given database.
--
-- With the script follows a bundle of files giving the ability to easily import the script, both temporaily and permanently, into any database
-- as well as creating a corresponding QGIS-project linked to the database.
--
-- This script is created by:
--		Casper Bertelsen Jensen
--		casperbj94@gmail.com


--
-- SCHEMAS
--

DROP SCHEMA IF EXISTS roles CASCADE;

CREATE SCHEMA roles;

COMMENT ON SCHEMA roles IS 'Role and privilege management in the current database.';


--
-- FUNCTIONS
--


-- DROP FUNCTION IF EXISTS roles.array_to_csv(_array anyarray, container text, leave_out text) CASCADE;

CREATE OR REPLACE FUNCTION roles.array_to_csv(_array anyarray, container text default '', leave_out text default '')
	RETURNS text
	LANGUAGE plpgsql AS
$$

DECLARE

	_return text;

BEGIN

	WITH
	cte1 AS( -- Unnest array into rows
		SELECT DISTINCT
			unnest($1) AS rolname, -- One row with the actual name
			$2 || unnest($1) || $2 AS unnest -- One row with name contained in container
	),

	cte2 AS(
		SELECT
			unnest(('{' || $3 || '}')::text[]) -- make roles to leave out into rows for comparison
	)

	SELECT
		string_agg(unnest,',') -- to csv
	FROM cte1
	WHERE rolname NOT IN(SELECT * FROM cte2) -- Exclude roles to leave out
	INTO _return;

	RETURN _return;

END $$;

COMMENT ON FUNCTION roles.array_to_csv(_array anyarray, container text, leave_out text) IS 'Converts an array into a list of comma-seperated values with the option to contain each value with a given symbol and leave out certain values.';


-- DROP FUNCTION IF EXISTS roles.group_relation(rol text) CASCADE;

CREATE OR REPLACE FUNCTION roles.group_relation(rol text)
	RETURNS TABLE (
		relations text
	)
	LANGUAGE plpgsql AS
$$

DECLARE

	i integer;
	_cte text;
	_union text;

BEGIN

	_cte := FORMAT( -- Select all the groups in which the specified role is a member
		$qt$

			WITH

				cte1 AS(
					SELECT
						(('{' || string_agg(c.rolname, ',') || '}')::text[]) AS grouprol
					FROM pg_catalog.pg_auth_members a
					LEFT JOIN pg_catalog.pg_authid b ON a.roleid = b.oid -- The group
					LEFT JOIN pg_catalog.pg_authid c ON a.member = c.oid -- The role that is the member of the group
					WHERE c.rolname NOT IN(SELECT role FROM roles.filter_role) AND c.rolinherit IS TRUE AND b.rolname = '%1$s'
				)

		$qt$, $1
	);

	_union := FORMAT( -- The last query where the result of all other CTE's is combined into a distinct list of relations
		$qt$

			SELECT
				'%1$s' AS grouprol

			UNION

			SELECT
				unnest(grouprol) AS grouprol
			FROM cte1

		$qt$, $1);

	FOR i IN 2..(SELECT COUNT(*) FROM pg_auth_members) LOOP

		_cte := _cte || FORMAT( -- Select all the groups in which the groups of the specified role is a member, and the groups of the groups.. and so on
			$qt$

				,cte%1$s AS(
					SELECT
						(('{' || string_agg(c.rolname, ',') || '}')::text[]) AS grouprol
					FROM pg_catalog.pg_auth_members a
					LEFT JOIN pg_catalog.pg_authid b ON a.roleid = b.oid -- The group
					LEFT JOIN pg_catalog.pg_authid c ON a.member = c.oid -- The role that is the member of the group
					WHERE c.rolname NOT IN(SELECT role FROM roles.filter_role) AND c.rolinherit IS TRUE AND b.rolname IN(SELECT unnest(grouprol) FROM cte%2$s) -- For all related groups found in the previous CTE
				)

			$qt$, i, i-1
		);

		_union := _union || FORMAT( -- Building the UNION
			$qt$

				UNION

				SELECT
					unnest(grouprol) AS grouprol
				FROM cte%1$s

			$qt$, i
		);

	END LOOP;

	RETURN QUERY
	EXECUTE FORMAT(
		$qt$

			%1$s -- The CTEs
			%2$s -- The UNIONs
			ORDER BY grouprol

		$qt$, _cte, _union
	);

END $$;

COMMENT ON FUNCTION roles.group_relation(rol text) IS 'Finds all relations to other roles, where this role is the group, on the database server, no matter how indirect. It doesn''t include relationships without INHERIT.';


-- DROP FUNCTION IF EXISTS roles.privilege_interpret(acl text) CASCADE;

CREATE OR REPLACE FUNCTION roles.privilege_interpret(acl text)
	RETURNS TABLE (
		privilege text
	)
	LANGUAGE plpgsql AS
$$

BEGIN

	RETURN QUERY
	EXECUTE FORMAT(
		$qt$

			WITH

				cte1 AS(
					SELECT
						regexp_replace(privilege, '\*', '') AS _dist, -- Remove asterisks for distinct later
						privilege
					FROM roles.privilege
					WHERE privilege = SUBSTRING('***=' || '%s', '***=' || privilege) -- Where a substring of all privileges matches the individual privileges, with regex string literal due to asterisks
					ORDER BY CASE -- Make order for grant option over no grant option
								WHEN substring(privilege, '.*(\*).*') IS NOT NULL
								THEN 1
								ELSE 2
							END
				)

				SELECT DISTINCT ON(_dist) -- Remove duplacte priviliges
					privilege
				FROM cte1
				ORDER BY _dist

		$qt$, $1
	);


END $$;

COMMENT ON FUNCTION roles.privilege_interpret(acl text) IS 'Function that interprets acl items based on values in a "translation"-table.';


-- DROP FUNCTION IF EXISTS roles.privilege_overview(rolname text) CASCADE;

CREATE OR REPLACE FUNCTION roles.privilege_overview(rolname text)
	RETURNS TABLE (
		overview text
	)
	LANGUAGE plpgsql AS
$$

BEGIN


	RETURN QUERY
	WITH

		-------------------- DATABASE ----------------------

		db_cte1 AS(
			SELECT
				UNNEST(datacl::text[])AS val -- UNNEST privileges for roles into individual rows
			FROM pg_catalog.pg_database
			WHERE datname = current_database() -- For current database only
		),

		db_cte2 AS(
			SELECT
				CASE -- Extract rolename from acl
					WHEN regexp_replace(val, '=.*', '') = '' -- If extract is an empty string the role is PUBLIC
					THEN 'PUBLIC'
					ELSE regexp_replace(val, '=.*', '')
				END AS rolname,
				roles.privilege_interpret( -- Interpret the privileges from the acl
					SUBSTRING(
						val, '=(.*)/'
					)
				) AS val
			FROM db_cte1
		),

		db_cte3 AS(
			SELECT DISTINCT ON(b.sql) -- Unique list based on the SQL used to GRANT/REVOKE privilege
				a.rolname,
				CASE
					WHEN a.rolname = $1
					THEN b.description
					ELSE b.description || '*' -- Asterisk indicates the privilege is granted indirectly by being a member of a group with that privilege
				END AS val,
				_order
			FROM db_cte2 a
			LEFT JOIN roles.privilege b ON a.val = b.privilege -- Join the actual privileges based on the acl-codes
			WHERE a.rolname IN(SELECT roles.usr_relation($1)) -- Find only for roles that the specified role is related to
			ORDER BY b.sql,
					CASE -- GRANT OPTION over no GRANT OPTION
						WHEN b.description ILIKE '%with grant option'
						THEN 1
						ELSE 2
					END,
					CASE -- Privileges given to the specified role over group privileges
						WHEN a.rolname = $1
						THEN 1
						ELSE 2
					END
		),

		db_cte AS(
			SELECT
				string_agg(val, E'\n' ORDER BY _order) AS privilege
			FROM db_cte3
		),

		-------------------- DATABASE HTML --------------------

		pre_db AS(
			SELECT
				FORMAT(
					'%s',
					'<tr><th colspan="4"><br>DATABASE<br><br></th></tr><tr><td colspan="4">' || regexp_replace(privilege, E'\n', '<br>', 'g') || '</td></tr>'
				)
			FROM db_cte
		),

		-------------------- SCHEMA --------------------

		sch_cte1 AS(
			SELECT
				nspname::text AS _schema,
				UNNEST(nspacl::text[]) AS val -- UNNEST privileges for roles into individual rows
			FROM pg_catalog.pg_namespace
			WHERE nspname NOT IN(SELECT schema FROM roles.filter_schema) -- Only schemas not present in the filter
		),

		sch_cte2 AS(
			SELECT
				_schema,
				CASE -- Extract rolename from acl
					WHEN regexp_replace(val, '=.*', '') = '' -- If extract is an empty string the role is PUBLIC
					THEN 'PUBLIC'
					ELSE regexp_replace(val, '=.*', '')
				END AS rolname,
				roles.privilege_interpret(
					SUBSTRING( -- Interpret the privileges from the acl
						val, '=(.*)/'
					)
				) AS val
			FROM sch_cte1
		),

		sch_cte3 AS(
			SELECT DISTINCT ON(a._schema, b.sql) -- Unique list based on the schema and the SQL used to GRANT/REVOKE privilege
				a._schema,
				a.rolname,
				CASE
					WHEN a.rolname = $1
					THEN b.description
					ELSE b.description || '*' -- Asterisk indicates the privilege is granted indirectly by being a member of a group with that privilege
				END AS val,
				b._order
			FROM sch_cte2 a
			LEFT JOIN roles.privilege b ON a.val = b.privilege -- Join the actual privileges based on the acl-codes
			WHERE a.rolname IN(SELECT roles.usr_relation($1)) -- Find only for roles that the specified role is related to
			ORDER BY a._schema,
					b.sql,
					CASE -- GRANT OPTION over no GRANT OPTION
						WHEN b.description ILIKE '%with grant option'
						THEN 1
						ELSE 2
					END,
					CASE -- Privileges given to the specified role over group privileges
						WHEN a.rolname = $1
						THEN 1
						ELSE 2
					END
		),

		sch_cte4 AS( -- String aggregate tables based on schemas
			SELECT
				string_agg(a._schema, ',' ORDER BY a._schema) AS val
			FROM roles.o_schemas a
			WHERE a._schema != 'ALL'
		),

		sch_cte5 AS(
			SELECT
				string_agg(a._schema, ',' ORDER BY a._schema) AS val,
				a.val AS privilege
			FROM sch_cte3 a
			GROUP BY a.val
		),

		sch_cte6 AS(
			SELECT DISTINCT
				CASE
					WHEN b.val = c.val
					THEN 'ALL'
					ELSE a._schema
				END AS _schema,
				a.val,
				a._order
			FROM sch_cte3 a
			LEFT JOIN sch_cte4 b ON TRUE
			LEFT JOIN sch_cte5 c ON (a.val) = (c.privilege)
		),

		sch_cte AS(
			SELECT
				a._schema,
				string_agg(a.val, E'\n' ORDER BY _order) AS privilege
			FROM sch_cte6 a
			GROUP BY a._schema
			ORDER BY CASE
						WHEN a._schema = 'ALL'
						THEN 1
						ELSE 2
					END,
					a._schema
		),

		-------------------- SCHEMA HTML --------------------

		pre_sch AS(
			SELECT
				FORMAT(
					'%s',
					'<tr><th colspan="4"><br>SCHEMAS<br><br></th></tr><tr><td colspan="3">' || string_agg(_schema || '</td><td>' || regexp_replace(privilege, E'\n', '<br>', 'g'), '</td></tr><tr><td colspan="3">') || '</td></tr>'
				)
			FROM sch_cte
		),

		-------------------- TABLE --------------------

		tbl_cte1 AS(
			SELECT
				b.nspname::text AS _schema,
				a.relname AS _table,
				unnest(a.relacl::text[])AS val -- UNNEST privileges for roles into individual rows
			FROM pg_catalog.pg_class a
			LEFT JOIN pg_catalog.pg_namespace b ON a.relnamespace = b.oid
			WHERE b.nspname NOT IN(SELECT schema FROM roles.filter_schema) AND a.relkind IN('r', 'v') -- Only schemas not present in the filter
		),

		tbl_cte2 AS(
			SELECT
				_schema,
				_table,
				CASE -- Extract rolename from acl
					WHEN regexp_replace(val, '=.*', '') = '' -- If extract is an empty string the role is PUBLIC
					THEN 'PUBLIC'
					ELSE regexp_replace(val, '=.*', '')
				END AS rolname,
				roles.privilege_interpret(
					SUBSTRING( -- Interpret the privileges from the acl
						val, '=(.*)/'
					)
				) AS val
			FROM tbl_cte1
		),

		tbl_cte3 AS(
			SELECT DISTINCT ON(a._schema, a._table, b.sql) -- Unique list based on the schema and the SQL used to GRANT/REVOKE privilege
				a._schema,
				a._table,
				a.rolname,
				CASE
					WHEN a.rolname = $1
					THEN b.description
					ELSE b.description || '*' -- Asterisk indicates the privilege is granted indirectly by being a member of a group with that privilege
				END AS val,
				b._order
			FROM tbl_cte2 a
			LEFT JOIN roles.privilege b ON a.val = b.privilege -- Join the actual privileges based on the acl-codes
			WHERE a.rolname IN(SELECT roles.usr_relation($1)) -- Find only for roles that the specified role is related to
			ORDER BY a._schema,
					a._table,
					b.sql,
					CASE -- GRANT OPTION over no GRANT OPTION
						WHEN b.description ILIKE '%with grant option'
						THEN 1
						ELSE 2
					END,
					CASE -- Privileges given to the specified role over group privileges
						WHEN a.rolname = $1
						THEN 1
						ELSE 2
					END
		),

		tbl_cte4 AS( -- String aggregate tables based on schemas
			SELECT
				a._schema,
				string_agg(a._table, ',' ORDER BY a._table) AS val
			FROM roles.o_tables a
			WHERE a._table != 'ALL'
			GROUP BY a._schema
		),

		tbl_cte5 AS(
			SELECT
				a._schema,
				string_agg(a._table, ',' ORDER BY a._table) AS val,
				a.val AS privilege
			FROM tbl_cte3 a
			GROUP BY a._schema, a.val
		),

		tbl_cte6 AS(
			SELECT DISTINCT
				a._schema,
				CASE
					WHEN b.val = c.val
					THEN 'ALL'
					ELSE a._table
				END AS _table,
				a.val,
				a._order
			FROM tbl_cte3 a
			LEFT JOIN tbl_cte4 b ON a._schema = b._schema
			LEFT JOIN tbl_cte5 c ON (a._schema, a.val) = (c._schema, c.privilege)
		),

		tbl_cte AS(
			SELECT
				a._schema,
				a._table,
				string_agg(a.val, E'\n' ORDER BY _order) AS privilege
			FROM tbl_cte6 a
			GROUP BY a._schema, a._table
			ORDER BY a._schema,
					CASE
						WHEN a._table = 'ALL'
						THEN 1
						ELSE 2
					END,
					a._table
		),

		-------------------- TABLE HTML --------------------

		pre_tbl_1 AS(
			SELECT
				_schema,
				COUNT(*) AS count_schema
			FROM tbl_cte
			GROUP BY _schema
		),

		pre_tbl_2 AS(
			SELECT
				_schema,
				FORMAT(
					'<td colspan="2">%s</td>',
					string_agg(_table || '</td><td>' || regexp_replace(privilege, E'\n', '<br>', 'g'), '</td></tr><tr><td colspan="2">')
				) AS _body
			FROM tbl_cte
			GROUP BY _schema
			ORDER BY _schema
		),

		pre_tbl AS(
			SELECT
				FORMAT(
					'%s',
					'<tr><th colspan="4"><br>TABLES<br><br></th></tr><tr><th rowspan="' || string_agg(b.count_schema || '">TABLES<br>IN SCHEMA<br><br>' || a._schema || '</th>' || a._body,  '</tr><tr><th rowspan="') || '</th></tr>'
				)
			FROM pre_tbl_2 a
			LEFT JOIN pre_tbl_1 b ON a._schema = b._schema
		),

		-------------------- COLUMN --------------------

		cl_cte1 AS(
			SELECT
				c.nspname::text AS _schema,
				b.relname AS _table,
				a.attname AS _column,
				unnest(a.attacl::text[])AS val -- UNNEST privileges for roles into individual rows
			FROM pg_catalog.pg_attribute a
			LEFT JOIN pg_catalog.pg_class b ON a.attrelid = b.oid
			LEFT JOIN pg_catalog.pg_namespace c ON b.relnamespace = c.oid
			WHERE c.nspname NOT IN(SELECT schema FROM roles.filter_schema) AND b.relkind IN('r', 'i') -- Only schemas not present in the filter
		),

		cl_cte2 AS(
			SELECT
				_schema,
				_table,
				_column,
				CASE -- Extract rolename from acl
					WHEN regexp_replace(val, '=.*', '') = '' -- If extract is an empty string the role is PUBLIC
					THEN 'PUBLIC'
					ELSE regexp_replace(val, '=.*', '')
				END AS rolname,
				roles.privilege_interpret(
					SUBSTRING( -- Interpret the privileges from the acl
						val, '=(.*)/'
					)
				) AS val
			FROM cl_cte1
		),

		cl_cte3 AS(
			SELECT DISTINCT ON(a._schema, a._table, a._column, b.sql) -- Unique list based on the schema and the SQL used to GRANT/REVOKE privilege
				a._schema,
				a._table,
				a._column,
				a.rolname,
				CASE
					WHEN a.rolname = $1
					THEN b.description
					ELSE b.description || '*' -- Asterisk indicates the privilege is granted indirectly by being a member of a group with that privilege
				END AS val,
				b._order
			FROM cl_cte2 a
			LEFT JOIN roles.privilege b ON a.val = b.privilege -- Join the actual privileges based on the acl-codes
			WHERE a.rolname IN(SELECT roles.usr_relation($1)) -- Find only for roles that the specified role is related to
			ORDER BY a._schema,
					a._table,
					a._column,
					b.sql,
					CASE -- GRANT OPTION over no GRANT OPTION
						WHEN b.description ILIKE '%with grant option'
						THEN 1
						ELSE 2
					END,
					CASE -- Privileges given to the specified role over group privileges
						WHEN a.rolname = $1
						THEN 1
						ELSE 2
					END
		),

		cl_cte AS(
			SELECT
				a._schema,
				a._table,
				a._column,
				string_agg(a.val, E'\n' ORDER BY _order) AS privilege
			FROM cl_cte3 a
			GROUP BY a._schema, a._table, a._column
			ORDER BY a._schema, a._table, a._column
		),

		-------------------- COLUMN HTML --------------------

		pre_cl_1 AS(
			SELECT
				_schema,
				COUNT(*) AS count_schema
			FROM cl_cte
			GROUP BY _schema
		),

		pre_cl_2 AS(
			SELECT
				_schema,
				_table,
				COUNT(*) AS count_table
			FROM cl_cte
			GROUP BY _schema, _table
		),

		pre_cl_3 AS(
			SELECT
				_schema,
				_table,
				FORMAT(
					'<td>%s</td>',
					string_agg(_column || '</td><td>' || regexp_replace(privilege, E'\n', '<br>', 'g'), '</td></tr><tr><td>')
				) AS _body
			FROM cl_cte
			GROUP BY _schema, _table
			ORDER BY _schema, _table
		),

		pre_cl_4 AS(
			SELECT
				a._schema,
				FORMAT(
					'<th rowspan="%s</th>',
					string_agg(b.count_table || '">COLUMNS<br>IN TABLE<br><br>' || a._table || '</th>' || a._body, '</tr><tr><th rowspan="')
				) AS _body
			FROM pre_cl_3 a
			LEFT JOIN pre_cl_2 b ON (a._schema, a._table) = (b._schema, b._table)
			GROUP BY a._schema
			ORDER BY a._schema
		),

		pre_cl AS(
			SELECT
				FORMAT(
					'%s',
					'<tr><th colspan="4"><br>COLUMNS<br><br></th></tr><tr><th rowspan="' || string_agg(b.count_schema || '">TABLES<br>IN SCHEMA<br><br>' || a._schema || '</th>' || a._body,  '</tr><tr><th rowspan="') || '</th></tr>'
				)
			FROM pre_cl_4 a
			LEFT JOIN pre_cl_1 b ON a._schema = b._schema
		),

		-------------------- FUNCTION --------------------

		func_cte1 AS(
			SELECT
				b.nspname::text AS _schema,
				a.proname || '(' || COALESCE(pg_catalog.pg_get_function_identity_arguments(a.oid), '') || ')' AS _function,
				unnest(a.proacl::text[])AS val -- UNNEST privileges for roles into individual rows
			FROM pg_catalog.pg_proc a
			LEFT JOIN pg_catalog.pg_namespace b ON a.pronamespace = b.oid
			WHERE b.nspname NOT IN(SELECT schema FROM roles.filter_schema UNION SELECT 'public') -- Only schemas not present in the filter
		),

		func_cte2 AS(
			SELECT
				_schema,
				_function,
				CASE -- Extract rolename from acl
					WHEN regexp_replace(val, '=.*', '') = '' -- If extract is an empty string the role is PUBLIC
					THEN 'PUBLIC'
					ELSE regexp_replace(val, '=.*', '')
				END AS rolname,
				roles.privilege_interpret(
					SUBSTRING( -- Interpret the privileges from the acl
						val, '=(.*)/'
					)
				) AS val
			FROM func_cte1
		),

		func_cte3 AS(
			SELECT DISTINCT ON(a._schema, a._function, b.sql) -- Unique list based on the schema and the SQL used to GRANT/REVOKE privilege
				a._schema,
				a._function,
				a.rolname,
				CASE
					WHEN a.rolname = $1
					THEN b.description
					ELSE b.description || '*' -- Asterisk indicates the privilege is granted indirectly by being a member of a group with that privilege
				END AS val,
				b._order
			FROM func_cte2 a
			LEFT JOIN roles.privilege b ON a.val = b.privilege -- Join the actual privileges based on the acl-codes
			WHERE a.rolname IN(SELECT roles.usr_relation($1)) -- Find only for roles that the specified role is related to
			ORDER BY a._schema,
					a._function,
					b.sql,
					CASE -- GRANT OPTION over no GRANT OPTION
						WHEN b.description ILIKE '%with grant option'
						THEN 1
						ELSE 2
					END,
					CASE -- Privileges given to the specified role over group privileges
						WHEN a.rolname = $1
						THEN 1
						ELSE 2
					END
		),

		func_cte4 AS( -- String aggregate tables based on schemas
			SELECT
				a._schema,
				string_agg(a._function, ',' ORDER BY a._function) AS val
			FROM roles.o_functions a
			WHERE a._function != 'ALL'
			GROUP BY a._schema
		),

		func_cte5 AS(
			SELECT
				a._schema,
				string_agg(a._function, ',' ORDER BY a._function) AS val,
				a.val AS privilege
			FROM func_cte3 a
			GROUP BY a._schema, a.val
		),

		func_cte6 AS(
			SELECT DISTINCT
				a._schema,
				CASE
					WHEN b.val = c.val
					THEN 'ALL'
					ELSE a._function
				END AS _function,
				a.val,
				a._order
			FROM func_cte3 a
			LEFT JOIN func_cte4 b ON a._schema = b._schema
			LEFT JOIN func_cte5 c ON (a._schema, a.val) = (c._schema, c.privilege)
		),

		func_cte AS(
			SELECT
				a._schema,
				a._function,
				string_agg(a.val, E'\n' ORDER BY _order) AS privilege
			FROM func_cte6 a
			GROUP BY a._schema, a._function
			ORDER BY a._schema,
					CASE
						WHEN a._function = 'ALL'
						THEN 1
						ELSE 2
					END,
					a._function
		),

		-------------------- FUNCTION HTML --------------------

		pre_func_1 AS(
			SELECT
				_schema,
				COUNT(*) AS count_schema
			FROM func_cte
			GROUP BY _schema
		),

		pre_func_2 AS(
			SELECT
				_schema,
				FORMAT(
					'<td colspan="2">%s</td>',
					string_agg(_function || '</td><td>' || regexp_replace(privilege, E'\n', '<br>', 'g'), '</td></tr><tr><td colspan="2">')
				) AS _body
			FROM func_cte
			GROUP BY _schema
			ORDER BY _schema
		),

		pre_func AS(
			SELECT
				FORMAT(
					'%s',
					'<tr><th colspan="4"><br>FUNCTIONS<br><br></th></tr><tr><th rowspan="' || string_agg(b.count_schema || '">FUNCTIONS<br>IN SCHEMA<br><br>' || a._schema || '</th>' || a._body,  '</tr><tr><th rowspan="') || '</th></tr>'
				)
			FROM pre_func_2 a
			LEFT JOIN pre_func_1 b ON a._schema = b._schema
		),

		-------------------- SEQUENCE --------------------

		seq_cte1 AS(
			SELECT
				b.nspname::text AS _schema,
				a.relname AS _sequence,
				unnest(a.relacl::text[])AS val -- UNNEST privileges for roles into individual rows
			FROM pg_catalog.pg_class a
			LEFT JOIN pg_catalog.pg_namespace b ON a.relnamespace = b.oid
			WHERE b.nspname NOT IN(SELECT schema FROM roles.filter_schema) AND a.relkind = 'S' -- Only schemas not present in the filter
		),

		seq_cte2 AS(
			SELECT
				_schema,
				_sequence,
				CASE -- Extract rolename from acl
					WHEN regexp_replace(val, '=.*', '') = '' -- If extract is an empty string the role is PUBLIC
					THEN 'PUBLIC'
					ELSE regexp_replace(val, '=.*', '')
				END AS rolname,
				roles.privilege_interpret(
					SUBSTRING( -- Interpret the privileges from the acl
						val, '=(.*)/'
					)
				) AS val
			FROM seq_cte1
		),

		seq_cte3 AS(
			SELECT DISTINCT ON(a._schema, a._sequence, b.sql) -- Unique list based on the schema and the SQL used to GRANT/REVOKE privilege
				a._schema,
				a._sequence,
				a.rolname,
				CASE
					WHEN a.rolname = $1
					THEN b.description
					ELSE b.description || '*' -- Asterisk indicates the privilege is granted indirectly by being a member of a group with that privilege
				END AS val,
				b._order
			FROM seq_cte2 a
			LEFT JOIN roles.privilege b ON a.val = b.privilege -- Join the actual privileges based on the acl-codes
			WHERE a.rolname IN(SELECT roles.usr_relation($1)) -- Find only for roles that the specified role is related to
			ORDER BY a._schema,
					a._sequence,
					b.sql,
					CASE -- GRANT OPTION over no GRANT OPTION
						WHEN b.description ILIKE '%with grant option'
						THEN 1
						ELSE 2
					END,
					CASE -- Privileges given to the specified role over group privileges
						WHEN a.rolname = $1
						THEN 1
						ELSE 2
					END
		),

		seq_cte4 AS( -- String aggregate tables based on schemas
			SELECT
				a._schema,
				string_agg(a._sequence, ',' ORDER BY a._sequence) AS val
			FROM roles.o_sequences a
			WHERE a._sequence != 'ALL'
			GROUP BY a._schema
		),

		seq_cte5 AS(
			SELECT
				a._schema,
				string_agg(a._sequence, ',' ORDER BY a._sequence) AS val,
				a.val AS privilege
			FROM seq_cte3 a
			GROUP BY a._schema, a.val
		),

		seq_cte6 AS(
			SELECT DISTINCT
				a._schema,
				CASE
					WHEN b.val = c.val
					THEN 'ALL'
					ELSE a._sequence
				END AS _sequence,
				a.val,
				a._order
			FROM seq_cte3 a
			LEFT JOIN seq_cte4 b ON a._schema = b._schema
			LEFT JOIN seq_cte5 c ON (a._schema, a.val) = (c._schema, c.privilege)
		),

		seq_cte AS(
			SELECT
				a._schema,
				a._sequence,
				string_agg(a.val, E'\n' ORDER BY _order) AS privilege
			FROM seq_cte6 a
			GROUP BY a._schema, a._sequence
			ORDER BY a._schema,
					CASE
						WHEN a._sequence = 'ALL'
						THEN 1
						ELSE 2
					END,
					a._sequence
		),

		-------------------- SEQUENCE HTML --------------------

		pre_seq_1 AS(
			SELECT
				_schema,
				COUNT(*) AS count_schema
			FROM seq_cte
			GROUP BY _schema
		),

		pre_seq_2 AS(
			SELECT
				_schema,
				FORMAT(
					'<td colspan="2">%s</td>',
					string_agg(_sequence || '</td><td>' || regexp_replace(privilege, E'\n', '<br>', 'g'), '</td></tr><tr><td colspan="2">')
				) AS _body
			FROM seq_cte
			GROUP BY _schema
			ORDER BY _schema
		),

		pre_seq AS(
			SELECT
				FORMAT(
					'%s',
					'<tr><th colspan="4"><br>SEQUENCES<br><br></th></tr><tr><th rowspan="' || string_agg(b.count_schema || '">SEQUENCES<br>IN SCHEMA<br><br>' || a._schema || '</th>' || a._body,  '</tr><tr><th rowspan="') || '</th></tr>'
				)
			FROM pre_seq_2 a
			LEFT JOIN pre_seq_1 b ON a._schema = b._schema
		),

		-------------------- DOMAIN --------------------

		dom_cte1 AS(
			SELECT
				b.nspname::text AS _schema,
				a.typname AS _domain,
				unnest(typacl::text[])AS val -- UNNEST privileges for roles into individual rows
			FROM pg_catalog.pg_type a
			LEFT JOIN pg_catalog.pg_namespace b ON a.typnamespace = b.oid
			WHERE b.nspname NOT IN(SELECT schema FROM roles.filter_schema) AND a.typtype = 'd' -- Only schemas not present in the filter
		),

		dom_cte2 AS(
			SELECT
				_schema,
				_domain,
				CASE -- Extract rolename from acl
					WHEN regexp_replace(val, '=.*', '') = '' -- If extract is an empty string the role is PUBLIC
					THEN 'PUBLIC'
					ELSE regexp_replace(val, '=.*', '')
				END AS rolname,
				roles.privilege_interpret(
					SUBSTRING( -- Interpret the privileges from the acl
						val, '=(.*)/'
					)
				) AS val
			FROM dom_cte1
		),

		dom_cte3 AS(
			SELECT DISTINCT ON(a._schema, a._domain, b.sql) -- Unique list based on the schema and the SQL used to GRANT/REVOKE privilege
				a._schema,
				a._domain,
				a.rolname,
				CASE
					WHEN a.rolname = $1
					THEN b.description
					ELSE b.description || '*' -- Asterisk indicates the privilege is granted indirectly by being a member of a group with that privilege
				END AS val,
				b._order
			FROM dom_cte2 a
			LEFT JOIN roles.privilege b ON a.val = b.privilege -- Join the actual privileges based on the acl-codes
			WHERE a.rolname IN(SELECT roles.usr_relation($1)) -- Find only for roles that the specified role is related to
			ORDER BY a._schema,
					a._domain,
					b.sql,
					CASE -- GRANT OPTION over no GRANT OPTION
						WHEN b.description ILIKE '%with grant option'
						THEN 1
						ELSE 2
					END,
					CASE -- Privileges given to the specified role over group privileges
						WHEN a.rolname = $1
						THEN 1
						ELSE 2
					END
		),

		dom_cte4 AS( -- String aggregate tables based on schemas
			SELECT
				a._schema,
				string_agg(a._domain, ',' ORDER BY a._domain) AS val
			FROM roles.o_domains a
			WHERE a._domain != 'ALL'
			GROUP BY a._schema
		),

		dom_cte5 AS(
			SELECT
				a._schema,
				string_agg(a._domain, ',' ORDER BY a._domain) AS val,
				a.val AS privilege
			FROM dom_cte3 a
			GROUP BY a._schema, a.val
		),

		dom_cte6 AS(
			SELECT DISTINCT
				a._schema,
				CASE
					WHEN b.val = c.val
					THEN 'ALL'
					ELSE a._domain
				END AS _domain,
				a.val,
				a._order
			FROM dom_cte3 a
			LEFT JOIN dom_cte4 b ON a._schema = b._schema
			LEFT JOIN dom_cte5 c ON (a._schema, a.val) = (c._schema, c.privilege)
		),

		dom_cte AS(
			SELECT
				a._schema,
				a._domain,
				string_agg(a.val, E'\n' ORDER BY _order) AS privilege
			FROM dom_cte6 a
			GROUP BY a._schema, a._domain
			ORDER BY a._schema,
					CASE
						WHEN a._domain = 'ALL'
						THEN 1
						ELSE 2
					END,
					a._domain
		),

		-------------------- DOMAIN HTML --------------------

		pre_dom_1 AS(
			SELECT
				_schema,
				COUNT(*) AS count_schema
			FROM dom_cte
			GROUP BY _schema
		),

		pre_dom_2 AS(
			SELECT
				_schema,
				FORMAT(
					'<td colspan="2">%s</td>',
					string_agg(_domain || '</td><td>' || regexp_replace(privilege, E'\n', '<br>', 'g'), '</td></tr><tr><td colspan="2">')
				) AS _body
			FROM dom_cte
			GROUP BY _schema
			ORDER BY _schema
		),

		pre_dom AS(
			SELECT
				FORMAT(
					'%s',
					'<tr><th colspan="4"><br>DOMAINS<br><br></th></tr><tr><th rowspan="' || string_agg(b.count_schema || '">DOMAINS<br>IN SCHEMA<br><br>' || a._schema || '</th>' || a._body,  '</tr><tr><th rowspan="') || '</th></tr>'
				)
			FROM pre_dom_2 a
			LEFT JOIN pre_dom_1 b ON a._schema = b._schema
		),

		-------------------- FOREIGN DATA WRAPPER --------------------
		
		fdw_cte1 AS(
			SELECT
				fdwname::text AS _fdw,
				unnest(fdwacl::text[]) AS val -- UNNEST privileges for roles into individual rows
			FROM pg_catalog.pg_foreign_data_wrapper
		),

		fdw_cte2 AS(
			SELECT
				_fdw,
				CASE -- Extract rolename from acl
					WHEN regexp_replace(val, '=.*', '') = '' -- If extract is an empty string the role is PUBLIC
					THEN 'PUBLIC'
					ELSE regexp_replace(val, '=.*', '')
				END AS rolname,
				roles.privilege_interpret(
					SUBSTRING( -- Interpret the privileges from the acl
						val, '=(.*)/'
					)
				) AS val
			FROM fdw_cte1
		),

		fdw_cte3 AS(
			SELECT DISTINCT ON(a._fdw, b.sql) -- Unique list based on the schema and the SQL used to GRANT/REVOKE privilege
				a._fdw,
				a.rolname,
				CASE
					WHEN a.rolname = $1
					THEN b.description
					ELSE b.description || '*' -- Asterisk indicates the privilege is granted indirectly by being a member of a group with that privilege
				END AS val,
				b._order
			FROM fdw_cte2 a
			LEFT JOIN roles.privilege b ON a.val = b.privilege -- Join the actual privileges based on the acl-codes
			WHERE a.rolname IN(SELECT roles.usr_relation($1)) -- Find only for roles that the specified role is related to
			ORDER BY a._fdw,
					b.sql,
					CASE -- GRANT OPTION over no GRANT OPTION
						WHEN b.description ILIKE '%with grant option'
						THEN 1
						ELSE 2
					END,
					CASE -- Privileges given to the specified role over group privileges
						WHEN a.rolname = $1
						THEN 1
						ELSE 2
					END
		),

		fdw_cte4 AS( -- String aggregate tables based on schemas
			SELECT
				string_agg(a.val, ',' ORDER BY a.val) AS val
			FROM roles.o_fdws a
			WHERE a.val != 'ALL'
		),

		fdw_cte5 AS(
			SELECT
				string_agg(a._fdw, ',' ORDER BY a._fdw) AS val,
				a.val AS privilege
			FROM fdw_cte3 a
			GROUP BY a.val
		),

		fdw_cte6 AS(
			SELECT DISTINCT
				CASE
					WHEN b.val = c.val
					THEN 'ALL'
					ELSE a._fdw
				END AS _fdw,
				a.val,
				a._order
			FROM fdw_cte3 a
			LEFT JOIN fdw_cte4 b ON TRUE
			LEFT JOIN fdw_cte5 c ON (a.val) = (c.privilege)
		),

		fdw_cte AS(
			SELECT
				a._fdw,
				string_agg(a.val, E'\n' ORDER BY _order) AS privilege
			FROM fdw_cte6 a
			GROUP BY a._fdw
			ORDER BY CASE
						WHEN a._fdw = 'ALL'
						THEN 1
						ELSE 2
					END,
					a._fdw
		),

		-------------------- FOREIGN DATA WRAPPER HTML --------------------

		pre_fdw AS(
			SELECT
				FORMAT(
					'%s',
					'<tr><th colspan="4"><br>FOREIGN DATA WRAPPERS<br><br></th></tr><tr><td colspan="3">' || string_agg(_fdw || '</td><td>' || regexp_replace(privilege, E'\n', '<br>', 'g'), '</td></tr><tr><td colspan="3">') || '</td></tr>'
				)
			FROM fdw_cte
		),

		-------------------- FOREIGN SERVER --------------------

		f_server_cte1 AS(
			SELECT
				srvname::text AS _foreign_server,
				unnest(srvacl::text[]) AS val -- UNNEST privileges for roles into individual rows
			FROM pg_catalog.pg_foreign_server
		),

		f_server_cte2 AS(
			SELECT
				_foreign_server,
				CASE -- Extract rolename from acl
					WHEN regexp_replace(val, '=.*', '') = '' -- If extract is an empty string the role is PUBLIC
					THEN 'PUBLIC'
					ELSE regexp_replace(val, '=.*', '')
				END AS rolname,
				roles.privilege_interpret(
					SUBSTRING( -- Interpret the privileges from the acl
						val, '=(.*)/'
					)
				) AS val
			FROM f_server_cte1
		),

		f_server_cte3 AS(
			SELECT DISTINCT ON(a._foreign_server, b.sql) -- Unique list based on the schema and the SQL used to GRANT/REVOKE privilege
				a._foreign_server,
				a.rolname,
				CASE
					WHEN a.rolname = $1
					THEN b.description
					ELSE b.description || '*' -- Asterisk indicates the privilege is granted indirectly by being a member of a group with that privilege
				END AS val,
				b._order
			FROM f_server_cte2 a
			LEFT JOIN roles.privilege b ON a.val = b.privilege -- Join the actual privileges based on the acl-codes
			WHERE a.rolname IN(SELECT roles.usr_relation($1)) -- Find only for roles that the specified role is related to
			ORDER BY a._foreign_server,
					b.sql,
					CASE -- GRANT OPTION over no GRANT OPTION
						WHEN b.description ILIKE '%with grant option'
						THEN 1
						ELSE 2
					END,
					CASE -- Privileges given to the specified role over group privileges
						WHEN a.rolname = $1
						THEN 1
						ELSE 2
					END
		),

		f_server_cte4 AS( -- String aggregate tables based on schemas
			SELECT
				string_agg(a.val, ',' ORDER BY a.val) AS val
			FROM roles.o_foreign_servers a
			WHERE a.val != 'ALL'
		),

		f_server_cte5 AS(
			SELECT
				string_agg(a._foreign_server, ',' ORDER BY a._foreign_server) AS val,
				a.val AS privilege
			FROM f_server_cte3 a
			GROUP BY a.val
		),

		f_server_cte6 AS(
			SELECT DISTINCT
				CASE
					WHEN b.val = c.val
					THEN 'ALL'
					ELSE a._foreign_server
				END AS _foreign_server,
				a.val,
				a._order
			FROM f_server_cte3 a
			LEFT JOIN f_server_cte4 b ON TRUE
			LEFT JOIN f_server_cte5 c ON (a.val) = (c.privilege)
		),

		f_server_cte AS(
			SELECT
				a._foreign_server,
				string_agg(a.val, E'\n' ORDER BY _order) AS privilege
			FROM f_server_cte6 a
			GROUP BY a._foreign_server
			ORDER BY CASE
						WHEN a._foreign_server = 'ALL'
						THEN 1
						ELSE 2
					END,
					a._foreign_server
		),

		-------------------- FOREIGN SERVER HTML --------------------

		pre_f_server AS(
			SELECT
				FORMAT(
					'%s',
					'<tr><th colspan="4"><br>FOREIGN SERVERS<br><br></th></tr><tr><td colspan="3">' || string_agg(_foreign_server || '</td><td>' || regexp_replace(privilege, E'\n', '<br>', 'g'), '</td></tr><tr><td colspan="3">') || '</td></tr>'
				)
			FROM f_server_cte
		),

		-------------------- LANGUAGE --------------------

		lan_cte1 AS(
			SELECT
				lanname::text AS _language,
				unnest(lanacl::text[]) AS val -- UNNEST privileges for roles into individual rows
			FROM pg_catalog.pg_language
			WHERE lanispl IS TRUE
		),

		lan_cte2 AS(
			SELECT
				_language,
				CASE -- Extract rolename from acl
					WHEN regexp_replace(val, '=.*', '') = '' -- If extract is an empty string the role is PUBLIC
					THEN 'PUBLIC'
					ELSE regexp_replace(val, '=.*', '')
				END AS rolname,
				roles.privilege_interpret(
					SUBSTRING( -- Interpret the privileges from the acl
						val, '=(.*)/'
					)
				) AS val
			FROM lan_cte1
		),

		lan_cte3 AS(
			SELECT DISTINCT ON(a._language, b.sql) -- Unique list based on the schema and the SQL used to GRANT/REVOKE privilege
				a._language,
				a.rolname,
				CASE
					WHEN a.rolname = $1
					THEN b.description
					ELSE b.description || '*' -- Asterisk indicates the privilege is granted indirectly by being a member of a group with that privilege
				END AS val,
				b._order
			FROM lan_cte2 a
			LEFT JOIN roles.privilege b ON a.val = b.privilege -- Join the actual privileges based on the acl-codes
			WHERE a.rolname IN(SELECT roles.usr_relation($1)) -- Find only for roles that the specified role is related to
			ORDER BY a._language,
					b.sql,
					CASE -- GRANT OPTION over no GRANT OPTION
						WHEN b.description ILIKE '%with grant option'
						THEN 1
						ELSE 2
					END,
					CASE -- Privileges given to the specified role over group privileges
						WHEN a.rolname = $1
						THEN 1
						ELSE 2
					END
		),

		lan_cte4 AS( -- String aggregate tables based on schemas
			SELECT
				string_agg(a.val, ',' ORDER BY a.val) AS val
			FROM roles.o_languages a
			WHERE a.val != 'ALL'
		),

		lan_cte5 AS(
			SELECT
				string_agg(a._language, ',' ORDER BY a._language) AS val,
				a.val AS privilege
			FROM lan_cte3 a
			GROUP BY a.val
		),

		lan_cte6 AS(
			SELECT DISTINCT
				CASE
					WHEN b.val = c.val
					THEN 'ALL'
					ELSE a._language
				END AS _language,
				a.val,
				a._order
			FROM lan_cte3 a
			LEFT JOIN lan_cte4 b ON TRUE
			LEFT JOIN lan_cte5 c ON (a.val) = (c.privilege)
		),

		lan_cte AS(
			SELECT
				a._language,
				string_agg(a.val, E'\n' ORDER BY _order) AS privilege
			FROM lan_cte6 a
			GROUP BY a._language
			ORDER BY CASE
						WHEN a._language = 'ALL'
						THEN 1
						ELSE 2
					END,
					a._language
		),

		-------------------- LANGUAGE HTML --------------------

		pre_lan AS(
			SELECT
				FORMAT(
					'%s',
					'<tr><th colspan="4"><br>LANGUAGES<br><br></th></tr><tr><td colspan="3">' || string_agg(_language || '</td><td>' || regexp_replace(privilege, E'\n', '<br>', 'g'), '</td></tr><tr><td colspan="3">') || '</td></tr>'
				)
			FROM lan_cte
		),

		-------------------- LARGE OBJECT --------------------

		loid_cte1 AS(
			SELECT
				oid::text AS _loid,
				unnest(lomacl::text[]) AS val -- UNNEST privileges for roles into individual rows
			FROM pg_catalog.pg_largeobject_metadata
		),

		loid_cte2 AS(
			SELECT
				_loid,
				CASE -- Extract rolename from acl
					WHEN regexp_replace(val, '=.*', '') = '' -- If extract is an empty string the role is PUBLIC
					THEN 'PUBLIC'
					ELSE regexp_replace(val, '=.*', '')
				END AS rolname,
				roles.privilege_interpret(
					SUBSTRING( -- Interpret the privileges from the acl
						val, '=(.*)/'
					)
				) AS val
			FROM loid_cte1
		),

		loid_cte3 AS(
			SELECT DISTINCT ON(a._loid, b.sql) -- Unique list based on the schema and the SQL used to GRANT/REVOKE privilege
				a._loid,
				a.rolname,
				CASE
					WHEN a.rolname = $1
					THEN b.description
					ELSE b.description || '*' -- Asterisk indicates the privilege is granted indirectly by being a member of a group with that privilege
				END AS val,
				b._order
			FROM loid_cte2 a
			LEFT JOIN roles.privilege b ON a.val = b.privilege -- Join the actual privileges based on the acl-codes
			WHERE a.rolname IN(SELECT roles.usr_relation($1)) -- Find only for roles that the specified role is related to
			ORDER BY a._loid,
					b.sql,
					CASE -- GRANT OPTION over no GRANT OPTION
						WHEN b.description ILIKE '%with grant option'
						THEN 1
						ELSE 2
					END,
					CASE -- Privileges given to the specified role over group privileges
						WHEN a.rolname = $1
						THEN 1
						ELSE 2
					END
		),

		loid_cte4 AS( -- String aggregate tables based on schemas
			SELECT
				string_agg(a.val, ',' ORDER BY a.val) AS val
			FROM roles.o_loids a
			WHERE a.val != 'ALL'
		),

		loid_cte5 AS(
			SELECT
				string_agg(a._loid, ',' ORDER BY a._loid) AS val,
				a.val AS privilege
			FROM loid_cte3 a
			GROUP BY a.val
		),

		loid_cte6 AS(
			SELECT DISTINCT
				CASE
					WHEN b.val = c.val
					THEN 'ALL'
					ELSE a._loid
				END AS _loid,
				a.val,
				a._order
			FROM loid_cte3 a
			LEFT JOIN loid_cte4 b ON TRUE
			LEFT JOIN loid_cte5 c ON (a.val) = (c.privilege)
		),

		loid_cte AS(
			SELECT
				a._loid,
				string_agg(a.val, E'\n' ORDER BY _order) AS privilege
			FROM loid_cte6 a
			GROUP BY a._loid
			ORDER BY CASE
						WHEN a._loid = 'ALL'
						THEN 1
						ELSE 2
					END,
					a._loid
		),

		-------------------- LARGE OBJECT HTML --------------------

		pre_loid AS(
			SELECT
				FORMAT(
					'%s',
					'<tr><th colspan="4"><br>LARGE OBJECTS<br><br></th></tr><tr><td colspan="3">' || string_agg(_loid || '</td><td>' || regexp_replace(privilege, E'\n', '<br>', 'g'), '</td></tr><tr><td colspan="3">') || '</td></tr>'
				)
			FROM loid_cte
		),

		-------------------- TABLESPACE --------------------

		tbs_cte1 AS(
			SELECT
				spcname::text AS _tablespace,
				unnest(spcacl::text[]) AS val -- UNNEST privileges for roles into individual rows
			FROM pg_catalog.pg_tablespace
		),

		tbs_cte2 AS(
			SELECT
				_tablespace,
				CASE -- Extract rolename from acl
					WHEN regexp_replace(val, '=.*', '') = '' -- If extract is an empty string the role is PUBLIC
					THEN 'PUBLIC'
					ELSE regexp_replace(val, '=.*', '')
				END AS rolname,
				roles.privilege_interpret(
					SUBSTRING( -- Interpret the privileges from the acl
						val, '=(.*)/'
					)
				) AS val
			FROM tbs_cte1
		),

		tbs_cte3 AS(
			SELECT DISTINCT ON(a._tablespace, b.sql) -- Unique list based on the schema and the SQL used to GRANT/REVOKE privilege
				a._tablespace,
				a.rolname,
				CASE
					WHEN a.rolname = $1
					THEN b.description
					ELSE b.description || '*' -- Asterisk indicates the privilege is granted indirectly by being a member of a group with that privilege
				END AS val,
				b._order
			FROM tbs_cte2 a
			LEFT JOIN roles.privilege b ON a.val = b.privilege -- Join the actual privileges based on the acl-codes
			WHERE a.rolname IN(SELECT roles.usr_relation($1)) -- Find only for roles that the specified role is related to
			ORDER BY a._tablespace,
					b.sql,
					CASE -- GRANT OPTION over no GRANT OPTION
						WHEN b.description ILIKE '%with grant option'
						THEN 1
						ELSE 2
					END,
					CASE -- Privileges given to the specified role over group privileges
						WHEN a.rolname = $1
						THEN 1
						ELSE 2
					END
		),

		tbs_cte4 AS( -- String aggregate tables based on schemas
			SELECT
				string_agg(a.val, ',' ORDER BY a.val) AS val
			FROM roles.o_tablespaces a
			WHERE a.val != 'ALL'
		),

		tbs_cte5 AS(
			SELECT
				string_agg(a._tablespace, ',' ORDER BY a._tablespace) AS val,
				a.val AS privilege
			FROM tbs_cte3 a
			GROUP BY a.val
		),

		tbs_cte6 AS(
			SELECT DISTINCT
				CASE
					WHEN b.val = c.val
					THEN 'ALL'
					ELSE a._tablespace
				END AS _tablespace,
				a.val,
				a._order
			FROM tbs_cte3 a
			LEFT JOIN tbs_cte4 b ON TRUE
			LEFT JOIN tbs_cte5 c ON (a.val) = (c.privilege)
		),

		tbs_cte AS(
			SELECT
				a._tablespace,
				string_agg(a.val, E'\n' ORDER BY _order) AS privilege
			FROM tbs_cte6 a
			GROUP BY a._tablespace
			ORDER BY CASE
						WHEN a._tablespace = 'ALL'
						THEN 1
						ELSE 2
					END,
					a._tablespace
		),

		-------------------- TABLESPACE HTML --------------------

		pre_tbs AS(
			SELECT
				FORMAT(
					'%s',
					'<tr><th colspan="4"><br>TABLESPACES<br><br></th></tr><tr><td colspan="3">' || string_agg(_tablespace || '</td><td>' || regexp_replace(privilege, E'\n', '<br>', 'g'), '</td></tr><tr><td colspan="3">') || '</td></tr>'
				)
			FROM tbs_cte
		),

		-------------------- DEFAULT PRIVILEGES --------------------

		dfp_conversion(code, val, _order) AS(
			VALUES ('r', 'TABLE', 2), ('S', 'SEQUENCE', 4), ('f', 'FUNCTION', 3), ('T', 'TYPE', 5), ('n', 'SCHEMA', 1)
		),

		dfp_cte1 AS(
			SELECT
				COALESCE(b.nspname::text, 'GLOBAL') AS _schema,
				c.val AS _type,
				(CASE
					WHEN b.nspname IS NULL
					THEN 1::text
					ELSE 2::text
				END ||c._order::text)::integer AS _order,
				unnest(a.defaclacl::text[]) AS val-- UNNEST privileges for roles into individual rows
			FROM pg_catalog.pg_default_acl a
			LEFT JOIN pg_catalog.pg_namespace b ON a.defaclnamespace = b.oid
			LEFT JOIN dfp_conversion c ON a.defaclobjtype = code
			WHERE b.nspname NOT IN(SELECT schema FROM roles.filter_schema) OR b.nspname IS NULL -- Only schemas not present in the filter
		),

		dfp_cte2 AS(
			SELECT
				_schema,
				_type,
				_order,
				CASE -- Extract rolename from acl
					WHEN regexp_replace(val, '=.*', '') = '' -- If extract is an empty string the role is PUBLIC
					THEN 'PUBLIC'
					ELSE regexp_replace(val, '=.*', '')
				END AS rolname,
				roles.privilege_interpret(
					SUBSTRING( -- Interpret the privileges from the acl
						val, '=(.*)/'
					)
				) AS val
			FROM dfp_cte1
		),

		dfp_cte3 AS(
			SELECT DISTINCT ON(a._schema, a._type, b.sql) -- Unique list based on the schema and the SQL used to GRANT/REVOKE privilege
				_schema,
				a._type,
				a.rolname,
				CASE
					WHEN a.rolname = $1
					THEN b.description
					ELSE b.description || '*' -- Asterisk indicates the privilege is granted indirectly by being a member of a group with that privilege
				END AS val,
				a._order,
				b._order AS _order_2
			FROM dfp_cte2 a
			LEFT JOIN roles.privilege b ON a.val = b.privilege -- Join the actual privileges based on the acl-codes
			WHERE a.rolname IN(SELECT roles.usr_relation($1)) -- Find only for roles that the specified role is related to
			ORDER BY a._schema,
					a._type,
					b.sql,
					CASE -- GRANT OPTION over no GRANT OPTION
						WHEN b.description ILIKE '%with grant option'
						THEN 1
						ELSE 2
					END,
					CASE -- Privileges given to the specified role over group privileges
						WHEN a.rolname = $1
						THEN 1
						ELSE 2
					END
		),

		dfp_cte AS(
			SELECT
				CASE
					WHEN a._schema = 'GLOBAL'
					THEN E'GLOBAL<br>DEFAULT PRIVILEGES'
					ELSE E'DEFAULT PRIVILEGES<br>IN SCHEMA<br><br>' || a._schema
				END AS _schema,
				a._type,
				string_agg(a.val, E'\n' ORDER BY _order_2) AS privilege
			FROM dfp_cte3 a
			WHERE CASE
					WHEN _schema = 'GLOBAL'
					THEN TRUE
					ELSE a.val NOT IN(SELECT val FROM dfp_cte3 WHERE _schema = 'GLOBAL' AND _type = a._type)
				END
			GROUP BY a._schema, a._order, a._type
			ORDER BY CASE
						WHEN _schema = 'GLOBAL'
						THEN 1
						ELSE 2
					END, a._schema, _order
		),

		-------------------- DEFAULT PRIVILEGES HTML --------------------

		pre_dfp_1 AS(
			SELECT
				_schema,
				COUNT(*) AS count_schema
			FROM dfp_cte
			GROUP BY _schema
		),

		pre_dfp_2 AS(
			SELECT
				_schema,
				FORMAT(
					'<td colspan="2">%s</td>',
					string_agg(_type || '</td><td>' || regexp_replace(privilege, E'\n', '<br>', 'g'), '</td></tr><tr><td colspan="2">')
				) AS _body
			FROM dfp_cte
			GROUP BY _schema
			ORDER BY CASE
						WHEN _schema ILIKE 'GLOBAL%'
						THEN 1
						ELSE 2
					END, _schema
		),

		pre_dfp AS(
			SELECT
				FORMAT(
					'%s',
					'<tr><th colspan="4"><br>DEFAULT PRIVILEGES<br><br></th></tr><tr><th rowspan="' || string_agg(b.count_schema || '">' || a._schema || '</th>' || a._body,  '</tr><tr><th rowspan="') || '</th></tr>'
				)
			FROM pre_dfp_2 a
			LEFT JOIN pre_dfp_1 b ON a._schema = b._schema
		)




	-------------------- HTML STATEMENT --------------------

	SELECT
		FORMAT(
			$qt$
			<!DOCTYPE html>
			<html>
			<head>
			<style>
			table, th, td {
				border: 3px solid black;
				border-collapse: collapse;
			}
			td {
				padding: 5px;
				text-align: left;
			}
			th {
				padding: 5px;
				text-align: center;
				vertical-align: top;
			}
			</style>
			</head>
			<body>
			<table style="width:100%%">
			  <tr><th colspan="4">PRIVILEGE OVERVIEW FOR ROLE: &nbsp;&nbsp;&nbsp;%s<br>IN DATABASE: &nbsp;&nbsp;&nbsp;%s</th></tr>
			  %s
			  %s
			  %s
			  %s
			  %s
			  %s
			  %s
			  %s
			  %s
			  %s
			  %s
			  %s
			  %s
			</table>
			</body>
			</html>
			 $qt$,
			$1, current_database(),
			(SELECT * FROM pre_db),
			(SELECT * FROM pre_sch),
			(SELECT * FROM pre_tbl),
			(SELECT * FROM pre_cl),
			(SELECT * FROM pre_func),
			(SELECT * FROM pre_seq),
			(SELECT * FROM pre_dfp),
			(SELECT * FROM pre_dom),
			(SELECT * FROM pre_fdw),
			(SELECT * FROM pre_f_server),
			(SELECT * FROM pre_lan),
			(SELECT * FROM pre_loid),
			(SELECT * FROM pre_tbs)
		);

END $$;

COMMENT ON FUNCTION roles.privilege_overview(rolname text) IS 'HTML overview of privileges for a given user. This includes privileges given through membership (marked with *).';


-- DROP FUNCTION IF EXISTS roles.usr_relation(rol text) CASCADE;

CREATE OR REPLACE FUNCTION roles.usr_relation(rol text)
	RETURNS TABLE (
		relations text
	)
	LANGUAGE plpgsql AS
$$

DECLARE

	i integer;
	_cte text;
	_union text;

BEGIN

	_cte := FORMAT( -- Select all the groups in which the specified role is a member
		$qt$

			WITH

				cte1 AS(
					SELECT
						(('{' || string_agg(b.rolname, ',') || '}')::text[]) AS grouprol
					FROM pg_catalog.pg_auth_members a
					LEFT JOIN pg_catalog.pg_authid b ON a.roleid = b.oid -- The group
					LEFT JOIN pg_catalog.pg_authid c ON a.member = c.oid -- The role that is the member of the group
					WHERE c.rolname NOT IN(SELECT role FROM roles.filter_role) AND c.rolinherit IS TRUE AND c.rolname = '%1$s'
				)

		$qt$, $1
	);

	_union := FORMAT( -- The last query where the result of all other CTE's is combined into a distinct list of relations
		$qt$

			SELECT 
				'%1$s' AS grouprol

			UNION

			SELECT
				'PUBLIC' AS grouprol

			UNION

			SELECT
				unnest(grouprol) AS grouprol
			FROM cte1

		$qt$, $1);

	FOR i IN 2..(SELECT COUNT(*) FROM pg_auth_members) LOOP

		_cte := _cte || FORMAT( -- Select all the groups in which the groups of the specified role is a member, and the groups of the groups.. and so on
			$qt$

				,cte%1$s AS(
					SELECT
						(('{' || string_agg(b.rolname, ',') || '}')::text[]) AS grouprol
					FROM pg_catalog.pg_auth_members a
					LEFT JOIN pg_catalog.pg_authid b ON a.roleid = b.oid -- The group
					LEFT JOIN pg_catalog.pg_authid c ON a.member = c.oid -- The role that is the member of the group
					WHERE c.rolname NOT IN(SELECT role FROM roles.filter_role) AND c.rolinherit IS TRUE AND c.rolname IN(SELECT unnest(grouprol) FROM cte%2$s) -- For all related groups found in the previous CTE
				)

			$qt$, i, i-1
		);

		_union := _union || FORMAT( -- Building the UNION
			$qt$

				UNION

				SELECT
					unnest(grouprol) AS grouprol
				FROM cte%1$s

			$qt$, i
		);

	END LOOP;

	RETURN QUERY
	EXECUTE FORMAT(
		$qt$

			%1$s -- The CTEs
			%2$s -- The UNIONs
			ORDER BY grouprol

		$qt$, _cte, _union
	);

END $$;

COMMENT ON FUNCTION roles.usr_relation(rol text) IS 'Finds all relations to other roles, where this role is a member, on the database server, no matter how indirect. It doesn''t include relationships without INHERIT.';


--
-- TRIGGER FUNCTIONS
--


-- DROP FUNCTION IF EXISTS roles.v_roles_privileges_trg();

CREATE OR REPLACE FUNCTION roles.v_roles_privileges_trg()
	RETURNS trigger
	LANGUAGE plpgsql AS
$$

DECLARE

	tofrom text;
	_grant text;
	_revoke text;
	_privileges text;
	_schema_string text;
	_table_string text;
	c_schema_string text;
	c_table_string text;

BEGIN

	IF TG_OP = 'DELETE' THEN

		IF OLD.rolname != 'PUBLIC' THEN -- Contain roles in quotes if it isn't PUBLIC

			OLD.rolname = '"' || OLD.rolname || '"';

		END IF;

		EXECUTE FORMAT(
			'REVOKE ALL ON DATABASE %2$s FROM %1$s;
			REVOKE ALL ON SCHEMA %3$s FROM %1$s;
			REVOKE ALL ON ALL TABLES IN SCHEMA %3$s FROM %1$s;
			REVOKE ALL ON ALL SEQUENCES IN SCHEMA %3$s FROM %1$s;
			REVOKE ALL ON ALL FUNCTIONS IN SCHEMA %3$s FROM %1$s;
			%4$s;
			%5$s;
			%6$s;
			%7$s;
			%8$s;
			%8$s;
			ALTER DEFAULT PRIVILEGES REVOKE ALL ON SCHEMAS FROM %1$s;
			ALTER DEFAULT PRIVILEGES REVOKE ALL ON TABLES FROM %1$s;
			ALTER DEFAULT PRIVILEGES REVOKE ALL ON SEQUENCES FROM %1$s;
			ALTER DEFAULT PRIVILEGES REVOKE ALL ON FUNCTIONS FROM %1$s;
			ALTER DEFAULT PRIVILEGES IN SCHEMA %3$s REVOKE ALL ON TABLES FROM %1$s;
			ALTER DEFAULT PRIVILEGES IN SCHEMA %3$s REVOKE ALL ON SEQUENCES FROM %1$s;
			ALTER DEFAULT PRIVILEGES IN SCHEMA %3$s REVOKE ALL ON FUNCTIONS FROM %1$s;',
			OLD.rolname,
			current_database(),
			(SELECT
				string_agg('"' || nspname::text || '"', ',')
			FROM pg_catalog.pg_namespace),
			(SELECT
				'REVOKE ALL ON DOMAIN ' || string_agg('"' || _schema || '"."' || _domain || '"', ',') || 'FROM ' || OLD.rolname
			FROM roles.o_domains
			WHERE val != 'ALL'),
			(SELECT
				'REVOKE ALL ON FOREIGN DATA WRAPPER ' || string_agg('"' || val || '"', ',') || 'FROM ' || OLD.rolname
			FROM roles.o_fdws
			WHERE val != 'ALL'),
			(SELECT
				'REVOKE ALL ON FOREIGN SERVER ' || string_agg('"' || val || '"', ',') || 'FROM ' || OLD.rolname
			FROM roles.o_foreign_servers
			WHERE val != 'ALL'),
			(SELECT
				'REVOKE ALL ON LANGUAGE ' || string_agg('"' || val || '"', ',') || 'FROM ' || OLD.rolname
			FROM roles.o_languages
			WHERE val != 'ALL'),
			(SELECT
				'REVOKE ALL ON LARGE OBJECT ' || string_agg('"' || val || '"', ',') || 'FROM ' || OLD.rolname
			FROM roles.o_loids
			WHERE val != 'ALL'),
			(SELECT
				'REVOKE ALL ON TABLESPACE ' || string_agg('"' || val || '"', ',') || 'FROM ' || OLD.rolname
			FROM roles.o_tablespaces
			WHERE val != 'ALL')
		);

		RETURN NULL;

	END IF;

	IF TG_OP IN('INSERT','UPDATE') THEN

		IF NEW._operation IS NULL OR NEW.rolname IS NULL THEN -- Missing values will result in RAISE

			RAISE EXCEPTION 'Missing Information';

		END IF;

		IF NEW.rolname != 'PUBLIC' THEN -- Contain roles in quotes if it isn't PUBLIC

			NEW.rolname := '"' || NEW.rolname || '"';

		END IF;

		IF NEW._operation = 'GRANT' THEN -- TO and FROM values for SQL

			tofrom := 'TO';
			_grant := 'WITH GRANT OPTION';
			_revoke := '';

		ELSIF NEW._operation = 'REVOKE' THEN

			tofrom := 'FROM';
			_grant := '';
			_revoke := 'GRANT OPTION FOR';

		END IF;

		IF cardinality(NEW.virtual_schema) = 0 OR EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.virtual_schema)) a WHERE unnest LIKE 'ALL%') THEN

			SELECT
				string_agg(_schema, ',')
			FROM roles.o_schemas
			WHERE _schema != 'ALL'
			INTO _schema_string;

			SELECT
				string_agg('"' || _schema || '"', ',')
			FROM roles.o_schemas
			WHERE _schema != 'ALL'
			INTO c_schema_string;

		ELSE

			SELECT
				roles.array_to_csv(NEW.virtual_schema)
			INTO _schema_string;

			SELECT
				roles.array_to_csv(NEW.virtual_schema, '"')
			INTO c_schema_string;

		END IF;

		IF cardinality(NEW.virtual_table) = 0 OR EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.virtual_table)) a WHERE unnest LIKE 'ALL%') THEN

			SELECT
				string_agg(_schema || '.' || _table, ',')
			FROM roles.o_tables
			WHERE _table != 'ALL' AND _schema IN(SELECT unnest(('{' || _schema_string || '}')::text[]))
			INTO _table_string;

			SELECT
				string_agg('"' || _schema || '"."' || _table || '"', ',')
			FROM roles.o_tables
			WHERE _table != 'ALL' AND _schema IN(SELECT unnest(('{' || _schema_string || '}')::text[]))
			INTO c_table_string;

		ELSE

			SELECT
				roles.array_to_csv(NEW.virtual_table)
			INTO _table_string;

			SELECT
				roles.array_to_csv(regexp_replace(NEW.virtual_table::text, '\.', '\".\"')::text[], '"')
			INTO c_table_string;

		END IF;

		-- DATABASE --

		IF cardinality(NEW.db_privilege) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.db_privilege)) a WHERE unnest LIKE 'ALL%') THEN -- ALL-option selected

				_privileges := 'ALL';

			ELSE

				SELECT
					roles.array_to_csv(NEW.db_privilege)
				INTO _privileges;

			END IF;

			EXECUTE FORMAT(
				'%s %s ON DATABASE %s %s %s;',
				NEW._operation, _privileges, current_database(), tofrom, NEW.rolname
			);

		END IF;

		-- DATABASE GRANT --

		IF cardinality(NEW.db_privilege_grant) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.db_privilege_grant)) a WHERE unnest LIKE 'ALL%') THEN -- ALL-option selected

				_privileges := 'ALL';

			ELSE

				SELECT
					roles.array_to_csv(NEW.db_privilege_grant)
				INTO _privileges;

			END IF;

			EXECUTE FORMAT( -- REVOKE GRANT OPTION FOR or GRANT WITH GRANT OPTION
				'%s %s %s ON DATABASE %s %s %s %s;',
				NEW._operation, _revoke, _privileges, current_database(), tofrom, NEW.rolname, _grant
			);

		END IF;

		-- SCHEMA, ALL --

		IF cardinality(NEW.schema_all) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.schema_all)) a WHERE unnest LIKE 'ALL%') THEN -- If all schemas has been chosen for ALL privileges

				SELECT
					string_agg('"' || _schema || '"', ',')
				FROM roles.o_schemas
				WHERE _schema != 'ALL'
				INTO _privileges;

			ELSE

				SELECT
					roles.array_to_csv(NEW.schema_all, '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT(
				'%s ALL ON SCHEMA %s %s %s;',
				NEW._operation, _privileges, tofrom, NEW.rolname
			);

		END IF;

		IF cardinality(NEW.schema_all_grant) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.schema_all_grant)) a WHERE unnest LIKE 'ALL%') THEN -- If all schemas has been chosen for ALL privileges

				SELECT
					string_agg('"' || _schema || '"', ',')
				FROM roles.o_schemas
				WHERE _schema != 'ALL'
				INTO _privileges;

			ELSE

				SELECT
					roles.array_to_csv(NEW.schema_all_grant, '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT( -- REVOKE GRANT OPTION FOR or GRANT WITH GRANT OPTION
				'%s %s ALL ON SCHEMA %s %s %s %s;',
				NEW._operation, _revoke, _privileges, tofrom, NEW.rolname, _grant
			);

		END IF;

		-- SCHEMA, USAGE --

		IF cardinality(NEW.schema_usage) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.schema_usage)) a WHERE unnest LIKE 'ALL%') THEN -- If all schemas has been chosen for USAGE privileges

				SELECT
					string_agg('"' || _schema || '"', ',')
				FROM roles.o_schemas
				WHERE _schema != 'ALL'
				INTO _privileges;

			ELSE

				SELECT
					roles.array_to_csv(NEW.schema_usage, '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT(
				'%s USAGE ON SCHEMA %s %s %s;',
				NEW._operation, _privileges, tofrom, NEW.rolname
			);

		END IF;

		IF cardinality(NEW.schema_usage_grant) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.schema_usage_grant)) a WHERE unnest LIKE 'ALL%') THEN -- If all schemas has been chosen for USAGE privileges

				SELECT
					string_agg('"' || _schema || '"', ',')
				FROM roles.o_schemas
				WHERE _schema != 'ALL'
				INTO _privileges;

			ELSE

				SELECT
					roles.array_to_csv(NEW.schema_usage_grant, '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT( -- REVOKE GRANT OPTION FOR or GRANT WITH GRANT OPTION
				'%s %s USAGE ON SCHEMA %s %s %s %s;',
				NEW._operation, _revoke, _privileges, tofrom, NEW.rolname, _grant
			);

		END IF;

		-- SCHEMA, CREATE --

		IF cardinality(NEW.schema_create) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.schema_create)) a WHERE unnest LIKE 'ALL%') THEN -- If all schemas has been chosen for CREATE privileges

				SELECT
					string_agg('"' || _schema || '"', ',')
				FROM roles.o_schemas
				WHERE _schema != 'ALL'
				INTO _privileges;

			ELSE

				SELECT
					roles.array_to_csv(NEW.schema_create, '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT(
				'%s CREATE ON SCHEMA %s %s %s;',
				NEW._operation, _privileges, tofrom, NEW.rolname
			);

		END IF;

		IF cardinality(NEW.schema_create_grant) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.schema_create_grant)) a WHERE unnest LIKE 'ALL%') THEN -- If all schemas has been chosen for CREATE privileges

				SELECT
					string_agg('"' || _schema || '"', ',')
				FROM roles.o_schemas
				WHERE _schema != 'ALL'
				INTO _privileges;

			ELSE

				SELECT
					roles.array_to_csv(NEW.schema_create_grant, '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT( -- REVOKE GRANT OPTION FOR or GRANT WITH GRANT OPTION
				'%s %s CREATE ON SCHEMA %s %s %s %s;',
				NEW._operation, _revoke, _privileges, tofrom, NEW.rolname, _grant
			);

		END IF;

		-- TABLE, ALL --

		IF cardinality(NEW.table_all) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.table_all)) a WHERE unnest LIKE 'ALL%') THEN -- If all tables has been chosen for ALL privileges

				SELECT
					string_agg('"' || _schema || '"."' || _table || '"',',')
				FROM roles.o_tables
				WHERE _schema IN(SELECT UNNEST(('{' || _schema_string || '}')::text[]))
				INTO _privileges;

			ELSE -- If ALL option hasn't been chosen, then list the selected tables


				SELECT
					roles.array_to_csv(regexp_replace(NEW.table_all::text, '\.', '\".\"', 'g')::text[], '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT(
				'%s ALL ON %s %s %s;',
				NEW._operation, _privileges, tofrom, NEW.rolname
			);

		END IF;

		IF cardinality(NEW.table_all_grant) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.table_all_grant)) a WHERE unnest LIKE 'ALL%') THEN -- If all tables has been chosen for ALL privileges

				SELECT
					string_agg('"' || _schema || '"."' || _table || '"',',')
				FROM roles.o_tables
				WHERE _schema IN(SELECT UNNEST(('{' || _schema_string || '}')::text[]))
				INTO _privileges;

			ELSE

				SELECT
					roles.array_to_csv(regexp_replace(NEW.table_all_grant::text, '\.', '\".\"', 'g')::text[], '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT( -- REVOKE GRANT OPTION FOR or GRANT WITH GRANT OPTION
				'%s %s ALL ON %s %s %s %s;',
				NEW._operation, _revoke, _privileges, tofrom, NEW.rolname, _grant
			);

		END IF;

		-- TABLE, SELECT --

		IF cardinality(NEW.table_select) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.table_select)) a WHERE unnest LIKE 'ALL%') THEN -- If all tables has been chosen for SELECT privileges

				SELECT
					string_agg('"' || _schema || '"."' || _table || '"',',')
				FROM roles.o_tables
				WHERE _schema IN(SELECT UNNEST(('{' || _schema_string || '}')::text[]))
				INTO _privileges;

			ELSE -- If ALL option hasn't been chosen, then list the selected tables

				SELECT
					roles.array_to_csv(regexp_replace(NEW.table_select::text, '\.', '\".\"', 'g')::text[], '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT(
				'%s SELECT ON %s %s %s;',
				NEW._operation, _privileges, tofrom, NEW.rolname
			);

		END IF;

		IF cardinality(NEW.table_select_grant) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.table_select_grant)) a WHERE unnest LIKE 'ALL%') THEN -- If all tables has been chosen for SELECT privileges

				SELECT
					string_agg('"' || _schema || '"."' || _table || '"',',')
				FROM roles.o_tables
				WHERE _schema IN(SELECT UNNEST(('{' || _schema_string || '}')::text[]))
				INTO _privileges;

			ELSE

				SELECT
					roles.array_to_csv(regexp_replace(NEW.table_select_grant::text, '\.', '\".\"', 'g')::text[], '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT( -- REVOKE GRANT OPTION FOR or GRANT WITH GRANT OPTION
				'%s %s SELECT ON %s %s %s %s;',
				NEW._operation, _revoke, _privileges, tofrom, NEW.rolname, _grant
			);

		END IF;

		-- TABLE, INSERT --

		IF cardinality(NEW.table_insert) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.table_insert)) a WHERE unnest LIKE 'ALL%') THEN -- If all tables has been chosen for INSERT privileges

				SELECT
					string_agg('"' || _schema || '"."' || _table || '"',',')
				FROM roles.o_tables
				WHERE _schema IN(SELECT UNNEST(('{' || _schema_string || '}')::text[]))
				INTO _privileges;

			ELSE -- If ALL option hasn't been chosen, then list the selected tables

				SELECT
					roles.array_to_csv(regexp_replace(NEW.table_insert::text, '\.', '\".\"', 'g')::text[], '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT(
				'%s INSERT ON %s %s %s;',
				NEW._operation, _privileges, tofrom, NEW.rolname
			);

		END IF;

		IF cardinality(NEW.table_insert_grant) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.table_insert_grant)) a WHERE unnest LIKE 'ALL%') THEN -- If all tables has been chosen for INSERT privileges

				SELECT
					string_agg('"' || _schema || '"."' || _table || '"',',')
				FROM roles.o_tables
				WHERE _schema IN(SELECT UNNEST(('{' || _schema_string || '}')::text[]))
				INTO _privileges;

			ELSE

				SELECT
					roles.array_to_csv(regexp_replace(NEW.table_insert_grant::text, '\.', '\".\"', 'g')::text[], '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT( -- REVOKE GRANT OPTION FOR or GRANT WITH GRANT OPTION
				'%s %s INSERT ON %s %s %s %s;',
				NEW._operation, _revoke, _privileges, tofrom, NEW.rolname, _grant
			);

		END IF;

		-- TABLE, UPDATE --

		IF cardinality(NEW.table_update) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.table_update)) a WHERE unnest LIKE 'ALL%') THEN -- If all tables has been chosen for UPDATE privileges

				SELECT
					string_agg('"' || _schema || '"."' || _table || '"',',')
				FROM roles.o_tables
				WHERE _schema IN(SELECT UNNEST(('{' || _schema_string || '}')::text[]))
				INTO _privileges;

			ELSE -- If ALL option hasn't been chosen, then list the selected tables

				SELECT
					roles.array_to_csv(regexp_replace(NEW.table_update::text, '\.', '\".\"', 'g')::text[], '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT(
				'%s UPDATE ON %s %s %s;',
				NEW._operation, _privileges, tofrom, NEW.rolname
			);

		END IF;

		IF cardinality(NEW.table_update_grant) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.table_update_grant)) a WHERE unnest LIKE 'ALL%') THEN -- If all tables has been chosen for UPDATE privileges

				SELECT
					string_agg('"' || _schema || '"."' || _table || '"',',')
				FROM roles.o_tables
				WHERE _schema IN(SELECT UNNEST(('{' || _schema_string || '}')::text[]))
				INTO _privileges;

			ELSE

				SELECT
					roles.array_to_csv(regexp_replace(NEW.table_update_grant::text, '\.', '\".\"', 'g')::text[], '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT( -- REVOKE GRANT OPTION FOR or GRANT WITH GRANT OPTION
				'%s %s UPDATE ON %s %s %s %s;',
				NEW._operation, _revoke, _privileges, tofrom, NEW.rolname, _grant
			);

		END IF;

		-- TABLE, DELETE --

		IF cardinality(NEW.table_delete) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.table_delete)) a WHERE unnest LIKE 'ALL%') THEN -- If all tables has been chosen for DELETE privileges

				SELECT
					string_agg('"' || _schema || '"."' || _table || '"',',')
				FROM roles.o_tables
				WHERE _schema IN(SELECT UNNEST(('{' || _schema_string || '}')::text[]))
				INTO _privileges;

			ELSE -- If ALL option hasn't been chosen, then list the selected tables

				SELECT
					roles.array_to_csv(regexp_replace(NEW.table_delete::text, '\.', '\".\"', 'g')::text[], '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT(
				'%s DELETE ON %s %s %s;',
				NEW._operation, _privileges, tofrom, NEW.rolname
			);

		END IF;

		IF cardinality(NEW.table_delete_grant) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.table_delete_grant)) a WHERE unnest LIKE 'ALL%') THEN -- If all tables has been chosen for DELETE privileges

				SELECT
					string_agg('"' || _schema || '"."' || _table || '"',',')
				FROM roles.o_tables
				WHERE _schema IN(SELECT UNNEST(('{' || _schema_string || '}')::text[]))
				INTO _privileges;

			ELSE

				SELECT
					roles.array_to_csv(regexp_replace(NEW.table_delete_grant::text, '\.', '\".\"', 'g')::text[], '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT( -- REVOKE GRANT OPTION FOR or GRANT WITH GRANT OPTION
				'%s %s DELETE ON %s %s %s %s;',
				NEW._operation, _revoke, _privileges, tofrom, NEW.rolname, _grant
			);

		END IF;

		-- TABLE, TRUNCATE --

		IF cardinality(NEW.table_truncate) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.table_truncate)) a WHERE unnest LIKE 'ALL%') THEN -- If all tables has been chosen for TRUNCATE privileges

				SELECT
					string_agg('"' || _schema || '"."' || _table || '"',',')
				FROM roles.o_tables
				WHERE _schema IN(SELECT UNNEST(('{' || _schema_string || '}')::text[]))
				INTO _privileges;

			ELSE -- If ALL option hasn't been chosen, then list the selected tables

				SELECT
					roles.array_to_csv(regexp_replace(NEW.table_truncate::text, '\.', '\".\"', 'g')::text[], '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT(
				'%s TRUNCATE ON %s %s %s;',
				NEW._operation, _privileges, tofrom, NEW.rolname
			);

		END IF;

		IF cardinality(NEW.table_truncate_grant) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.table_truncate_grant)) a WHERE unnest LIKE 'ALL%') THEN -- If all tables has been chosen for TRUNCATE privileges

				SELECT
					string_agg('"' || _schema || '"."' || _table || '"',',')
				FROM roles.o_tables
				WHERE _schema IN(SELECT UNNEST(('{' || _schema_string || '}')::text[]))
				INTO _privileges;

			ELSE

				SELECT
					roles.array_to_csv(regexp_replace(NEW.table_truncate_grant::text, '\.', '\".\"', 'g')::text[], '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT( -- REVOKE GRANT OPTION FOR or GRANT WITH GRANT OPTION
				'%s %s TRUNCATE ON %s %s %s %s;',
				NEW._operation, _revoke, _privileges, tofrom, NEW.rolname, _grant
			);

		END IF;

		-- TABLE, REFERENCES --

		IF cardinality(NEW.table_references) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.table_references)) a WHERE unnest LIKE 'ALL%') THEN -- If all tables has been chosen for REFERENCES privileges

				SELECT
					string_agg('"' || _schema || '"."' || _table || '"',',')
				FROM roles.o_tables
				WHERE _schema IN(SELECT UNNEST(('{' || _schema_string || '}')::text[]))
				INTO _privileges;

			ELSE -- If ALL option hasn't been chosen, then list the selected tables

				SELECT
					roles.array_to_csv(regexp_replace(NEW.table_references::text, '\.', '\".\"', 'g')::text[], '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT(
				'%s REFERENCES ON %s %s %s;',
				NEW._operation, _privileges, tofrom, NEW.rolname
			);

		END IF;

		IF cardinality(NEW.table_references_grant) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.table_references_grant)) a WHERE unnest LIKE 'ALL%') THEN -- If all tables has been chosen for REFERENCES privileges

				SELECT
					string_agg('"' || _schema || '"."' || _table || '"',',')
				FROM roles.o_tables
				WHERE _schema IN(SELECT UNNEST(('{' || _schema_string || '}')::text[]))
				INTO _privileges;

			ELSE

				SELECT
					roles.array_to_csv(regexp_replace(NEW.table_references_grant::text, '\.', '\".\"', 'g')::text[], '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT( -- REVOKE GRANT OPTION FOR or GRANT WITH GRANT OPTION
				'%s %s REFERENCES ON %s %s %s %s;',
				NEW._operation, _revoke, _privileges, tofrom, NEW.rolname, _grant
			);

		END IF;

		-- TABLE, TRIGGER --

		IF cardinality(NEW.table_trigger) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.table_trigger)) a WHERE unnest LIKE 'ALL%') THEN -- If all tables has been chosen for TRIGGER privileges

				SELECT
					string_agg('"' || _schema || '"."' || _table || '"',',')
				FROM roles.o_tables
				WHERE _schema IN(SELECT UNNEST(('{' || _schema_string || '}')::text[]))
				INTO _privileges;

			ELSE -- If ALL option hasn't been chosen, then list the selected tables

				SELECT
					roles.array_to_csv(regexp_replace(NEW.table_trigger::text, '\.', '\".\"', 'g')::text[], '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT(
				'%s TRIGGER ON %s %s %s;',
				NEW._operation, _privileges, tofrom, NEW.rolname
			);

		END IF;

		IF cardinality(NEW.table_trigger_grant) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.table_trigger_grant)) a WHERE unnest LIKE 'ALL%') THEN -- If all tables has been chosen for TRIGGER privileges

				SELECT
					string_agg('"' || _schema || '"."' || _table || '"',',')
				FROM roles.o_tables
				WHERE _schema IN(SELECT UNNEST(('{' || _schema_string || '}')::text[]))
				INTO _privileges;

			ELSE

				SELECT
					roles.array_to_csv(regexp_replace(NEW.table_trigger_grant::text, '\.', '\".\"', 'g')::text[], '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT( -- REVOKE GRANT OPTION FOR or GRANT WITH GRANT OPTION
				'%s %s TRIGGER ON %s %s %s %s;',
				NEW._operation, _revoke, _privileges, tofrom, NEW.rolname, _grant
			);

		END IF;

		-- FUNCTION, ALL --

		IF cardinality(NEW.function_all) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.function_all)) a WHERE unnest LIKE 'ALL%') THEN -- If all functions has been chosen for ALL privileges

				_privileges := 'ALL FUNCTIONS IN SCHEMA ' || c_schema_string;

			ELSE -- If ALL option hasn't been chosen, then list the selected functions

				WITH

					cte1 AS(
						SELECT
							regexp_split_to_table(regexp_replace(NEW.function_all::text, '[{|}|"]', '', 'g'), '\),') AS val
					)

				SELECT
					'FUNCTION ' || string_agg('"' || SUBSTRING(val, '(.*)\..*\(.*') || '"."' || SUBSTRING(val, '.*\.(.*)\(.*') || '"' || SUBSTRING(val, '.*\..*(\(.*)'), '),') FROM cte1
				INTO _privileges;

			END IF;

			EXECUTE FORMAT(
				'%s ALL ON %s %s %s;',
				NEW._operation, _privileges, tofrom, NEW.rolname
			);

		END IF;

		IF cardinality(NEW.function_all_grant) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.function_all_grant)) a WHERE unnest LIKE 'ALL%') THEN -- If all functions has been chosen for ALL privileges

				_privileges := 'ALL FUNCTIONS IN SCHEMA ' || c_schema_string;

			ELSE

				WITH

					cte1 AS(
						SELECT
							regexp_split_to_table(regexp_replace(NEW.function_all_grant::text, '[{|}|"]', '', 'g'), '\),') AS val
					)

				SELECT
					'FUNCTION ' || string_agg('"' || SUBSTRING(val, '(.*)\..*\(.*') || '"."' || SUBSTRING(val, '.*\.(.*)\(.*') || '"' || SUBSTRING(val, '.*\..*(\(.*)'), '),') FROM cte1
				INTO _privileges;

			END IF;

			EXECUTE FORMAT( -- REVOKE GRANT OPTION FOR or GRANT WITH GRANT OPTION
				'%s %s ALL ON %s %s %s %s;',
				NEW._operation, _revoke, _privileges, tofrom, NEW.rolname, _grant
			);

		END IF;

		-- FUNCTION, EXECUTE --

		IF cardinality(NEW.function_execute) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.function_execute)) a WHERE unnest LIKE 'ALL%') THEN -- If all functions has been chosen for EXECUTE privileges

				_privileges := 'ALL FUNCTIONS IN SCHEMA ' || c_schema_string;

			ELSE -- If ALL option hasn't been chosen, then list the selected functions

				WITH

					cte1 AS(
						SELECT
							regexp_split_to_table(regexp_replace(NEW.function_execute::text, '[{|}|"]', '', 'g'), '\),') AS val
					)

				SELECT
					'FUNCTION ' || string_agg('"' || SUBSTRING(val, '(.*)\..*\(.*') || '"."' || SUBSTRING(val, '.*\.(.*)\(.*') || '"' || SUBSTRING(val, '.*\..*(\(.*)'), '),') FROM cte1
				INTO _privileges;

			END IF;

			EXECUTE FORMAT(
				'%s EXECUTE ON %s %s %s;',
				NEW._operation, _privileges, tofrom, NEW.rolname
			);

		END IF;

		IF cardinality(NEW.function_execute_grant) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.function_execute_grant)) a WHERE unnest LIKE 'ALL%') THEN -- If all functions has been chosen for EXECUTE privileges

				_privileges := 'ALL FUNCTIONS IN SCHEMA ' || c_schema_string;

			ELSE

				WITH

					cte1 AS(
						SELECT
							regexp_split_to_table(regexp_replace(NEW.function_execute_grant::text, '[{|}|"]', '', 'g'), '\),') AS val
					)

				SELECT
					'FUNCTION ' || string_agg('"' || SUBSTRING(val, '(.*)\..*\(.*') || '"."' || SUBSTRING(val, '.*\.(.*)\(.*') || '"' || SUBSTRING(val, '.*\..*(\(.*)'), '),') FROM cte1
				INTO _privileges;

			END IF;

			EXECUTE FORMAT( -- REVOKE GRANT OPTION FOR or GRANT WITH GRANT OPTION
				'%s %s EXECUTE ON %s %s %s %s;',
				NEW._operation, _revoke, _privileges, tofrom, NEW.rolname, _grant
			);

		END IF;

		-- SEQUENCE, ALL --

		IF cardinality(NEW.sequence_all) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.sequence_all)) a WHERE unnest LIKE 'ALL%') THEN -- If all sequences has been chosen for ALL privileges

				_privileges := 'ALL SEQUENCES IN SCHEMA ' || c_schema_string;

			ELSE -- If ALL option hasn't been chosen, then list the selected sequences

				SELECT
					'SEQUENCE ' || roles.array_to_csv(regexp_replace(NEW.sequence_all::text, '\.', '\".\"', 'g')::text[], '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT(
				'%s ALL ON %s %s %s;',
				NEW._operation, _privileges, tofrom, NEW.rolname
			);

		END IF;

		IF cardinality(NEW.sequence_all_grant) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.sequence_all_grant)) a WHERE unnest LIKE 'ALL%') THEN -- If all sequences has been chosen for ALL privileges

				_privileges := 'ALL SEQUENCES IN SCHEMA ' || c_schema_string;

			ELSE

				SELECT
					'SEQUENCE ' || roles.array_to_csv(regexp_replace(NEW.sequence_all_grant::text, '\.', '\".\"', 'g')::text[], '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT( -- REVOKE GRANT OPTION FOR or GRANT WITH GRANT OPTION
				'%s %s ALL ON %s %s %s %s;',
				NEW._operation, _revoke, _privileges, tofrom, NEW.rolname, _grant
			);

		END IF;

		-- SEQUENCE, USAGE --

		IF cardinality(NEW.sequence_usage) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.sequence_usage)) a WHERE unnest LIKE 'ALL%') THEN -- If all sequences has been chosen for USAGE privileges

				_privileges := 'ALL SEQUENCES IN SCHEMA ' || c_schema_string;

			ELSE -- If ALL option hasn't been chosen, then list the selected sequences

				SELECT
					'SEQUENCE ' || roles.array_to_csv(regexp_replace(NEW.sequence_usage::text, '\.', '\".\"', 'g')::text[], '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT(
				'%s USAGE ON %s %s %s;',
				NEW._operation, _privileges, tofrom, NEW.rolname
			);

		END IF;

		IF cardinality(NEW.sequence_usage_grant) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.sequence_usage_grant)) a WHERE unnest LIKE 'ALL%') THEN -- If all sequences has been chosen for USAGE privileges

				_privileges := 'ALL SEQUENCES IN SCHEMA ' || c_schema_string;

			ELSE

				SELECT
					'SEQUENCE ' || roles.array_to_csv(regexp_replace(NEW.sequence_usage_grant::text, '\.', '\".\"', 'g')::text[], '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT( -- REVOKE GRANT OPTION FOR or GRANT WITH GRANT OPTION
				'%s %s USAGE ON %s %s %s %s;',
				NEW._operation, _revoke, _privileges, tofrom, NEW.rolname, _grant
			);

		END IF;

		-- SEQUENCE, SELECT --

		IF cardinality(NEW.sequence_select) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.sequence_select)) a WHERE unnest LIKE 'ALL%') THEN -- If all sequences has been chosen for SELECT privileges

				_privileges := 'ALL SEQUENCES IN SCHEMA ' || c_schema_string;

			ELSE -- If ALL option hasn't been chosen, then list the selected sequences

				SELECT
					'SEQUENCE ' || roles.array_to_csv(regexp_replace(NEW.sequence_select::text, '\.', '\".\"', 'g')::text[], '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT(
				'%s SELECT ON %s %s %s;',
				NEW._operation, _privileges, tofrom, NEW.rolname
			);

		END IF;

		IF cardinality(NEW.sequence_select_grant) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.sequence_select_grant)) a WHERE unnest LIKE 'ALL%') THEN -- If all sequences has been chosen for SELECT privileges

				_privileges := 'ALL SEQUENCES IN SCHEMA ' || c_schema_string;

			ELSE

				SELECT
					'SEQUENCE ' || roles.array_to_csv(regexp_replace(NEW.sequence_select_grant::text, '\.', '\".\"', 'g')::text[], '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT( -- REVOKE GRANT OPTION FOR or GRANT WITH GRANT OPTION
				'%s %s SELECT ON %s %s %s %s;',
				NEW._operation, _revoke, _privileges, tofrom, NEW.rolname, _grant
			);

		END IF;

		-- SEQUENCE, UPDATE --

		IF cardinality(NEW.sequence_update) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.sequence_update)) a WHERE unnest LIKE 'ALL%') THEN -- If all sequences has been chosen for UPDATE privileges

				_privileges := 'ALL SEQUENCES IN SCHEMA ' || c_schema_string;

			ELSE -- If ALL option hasn't been chosen, then list the selected sequences

				SELECT
					'SEQUENCE ' || roles.array_to_csv(regexp_replace(NEW.sequence_update::text, '\.', '\".\"', 'g')::text[], '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT(
				'%s UPDATE ON %s %s %s;',
				NEW._operation, _privileges, tofrom, NEW.rolname
			);

		END IF;

		IF cardinality(NEW.sequence_update_grant) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.sequence_update_grant)) a WHERE unnest LIKE 'ALL%') THEN -- If all sequences has been chosen for UPDATE privileges

				_privileges := 'ALL SEQUENCES IN SCHEMA ' || c_schema_string;

			ELSE

				SELECT
					'SEQUENCE ' || roles.array_to_csv(regexp_replace(NEW.sequence_update_grant::text, '\.', '\".\"', 'g')::text[], '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT( -- REVOKE GRANT OPTION FOR or GRANT WITH GRANT OPTION
				'%s %s UPDATE ON %s %s %s %s;',
				NEW._operation, _revoke, _privileges, tofrom, NEW.rolname, _grant
			);

		END IF;

		-- COLUMN, ALL --

		IF cardinality(NEW.column_all) > 0 THEN

			EXECUTE FORMAT(
				$qt$WITH

						cte1 AS(
							SELECT
								UNNEST('%s'::text[]) AS val
						),

						cte2 AS(
							SELECT
								SUBSTRING(val, '(.+)\..+\..+') AS c_schema,
								SUBSTRING(val, '.+\.(.+)\..+') AS c_table,
								SUBSTRING(val, '.+\..+\.(.+)') AS c_column
							FROM cte1
						),

						cte3 AS(
							SELECT
								'%s %s (' || string_agg('"' || c_column || '"', ',') || ') ON "' || c_schema || '"."' || c_table || '" %s %s;' AS val
							FROM cte2
							GROUP BY c_schema, c_table
						)

					SELECT
						string_agg(val, E'\n')
					FROM cte3$qt$,
					NEW.column_all, NEW._operation, 'ALL', tofrom, NEW.rolname
			) INTO _privileges;

			EXECUTE _privileges;

		END IF;

		IF cardinality(NEW.column_all_grant) > 0 THEN

			EXECUTE FORMAT(
				$qt$WITH

						cte1 AS(
							SELECT
								UNNEST('%s'::text[]) AS val
						),

						cte2 AS(
							SELECT
								SUBSTRING(val, '(.+)\..+\..+') AS c_schema,
								SUBSTRING(val, '.+\.(.+)\..+') AS c_table,
								SUBSTRING(val, '.+\..+\.(.+)') AS c_column
							FROM cte1
						),

						cte3 AS(
							SELECT
								'%s %s %s (' || string_agg('"' || c_column || '"', ',') || ') ON "' || c_schema || '"."' || c_table || '" %s %s %s;' AS val
							FROM cte2
							GROUP BY c_schema, c_table
						)

					SELECT
						string_agg(val, E'\n')
					FROM cte3$qt$,
					NEW.column_all_grant, NEW._operation, _revoke, 'ALL', tofrom, NEW.rolname, _grant
			) INTO _privileges;

			EXECUTE _privileges;

		END IF;

		-- COLUMN, SELECT --

		IF cardinality(NEW.column_select) > 0 THEN

			EXECUTE FORMAT(
				$qt$WITH

						cte1 AS(
							SELECT
								UNNEST('%s'::text[]) AS val
						),

						cte2 AS(
							SELECT
								SUBSTRING(val, '(.+)\..+\..+') AS c_schema,
								SUBSTRING(val, '.+\.(.+)\..+') AS c_table,
								SUBSTRING(val, '.+\..+\.(.+)') AS c_column
							FROM cte1
						),

						cte3 AS(
							SELECT
								'%s %s (' || string_agg('"' || c_column || '"', ',') || ') ON "' || c_schema || '"."' || c_table || '" %s %s;' AS val
							FROM cte2
							GROUP BY c_schema, c_table
						)

					SELECT
						string_agg(val, E'\n')
					FROM cte3$qt$,
					NEW.column_select, NEW._operation, 'SELECT', tofrom, NEW.rolname
			) INTO _privileges;

			EXECUTE _privileges;

		END IF;

		IF cardinality(NEW.column_select_grant) > 0 THEN

			EXECUTE FORMAT(
				$qt$WITH

						cte1 AS(
							SELECT
								UNNEST('%s'::text[]) AS val
						),

						cte2 AS(
							SELECT
								SUBSTRING(val, '(.+)\..+\..+') AS c_schema,
								SUBSTRING(val, '.+\.(.+)\..+') AS c_table,
								SUBSTRING(val, '.+\..+\.(.+)') AS c_column
							FROM cte1
						),

						cte3 AS(
							SELECT
								'%s %s %s (' || string_agg('"' || c_column || '"', ',') || ') ON "' || c_schema || '"."' || c_table || '" %s %s %s;' AS val
							FROM cte2
							GROUP BY c_schema, c_table
						)

					SELECT
						string_agg(val, E'\n')
					FROM cte3$qt$,
					NEW.column_select_grant, NEW._operation, _revoke, 'SELECT', tofrom, NEW.rolname, _grant
			) INTO _privileges;

			EXECUTE _privileges;

		END IF;

		-- COLUMN, INSERT --

		IF cardinality(NEW.column_insert) > 0 THEN

			EXECUTE FORMAT(
				$qt$WITH

						cte1 AS(
							SELECT
								UNNEST('%s'::text[]) AS val
						),

						cte2 AS(
							SELECT
								SUBSTRING(val, '(.+)\..+\..+') AS c_schema,
								SUBSTRING(val, '.+\.(.+)\..+') AS c_table,
								SUBSTRING(val, '.+\..+\.(.+)') AS c_column
							FROM cte1
						),

						cte3 AS(
							SELECT
								'%s %s (' || string_agg('"' || c_column || '"', ',') || ') ON "' || c_schema || '"."' || c_table || '" %s %s;' AS val
							FROM cte2
							GROUP BY c_schema, c_table
						)

					SELECT
						string_agg(val, E'\n')
					FROM cte3$qt$,
					NEW.column_insert, NEW._operation, 'INSERT', tofrom, NEW.rolname
			) INTO _privileges;

			EXECUTE _privileges;

		END IF;

		IF cardinality(NEW.column_insert_grant) > 0 THEN

			EXECUTE FORMAT(
				$qt$WITH

						cte1 AS(
							SELECT
								UNNEST('%s'::text[]) AS val
						),

						cte2 AS(
							SELECT
								SUBSTRING(val, '(.+)\..+\..+') AS c_schema,
								SUBSTRING(val, '.+\.(.+)\..+') AS c_table,
								SUBSTRING(val, '.+\..+\.(.+)') AS c_column
							FROM cte1
						),

						cte3 AS(
							SELECT
								'%s %s %s (' || string_agg('"' || c_column || '"', ',') || ') ON "' || c_schema || '"."' || c_table || '" %s %s %s;' AS val
							FROM cte2
							GROUP BY c_schema, c_table
						)

					SELECT
						string_agg(val, E'\n')
					FROM cte3$qt$,
					NEW.column_insert_grant, NEW._operation, _revoke, 'INSERT', tofrom, NEW.rolname, _grant
			) INTO _privileges;

			EXECUTE _privileges;

		END IF;

		-- COLUMN, UPDATE --

		IF cardinality(NEW.column_update) > 0 THEN

			EXECUTE FORMAT(
				$qt$WITH

						cte1 AS(
							SELECT
								UNNEST('%s'::text[]) AS val
						),

						cte2 AS(
							SELECT
								SUBSTRING(val, '(.+)\..+\..+') AS c_schema,
								SUBSTRING(val, '.+\.(.+)\..+') AS c_table,
								SUBSTRING(val, '.+\..+\.(.+)') AS c_column
							FROM cte1
						),

						cte3 AS(
							SELECT
								'%s %s (' || string_agg('"' || c_column || '"', ',') || ') ON "' || c_schema || '"."' || c_table || '" %s %s;' AS val
							FROM cte2
							GROUP BY c_schema, c_table
						)

					SELECT
						string_agg(val, E'\n')
					FROM cte3$qt$,
					NEW.column_update, NEW._operation, 'UPDATE', tofrom, NEW.rolname
			) INTO _privileges;

			EXECUTE _privileges;

		END IF;

		IF cardinality(NEW.column_update_grant) > 0 THEN

			EXECUTE FORMAT(
				$qt$WITH

						cte1 AS(
							SELECT
								UNNEST('%s'::text[]) AS val
						),

						cte2 AS(
							SELECT
								SUBSTRING(val, '(.+)\..+\..+') AS c_schema,
								SUBSTRING(val, '.+\.(.+)\..+') AS c_table,
								SUBSTRING(val, '.+\..+\.(.+)') AS c_column
							FROM cte1
						),

						cte3 AS(
							SELECT
								'%s %s %s (' || string_agg('"' || c_column || '"', ',') || ') ON "' || c_schema || '"."' || c_table || '" %s %s %s;' AS val
							FROM cte2
							GROUP BY c_schema, c_table
						)

					SELECT
						string_agg(val, E'\n')
					FROM cte3$qt$,
					NEW.column_update_grant, NEW._operation, _revoke, 'UPDATE', tofrom, NEW.rolname, _grant
			) INTO _privileges;

			EXECUTE _privileges;

		END IF;

		-- COLUMN, REFERENCES --

		IF cardinality(NEW.column_references) > 0 THEN

			EXECUTE FORMAT(
				$qt$WITH

						cte1 AS(
							SELECT
								UNNEST('%s'::text[]) AS val
						),

						cte2 AS(
							SELECT
								SUBSTRING(val, '(.+)\..+\..+') AS c_schema,
								SUBSTRING(val, '.+\.(.+)\..+') AS c_table,
								SUBSTRING(val, '.+\..+\.(.+)') AS c_column
							FROM cte1
						),

						cte3 AS(
							SELECT
								'%s %s (' || string_agg('"' || c_column || '"', ',') || ') ON "' || c_schema || '"."' || c_table || '" %s %s;' AS val
							FROM cte2
							GROUP BY c_schema, c_table
						)

					SELECT
						string_agg(val, E'\n')
					FROM cte3$qt$,
					NEW.column_references, NEW._operation, 'REFERENCES', tofrom, NEW.rolname
			) INTO _privileges;

			EXECUTE _privileges;

		END IF;

		IF cardinality(NEW.column_references_grant) > 0 THEN

			EXECUTE FORMAT(
				$qt$WITH

						cte1 AS(
							SELECT
								UNNEST('%s'::text[]) AS val
						),

						cte2 AS(
							SELECT
								SUBSTRING(val, '(.+)\..+\..+') AS c_schema,
								SUBSTRING(val, '.+\.(.+)\..+') AS c_table,
								SUBSTRING(val, '.+\..+\.(.+)') AS c_column
							FROM cte1
						),

						cte3 AS(
							SELECT
								'%s %s %s (' || string_agg('"' || c_column || '"', ',') || ') ON "' || c_schema || '"."' || c_table || '" %s %s %s;' AS val
							FROM cte2
							GROUP BY c_schema, c_table
						)

					SELECT
						string_agg(val, E'\n')
					FROM cte3$qt$,
					NEW.column_references_grant, NEW._operation, _revoke, 'REFERENCES', tofrom, NEW.rolname, _grant
			) INTO _privileges;

			EXECUTE _privileges;

		END IF;

		-- DOMAIN, ALL --

		IF cardinality(NEW.domain_all) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.domain_all)) a WHERE unnest LIKE 'ALL%') THEN -- If all domains has been chosen for ALL privileges

				SELECT
					'DOMAIN ' || string_agg('"' || _schema || '"' || '.' || '"' || _domain || '"', ',')
				FROM roles.o_domains
				WHERE val != 'ALL' AND _schema IN(SELECT unnest(('{' || _schema_string || '}')::text[])) -- If specific schemas has been chosen
				INTO _privileges;

			ELSE -- If ALL option hasn't been chosen, then list the selected domains

				SELECT
					'DOMAIN ' || roles.array_to_csv(regexp_replace(NEW.domain_all::text, '\.', '\".\"', 'g')::text[], '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT(
				'%s ALL ON %s %s %s;',
				NEW._operation, _privileges, tofrom, NEW.rolname
			);

		END IF;

		IF cardinality(NEW.domain_all_grant) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.domain_all_grant)) a WHERE unnest LIKE 'ALL%') THEN -- If all DOMAINS has been chosen for ALL privileges

				SELECT
					'DOMAIN ' || string_agg('"' || _schema || '"' || '.' || '"' || _domain || '"', ',')
				FROM roles.o_domains
				WHERE val != 'ALL' AND _schema IN(SELECT unnest(('{' || _schema_string || '}')::text[])) -- If specific schemas has been chosen
				INTO _privileges;

			ELSE

				SELECT
					'DOMAIN ' || roles.array_to_csv(regexp_replace(NEW.domain_all_grant::text, '\.', '\".\"', 'g')::text[], '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT( -- REVOKE GRANT OPTION FOR or GRANT WITH GRANT OPTION
				'%s %s ALL ON %s %s %s %s;',
				NEW._operation, _revoke, _privileges, tofrom, NEW.rolname, _grant
			);

		END IF;

		-- DOMAIN, USAGE --

		IF cardinality(NEW.domain_usage) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.domain_usage)) a WHERE unnest LIKE 'ALL%') THEN -- If all DOMAINS has been chosen for ALL privileges

				SELECT
					'DOMAIN ' || string_agg('"' || _schema || '"' || '.' || '"' || _domain || '"', ',')
				FROM roles.o_domains
				WHERE val != 'ALL' AND _schema IN(SELECT unnest(('{' || _schema_string || '}')::text[])) -- If specific schemas has been chosen
				INTO _privileges;

			ELSE -- If ALL option hasn't been chosen, then list the selected DOMAINS

				SELECT
					'DOMAIN ' || roles.array_to_csv(regexp_replace(NEW.domain_usage::text, '\.', '\".\"', 'g')::text[], '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT(
				'%s USAGE ON %s %s %s;',
				NEW._operation, _privileges, tofrom, NEW.rolname
			);

		END IF;

		IF cardinality(NEW.domain_usage_grant) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.domain_usage_grant)) a WHERE unnest LIKE 'ALL%') THEN -- If all DOMAINS has been chosen for USAGE privileges

				SELECT
					'DOMAIN ' || string_agg('"' || _schema || '"' || '.' || '"' || _domain || '"', ',')
				FROM roles.o_domains
				WHERE val != 'ALL' AND _schema IN(SELECT unnest(('{' || _schema_string || '}')::text[])) -- If specific schemas has been chosen
				INTO _privileges;

			ELSE

				SELECT
					'DOMAIN ' || roles.array_to_csv(regexp_replace(NEW.domain_usage_grant::text, '\.', '\".\"', 'g')::text[], '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT( -- REVOKE GRANT OPTION FOR or GRANT WITH GRANT OPTION
				'%s %s USAGE ON %s %s %s %s;',
				NEW._operation, _revoke, _privileges, tofrom, NEW.rolname, _grant
			);

		END IF;

		-- FDW, ALL --

		IF cardinality(NEW.fdw_all) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.fdw_all)) a WHERE unnest LIKE 'ALL%') THEN -- If all FDWs has been chosen for ALL privileges

				SELECT
					'FOREIGN DATA WRAPPER ' || string_agg('"' || val || '"', ',')
				FROM roles.o_fdws
				WHERE val != 'ALL'
				INTO _privileges;

			ELSE -- If ALL option hasn't been chosen, then list the selected FDWs

				SELECT
					'FOREIGN DATA WRAPPER ' || roles.array_to_csv(NEW.fdw_all, '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT(
				'%s ALL ON %s %s %s;',
				NEW._operation, _privileges, tofrom, NEW.rolname
			);

		END IF;

		IF cardinality(NEW.fdw_all_grant) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.fdw_all_grant)) a WHERE unnest LIKE 'ALL%') THEN -- If all FDWs has been chosen for ALL privileges

				SELECT
					'FOREIGN DATA WRAPPER ' || string_agg('"' || val || '"', ',')
				FROM roles.o_fdws
				WHERE val != 'ALL'
				INTO _privileges;

			ELSE

				SELECT
					'FOREIGN DATA WRAPPER ' || roles.array_to_csv(NEW.fdw_all_grant, '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT( -- REVOKE GRANT OPTION FOR or GRANT WITH GRANT OPTION
				'%s %s ALL ON %s %s %s %s;',
				NEW._operation, _revoke, _privileges, tofrom, NEW.rolname, _grant
			);

		END IF;

		-- FDW, USAGE --

		IF cardinality(NEW.fdw_usage) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.fdw_usage)) a WHERE unnest LIKE 'ALL%') THEN -- If all FDWs has been chosen for USAGE privileges

				SELECT
					'FOREIGN DATA WRAPPER ' || string_agg('"' || val || '"', ',')
				FROM roles.o_fdws
				WHERE val != 'ALL'
				INTO _privileges;

			ELSE -- If ALL option hasn't been chosen, then list the selected FDWs

				SELECT
					'FOREIGN DATA WRAPPER ' || roles.array_to_csv(NEW.fdw_usage, '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT(
				'%s USAGE ON %s %s %s;',
				NEW._operation, _privileges, tofrom, NEW.rolname
			);

		END IF;

		IF cardinality(NEW.fdw_usage_grant) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.fdw_usage_grant)) a WHERE unnest LIKE 'ALL%') THEN -- If all FDWs has been chosen for USAGE privileges

				SELECT
					'FOREIGN DATA WRAPPER ' || string_agg('"' || val || '"', ',')
				FROM roles.o_fdws
				WHERE val != 'ALL'
				INTO _privileges;

			ELSE

				SELECT
					'FOREIGN DATA WRAPPER ' || roles.array_to_csv(NEW.fdw_usage_grant, '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT( -- REVOKE GRANT OPTION FOR or GRANT WITH GRANT OPTION
				'%s %s USAGE ON %s %s %s %s;',
				NEW._operation, _revoke, _privileges, tofrom, NEW.rolname, _grant
			);

		END IF;

		-- FOREIGN SERVER, ALL --

		IF cardinality(NEW.f_server_all) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.f_server_all)) a WHERE unnest LIKE 'ALL%') THEN -- If all FOREIGN SERVERS has been chosen for ALL privileges

				SELECT
					'FOREIGN SERVER ' || string_agg('"' || val || '"', ',')
				FROM roles.o_foreign_servers
				WHERE val != 'ALL'
				INTO _privileges;

			ELSE -- If ALL option hasn't been chosen, then list the selected FOREIGN SERVERS

				SELECT
					'FOREIGN SERVER ' || roles.array_to_csv(NEW.f_server_all, '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT(
				'%s ALL ON %s %s %s;',
				NEW._operation, _privileges, tofrom, NEW.rolname
			);

		END IF;

		IF cardinality(NEW.f_server_all_grant) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.f_server_all_grant)) a WHERE unnest LIKE 'ALL%') THEN -- If all FOREIGN SERVERS has been chosen for ALL privileges

				SELECT
					'FOREIGN SERVER ' || string_agg('"' || val || '"', ',')
				FROM roles.o_foreign_servers
				WHERE val != 'ALL'
				INTO _privileges;

			ELSE

				SELECT
					'FOREIGN SERVER ' || roles.array_to_csv(NEW.f_server_all_grant, '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT( -- REVOKE GRANT OPTION FOR or GRANT WITH GRANT OPTION
				'%s %s ALL ON %s %s %s %s;',
				NEW._operation, _revoke, _privileges, tofrom, NEW.rolname, _grant
			);

		END IF;

		-- FOREIGN SERVER, USAGE --

		IF cardinality(NEW.f_server_usage) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.f_server_usage)) a WHERE unnest LIKE 'ALL%') THEN -- If all FOREIGN SERVERS has been chosen for USAGE privileges

				SELECT
					'FOREIGN SERVER ' || string_agg('"' || val || '"', ',')
				FROM roles.o_foreign_servers
				WHERE val != 'ALL'
				INTO _privileges;

			ELSE -- If ALL option hasn't been chosen, then list the selected FOREIGN SERVERS

				SELECT
					'FOREIGN SERVER ' || roles.array_to_csv(NEW.f_server_usage, '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT(
				'%s USAGE ON %s %s %s;',
				NEW._operation, _privileges, tofrom, NEW.rolname
			);

		END IF;

		IF cardinality(NEW.f_server_all_grant) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.f_server_all_grant)) a WHERE unnest LIKE 'ALL%') THEN -- If all FOREIGN SERVERS has been chosen for USAGE privileges

				SELECT
					'FOREIGN SERVER ' || string_agg('"' || val || '"', ',')
				FROM roles.o_foreign_servers
				WHERE val != 'ALL'
				INTO _privileges;

			ELSE

				SELECT
					'FOREIGN SERVER ' || roles.array_to_csv(NEW.f_server_all_grant, '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT( -- REVOKE GRANT OPTION FOR or GRANT WITH GRANT OPTION
				'%s %s USAGE ON %s %s %s %s;',
				NEW._operation, _revoke, _privileges, tofrom, NEW.rolname, _grant
			);

		END IF;

		-- LANGUAGE, ALL --

		IF cardinality(NEW.language_all) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.language_all)) a WHERE unnest LIKE 'ALL%') THEN -- If all LANGUAGES has been chosen for ALL privileges

				SELECT
					'LANGUAGE ' || string_agg('"' || val || '"', ',')
				FROM roles.o_languages
				WHERE val != 'ALL'
				INTO _privileges;

			ELSE -- If ALL option hasn't been chosen, then list the selected LANGUAGES

				SELECT
					'LANGUAGE ' || roles.array_to_csv(NEW.language_all, '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT(
				'%s ALL ON %s %s %s;',
				NEW._operation, _privileges, tofrom, NEW.rolname
			);

		END IF;

		IF cardinality(NEW.language_all_grant) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.language_all_grant)) a WHERE unnest LIKE 'ALL%') THEN -- If all LANGUAGES has been chosen for ALL privileges

				SELECT
					'LANGUAGE ' || string_agg('"' || val || '"', ',')
				FROM roles.o_languages
				WHERE val != 'ALL'
				INTO _privileges;

			ELSE

				SELECT
					'LANGUAGE ' || roles.array_to_csv(NEW.language_all_grant, '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT( -- REVOKE GRANT OPTION FOR or GRANT WITH GRANT OPTION
				'%s %s ALL ON %s %s %s %s;',
				NEW._operation, _revoke, _privileges, tofrom, NEW.rolname, _grant
			);

		END IF;

		-- LANGUAGE, USAGE --

		IF cardinality(NEW.language_usage) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.language_usage)) a WHERE unnest LIKE 'ALL%') THEN -- If all LANGUAGES has been chosen for USAGE privileges

				SELECT
					'LANGUAGE ' || string_agg('"' || val || '"', ',')
				FROM roles.o_languages
				WHERE val != 'ALL'
				INTO _privileges;

			ELSE -- If ALL option hasn't been chosen, then list the selected LANGUAGES

				SELECT
					'LANGUAGE ' || roles.array_to_csv(NEW.language_usage, '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT(
				'%s USAGE ON %s %s %s;',
				NEW._operation, _privileges, tofrom, NEW.rolname
			);

		END IF;

		IF cardinality(NEW.language_usage_grant) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.language_usage_grant)) a WHERE unnest LIKE 'ALL%') THEN -- If all LANGUAGES has been chosen for USAGE privileges

				SELECT
					'LANGUAGE ' || string_agg('"' || val || '"', ',')
				FROM roles.o_languages
				WHERE val != 'ALL'
				INTO _privileges;

			ELSE

				SELECT
					'LANGUAGE ' || roles.array_to_csv(NEW.language_usage_grant, '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT( -- REVOKE GRANT OPTION FOR or GRANT WITH GRANT OPTION
				'%s %s USAGE ON %s %s %s %s;',
				NEW._operation, _revoke, _privileges, tofrom, NEW.rolname, _grant
			);

		END IF;

		-- LOID, ALL --

		IF cardinality(NEW.loid_all) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.loid_all)) a WHERE unnest LIKE 'ALL%') THEN -- If all LOIDs has been chosen for ALL privileges

				SELECT
					'LARGE OBJECT ' || string_agg('"' || val || '"', ',')
				FROM roles.o_loids
				WHERE val != 'ALL'
				INTO _privileges;

			ELSE -- If ALL option hasn't been chosen, then list the selected LOIDs

				SELECT
					'LARGE OBJECT ' || roles.array_to_csv(NEW.loid_all, '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT(
				'%s ALL ON %s %s %s;',
				NEW._operation, _privileges, tofrom, NEW.rolname
			);

		END IF;

		IF cardinality(NEW.loid_all_grant) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.loid_all_grant)) a WHERE unnest LIKE 'ALL%') THEN -- If all LOIDs has been chosen for ALL privileges

				SELECT
					'LARGE OBJECT ' || string_agg('"' || val || '"', ',')
				FROM roles.o_loids
				WHERE val != 'ALL'
				INTO _privileges;

			ELSE

				SELECT
					'LARGE OBJECT ' || roles.array_to_csv(NEW.loid_all_grant, '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT( -- REVOKE GRANT OPTION FOR or GRANT WITH GRANT OPTION
				'%s %s ALL ON %s %s %s %s;',
				NEW._operation, _revoke, _privileges, tofrom, NEW.rolname, _grant
			);

		END IF;

		-- LOID, SELECT --

		IF cardinality(NEW.loid_select) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.loid_select)) a WHERE unnest LIKE 'ALL%') THEN -- If all LOIDs has been chosen for SELECT privileges

				SELECT
					'LARGE OBJECT ' || string_agg('"' || val || '"', ',')
				FROM roles.o_loids
				WHERE val != 'ALL'
				INTO _privileges;

			ELSE -- If ALL option hasn't been chosen, then list the selected LOIDs

				SELECT
					'LARGE OBJECT ' || roles.array_to_csv(NEW.loid_select, '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT(
				'%s SELECT ON %s %s %s;',
				NEW._operation, _privileges, tofrom, NEW.rolname
			);

		END IF;

		IF cardinality(NEW.loid_select_grant) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.loid_select_grant)) a WHERE unnest LIKE 'ALL%') THEN -- If all LOIDs has been chosen for SELECT privileges

				SELECT
					'LARGE OBJECT ' || string_agg('"' || val || '"', ',')
				FROM roles.o_loids
				WHERE val != 'ALL'
				INTO _privileges;

			ELSE

				SELECT
					'LARGE OBJECT ' || roles.array_to_csv(NEW.loid_select_grant, '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT( -- REVOKE GRANT OPTION FOR or GRANT WITH GRANT OPTION
				'%s %s SELECT ON %s %s %s %s;',
				NEW._operation, _revoke, _privileges, tofrom, NEW.rolname, _grant
			);

		END IF;

		-- LOID, UPDATE --

		IF cardinality(NEW.loid_update) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.loid_update)) a WHERE unnest LIKE 'ALL%') THEN -- If all LOIDs has been chosen for UPDATE privileges

				SELECT
					'LARGE OBJECT ' || string_agg('"' || val || '"', ',')
				FROM roles.o_loids
				WHERE val != 'ALL'
				INTO _privileges;

			ELSE -- If ALL option hasn't been chosen, then list the selected LOIDs

				SELECT
					'LARGE OBJECT ' || roles.array_to_csv(NEW.loid_update, '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT(
				'%s UPDATE ON %s %s %s;',
				NEW._operation, _privileges, tofrom, NEW.rolname
			);

		END IF;

		IF cardinality(NEW.loid_update_grant) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.loid_update_grant)) a WHERE unnest LIKE 'ALL%') THEN -- If all LOIDs has been chosen for UPDATE privileges

				SELECT
					'LARGE OBJECT ' || string_agg('"' || val || '"', ',')
				FROM roles.o_loids
				WHERE val != 'ALL'
				INTO _privileges;

			ELSE

				SELECT
					'LARGE OBJECT ' || roles.array_to_csv(NEW.loid_update_grant, '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT( -- REVOKE GRANT OPTION FOR or GRANT WITH GRANT OPTION
				'%s %s UPDATE ON %s %s %s %s;',
				NEW._operation, _revoke, _privileges, tofrom, NEW.rolname, _grant
			);

		END IF;

		-- TABLESPACE, ALL --

		IF cardinality(NEW.tablespace_all) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.tablespace_all)) a WHERE unnest LIKE 'ALL%') THEN -- If all TABLESPACES has been chosen for ALL privileges

				SELECT
					'TABLESPACE ' || string_agg('"' || val || '"', ',')
				FROM roles.o_tablespaces
				WHERE val != 'ALL'
				INTO _privileges;

			ELSE -- If ALL option hasn't been chosen, then list the selected TABLESPACES

				SELECT
					'TABLESPACE ' || roles.array_to_csv(NEW.tablespace_all, '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT(
				'%s ALL ON %s %s %s;',
				NEW._operation, _privileges, tofrom, NEW.rolname
			);

		END IF;

		IF cardinality(NEW.tablespace_all_grant) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.tablespace_all_grant)) a WHERE unnest LIKE 'ALL%') THEN -- If all TABLESPACES has been chosen for ALL privileges

				SELECT
					'TABLESPACE ' || string_agg('"' || val || '"', ',')
				FROM roles.o_tablespaces
				WHERE val != 'ALL'
				INTO _privileges;

			ELSE

				SELECT
					'TABLESPACE ' || roles.array_to_csv(NEW.tablespace_all_grant, '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT( -- REVOKE GRANT OPTION FOR or GRANT WITH GRANT OPTION
				'%s %s ALL ON %s %s %s %s;',
				NEW._operation, _revoke, _privileges, tofrom, NEW.rolname, _grant
			);

		END IF;

		-- TABLESPACE, CREATE --

		IF cardinality(NEW.tablespace_create) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.tablespace_create)) a WHERE unnest LIKE 'ALL%') THEN -- If all TABLESPACES has been chosen for CREATE privileges

				SELECT
					'TABLESPACE ' || string_agg('"' || val || '"', ',')
				FROM roles.o_tablespaces
				WHERE val != 'ALL'
				INTO _privileges;

			ELSE -- If ALL option hasn't been chosen, then list the selected TABLESPACES

				SELECT
					'TABLESPACE ' || roles.array_to_csv(NEW.tablespace_create, '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT(
				'%s CREATE ON %s %s %s;',
				NEW._operation, _privileges, tofrom, NEW.rolname
			);

		END IF;

		IF cardinality(NEW.tablespace_create_grant) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.tablespace_create_grant)) a WHERE unnest LIKE 'ALL%') THEN -- If all TABLESPACES has been chosen for CREATE privileges

				SELECT
					'TABLESPACE ' || string_agg('"' || val || '"', ',')
				FROM roles.o_tablespaces
				WHERE val != 'ALL'
				INTO _privileges;

			ELSE

				SELECT
					'TABLESPACE ' || roles.array_to_csv(NEW.tablespace_create_grant, '"')
				INTO _privileges;

			END IF;

			EXECUTE FORMAT( -- REVOKE GRANT OPTION FOR or GRANT WITH GRANT OPTION
				'%s %s CREATE ON %s %s %s %s;',
				NEW._operation, _revoke, _privileges, tofrom, NEW.rolname, _grant
			);

		END IF;

		-- DEFAULT PRIVILEGES, SCHEMA --

		IF cardinality(NEW.default_schema) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.default_schema)) a WHERE unnest LIKE 'ALL%') THEN -- ALL-option selected

				_privileges := 'ALL';

			ELSE

				SELECT
					roles.array_to_csv(NEW.default_schema)
				INTO _privileges;

			END IF;

			EXECUTE FORMAT(
				'ALTER DEFAULT PRIVILEGES
					%s %s ON SCHEMAS %s %s;',
				NEW._operation, _privileges, tofrom, NEW.rolname
			);

		END IF;

		-- DEFAULT PRIVILEGES, SCHEMA GRANT --

		IF cardinality(NEW.default_schema_grant) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.default_schema_grant)) a WHERE unnest LIKE 'ALL%') THEN -- ALL-option selected

				_privileges := 'ALL';

			ELSE

				SELECT
					roles.array_to_csv(NEW.default_schema_grant)
				INTO _privileges;

			END IF;

			EXECUTE FORMAT(
				'ALTER DEFAULT PRIVILEGES
					%s %s %s ON SCHEMAS %s %s %s;',
				NEW._operation, _revoke, _privileges, tofrom, NEW.rolname, _grant
			);

		END IF;

		-- GLOBAL PRIVILEGES --

		IF NEW._global IS FALSE THEN

			_schema_string := 'IN SCHEMA ' || c_schema_string;

		ELSIF NEW._global IS TRUE THEN

			_schema_string := '';

		END IF;

		-- DEFAULT PRIVILEGES, TABLE --

		IF cardinality(NEW.default_table) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.default_table)) a WHERE unnest LIKE 'ALL%') THEN -- ALL-option selected

				_privileges := 'ALL';

			ELSE

				SELECT
					roles.array_to_csv(NEW.default_table)
				INTO _privileges;

			END IF;

			EXECUTE FORMAT(
				'ALTER DEFAULT PRIVILEGES
					%s
					%s %s ON TABLES %s %s;',
				_schema_string, NEW._operation, _privileges, tofrom, NEW.rolname
			);

		END IF;

		-- DEFAULT PRIVILEGES, TABLE GRANT --

		IF cardinality(NEW.default_table_grant) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.default_table_grant)) a WHERE unnest LIKE 'ALL%') THEN -- ALL-option selected

				_privileges := 'ALL';

			ELSE

				SELECT
					roles.array_to_csv(NEW.default_table_grant)
				INTO _privileges;

			END IF;

			EXECUTE FORMAT(
				'ALTER DEFAULT PRIVILEGES
					%s
					%s %s %s ON TABLES %s %s %s;',
				_schema_string, NEW._operation, _revoke, _privileges, tofrom, NEW.rolname, _grant
			);

		END IF;

		-- DEFAULT PRIVILEGES, FUNCTION --

		IF cardinality(NEW.default_function) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.default_function)) a WHERE unnest LIKE 'ALL%') THEN -- ALL-option selected

				_privileges := 'ALL';

			ELSE

				SELECT
					roles.array_to_csv(NEW.default_function)
				INTO _privileges;

			END IF;

			EXECUTE FORMAT(
				'ALTER DEFAULT PRIVILEGES
					%s
					%s %s ON FUNCTIONS %s %s;',
				_schema_string, NEW._operation, _privileges, tofrom, NEW.rolname
			);

		END IF;

		-- DEFAULT PRIVILEGES, FUNCTION GRANT --

		IF cardinality(NEW.default_function_grant) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.default_function_grant)) a WHERE unnest LIKE 'ALL%') THEN -- ALL-option selected

				_privileges := 'ALL';

			ELSE

				SELECT
					roles.array_to_csv(NEW.default_function_grant)
				INTO _privileges;

			END IF;

			EXECUTE FORMAT(
				'ALTER DEFAULT PRIVILEGES
					%s
					%s %s %s ON FUNCTIONS %s %s %s;',
				_schema_string, NEW._operation, _revoke, _privileges, tofrom, NEW.rolname, _grant
			);

		END IF;

		-- DEFAULT PRIVILEGES, SEQUENCE --

		IF cardinality(NEW.default_sequence) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.default_sequence)) a WHERE unnest LIKE 'ALL%') THEN -- ALL-option selected

				_privileges := 'ALL';

			ELSE

				SELECT
					roles.array_to_csv(NEW.default_sequence)
				INTO _privileges;

			END IF;

			EXECUTE FORMAT(
				'ALTER DEFAULT PRIVILEGES
					%s
					%s %s ON SEQUENCES %s %s;',
				_schema_string, NEW._operation, _privileges, tofrom, NEW.rolname
			);

		END IF;

		-- DEFAULT PRIVILEGES, SEQUENCE GRANT --

		IF cardinality(NEW.default_sequence_grant) > 0 THEN

			IF EXISTS(SELECT 1 FROM (SELECT UNNEST(NEW.default_sequence_grant)) a WHERE unnest LIKE 'ALL%') THEN -- ALL-option selected

				_privileges := 'ALL';

			ELSE

				SELECT
					roles.array_to_csv(NEW.default_sequence_grant)
				INTO _privileges;

			END IF;

			EXECUTE FORMAT(
				'ALTER DEFAULT PRIVILEGES
					%s
					%s %s %s ON SEQUENCES %s %s %s;',
				_schema_string, NEW._operation, _revoke, _privileges, tofrom, NEW.rolname, _grant
			);

		END IF;

	END IF;

	RETURN NULL;

END $$;

COMMENT ON FUNCTION roles.v_roles_privileges_trg() IS 'Management of role privileges.';


-- DROP FUNCTION IF EXISTS roles.v_roles_trg();

CREATE OR REPLACE FUNCTION roles.v_roles_trg()
	RETURNS trigger
	LANGUAGE plpgsql AS
$$

DECLARE

	rec record;
	superuser text;
	createdb text;
	createrole text;
	inherit text;
	login text;
	replication text;
	bypassrls text;
	conlimit text;
	pw text;
	valid text;
	_in_role text;
	_in_role_admin text;
	_role text;
	_adminrole text;
	_void boolean;

BEGIN

	IF TG_OP = 'DELETE' THEN -- DELETE operation

		DELETE
			FROM roles.v_roles_privileges
		WHERE rolname = OLD.rolname;

		IF OLD.rolname != 'PUBLIC' THEN

			EXECUTE FORMAT(
				'DROP ROLE IF EXISTS "%s"', OLD.rolname
			);

		END IF;

		RETURN NULL;

	END IF;

	IF NEW.rolname != 'PUBLIC' THEN

		IF TG_OP IN('UPDATE', 'INSERT') THEN -- Variables to create or alter role

			IF NEW.rolsuper IS TRUE THEN -- SUPERUSER variable

				superuser := 'SUPERUSER';

			ELSE

				superuser := 'NOSUPERUSER';

			END IF;


			IF NEW.rolcreatedb IS TRUE THEN -- CREATEDB variable

				createdb := 'CREATEDB';

			ELSE

				createdb := 'NOCREATEDB';

			END IF;


			IF NEW.rolcreaterole IS TRUE THEN -- CREATEROLE variable

				createrole := 'CREATEROLE';

			ELSE

				createrole := 'NOCREATEROLE';

			END IF;


			IF NEW.rolinherit IS TRUE THEN -- INHERIT variable

				inherit := 'INHERIT';

			ELSE

				inherit := 'NOINHERIT';

			END IF;


			IF NEW.rolcanlogin IS TRUE THEN -- CANLOGIN variable

				login := 'LOGIN';

			ELSE

				login := 'NOLOGIN';

			END IF;


			IF NEW.rolreplication IS TRUE THEN -- REPLICATION variable

				replication := 'REPLICATION';

			ELSE

				replication := 'NOREPLICATION';

			END IF;


			IF NEW.rolbypassrls IS TRUE THEN -- BYPASSRLS variable

				bypassrls := 'BYPASSRLS';

			ELSE

				bypassrls := 'NOBYPASSRLS';

			END IF;


			IF NEW.rolconnlimit > 0 THEN -- Connection limit, if value isn't greater than zero, then set to -1 for unlimited

				conlimit := 'CONNECTION LIMIT ' || NEW.rolconnlimit;

			ELSE

				conlimit := 'CONNECTION LIMIT -1';

			END IF;


			IF (NEW.rolpassword !~ '[\*]{3,}' AND NEW.rolpassword != '') OR NEW.rolpassword IS NULL THEN -- If password no longer is made up of asterisk and isn't an empty string OR if the password is NULL

				IF NEW.rolpassword IS NULL THEN -- Making it into af group role

					pw := 'PASSWORD NULL';

				ELSE -- New password, if isn't empty or made up of asterisks (set to three in a row)

					pw := 'PASSWORD ''' || NEW.rolpassword || '''';

				END IF;

			ELSE -- No changes to password

				pw := '';

			END IF;


			IF NEW.rolvaliduntil IS NOT NULL THEN -- VALID UNTIL variable

				valid := 'VALID UNTIL ''' || NEW.rolvaliduntil || '''';

			ELSE

				valid := '';

			END IF;


			IF cardinality(NEW.in_role) > 0 THEN -- If any values are present in the IN ROLE variable

				_in_role := (SELECT roles.array_to_csv(NEW.in_role, '"', NEW.rolname || ',PUBLIC'));

				IF TG_OP = 'INSERT' THEN -- Append IN ROLE

					_in_role := 'IN ROLE ' || _in_role;

				END IF;

			ELSE

				_in_role := '';

			END IF;


			IF cardinality(NEW.in_role_admin) > 0 THEN -- If any values are present in the IN ROLE ADMIN variable

				_in_role_admin := (SELECT
										roles.array_to_csv( -- Get list of values in the IN ROLE ADMIN variable
											NEW.in_role_admin, '"', COALESCE( -- Leave out any values select in the IN ROLE variable
																	roles.array_to_csv(NEW.in_role, '"') || ',', ''
																) || NEW.rolname || ',PUBLIC' -- As well as the role itself and PUBLIC
										)
									);

			END IF;


			IF cardinality(NEW.role) > 0 THEN -- If any values are present in the ROLE variable

				_role := (SELECT roles.array_to_csv(NEW.role, '"', NEW.rolname || ',PUBLIC'));

				IF TG_OP = 'INSERT' THEN -- Append ROLE

					_role := 'ROLE ' || _role;

				END IF;

			ELSE

				_role := '';

			END IF;


			IF cardinality(NEW.adminrole) > 0 THEN -- If any values are present in the ADMIN variable

				_adminrole := (SELECT
									roles.array_to_csv( -- Get list of values in the ADMIN variable
										NEW.adminrole, '"', COALESCE( -- Leave out any values select in the ROLE variable
																roles.array_to_csv(NEW.role, '"') || ',', ''
															) || NEW.rolname || ',PUBLIC' -- As well as the role itself and PUBLIC
									)
								);

				IF TG_OP = 'INSERT' THEN -- Append ADMIN

					_adminrole := 'ADMIN ' || _adminrole;

				END IF;

			ELSE

				_adminrole := '';

			END IF;

		END IF;

		IF TG_OP = 'UPDATE' THEN -- UPDATE operation

			IF NEW.rolname != OLD.rolname THEN -- If the name has been changed then RENAME

				EXECUTE FORMAT(
					'ALTER ROLE "%s" RENAME TO "%s"', OLD.rolname, NEW.rolname
				);

			END IF;

			NEW.rolname := '"' || NEW.rolname || '"';

			IF NEW.rolcanlogin IS FALSE AND OLD.rolcanlogin IS TRUE THEN

				EXECUTE FORMAT(
					$qt$SELECT
						pg_terminate_backend(a.pid)
					FROM pg_stat_activity a
					LEFT JOIN pg_authid b ON a.usesysid = b.oid
					WHERE a.datname = '%s' AND b.rolname = '%s';$qt$,
					current_database(), NEW.rolname
				)
				INTO _void;

			END IF;

			EXECUTE FORMAT( -- Alter role with new properties
				'ALTER ROLE %s %s %s %s %s %s %s %s %s %s %s',
				NEW.rolname, superuser, createdb, createrole, inherit, login, replication, bypassrls, conlimit, pw, valid
			);


			IF cardinality(OLD.in_role) > 0 THEN -- Remove any memberships in other roles

				EXECUTE FORMAT(
					'REVOKE %s FROM %s', (SELECT roles.array_to_csv(OLD.in_role, '"', NEW.rolname || ',PUBLIC')), NEW.rolname
				);

			END IF;

			IF cardinality(OLD.in_role_admin) > 0 THEN -- Remove any memberships ADMIN grant option in other roles

				EXECUTE FORMAT(
					'REVOKE %s FROM %s', (SELECT roles.array_to_csv(OLD.in_role_admin, '"', NEW.rolname || ',PUBLIC')), NEW.rolname
				);

			END IF;

			IF cardinality(OLD.role) > 0 THEN -- Remove any members from the role as a group itself

				EXECUTE FORMAT(
					'REVOKE %s FROM %s', NEW.rolname, (SELECT roles.array_to_csv(OLD.role, '"', NEW.rolname || ',PUBLIC'))
				);

			END IF;

			IF cardinality(OLD.adminrole) > 0 THEN -- Remove any members with ADMIN option from the role as a group itself

				EXECUTE FORMAT(
					'REVOKE %s FROM %s', NEW.rolname, (SELECT roles.array_to_csv(OLD.adminrole, '"', NEW.rolname || ',PUBLIC'))
				);

			END IF;


			IF _in_role != '' THEN -- Add any memberships in other roles

				EXECUTE FORMAT(
					'GRANT %s TO %s', _in_role, NEW.rolname
				);

			END IF;

			IF _in_role_admin != '' THEN -- Add any memberships with ADMIN option in other roles

				EXECUTE FORMAT(
					'GRANT %s TO %s WITH ADMIN OPTION', _in_role_admin, NEW.rolname
				);

			END IF;

			IF _role != '' THEN -- Add any members from the role as a group itself

				EXECUTE FORMAT(
					'GRANT %s TO %s', NEW.rolname, _role
				);

			END IF;

			IF _adminrole != '' THEN -- Add any members with ADMIN option from the role as a group itself

				EXECUTE FORMAT(
					'GRANT %s TO %s WITH ADMIN OPTION', NEW.rolname, _adminrole
				);

			END IF;

			-- MAYBE SET OPTIONS TO BE IMPLEMENTET

		ELSIF TG_OP = 'INSERT' THEN

			NEW.rolname := '"' || NEW.rolname || '"';

			EXECUTE FORMAT(
				'CREATE ROLE %s %s %s %s %s %s %s %s %s %s %s %s %s %s',
				NEW.rolname, superuser, createdb, createrole, inherit, login, replication, bypassrls, conlimit, pw, valid, _in_role, _role, _adminrole
			);

			IF _in_role_admin != '' THEN -- Add any memberships with ADMIN option in other roles

				EXECUTE FORMAT(
					'GRANT %s TO %s WITH ADMIN OPTION', _in_role_admin, NEW.rolname
				);

			END IF;

		END IF;

	END IF;

	IF NEW.disconnect_members IS TRUE THEN

		EXECUTE FORMAT(
		$qt$SELECT
			pg_terminate_backend(a.pid)
		FROM pg_stat_activity a
		LEFT JOIN pg_authid b ON a.usesysid = b.oid
		WHERE a.datname = '%s' AND b.rolname IN(SELECT roles.group_relation('%s'));$qt$,
		current_database(), NEW.rolname
		)
		INTO _void;

	END IF;

	RETURN NULL;

END $$;

COMMENT ON FUNCTION roles.v_roles_trg() IS 'Management of role properties as well as role membership.';


--
-- TABLES
--


-- DROP TABLE IF EXISTS roles.filter_role CASCADE;

CREATE TABLE roles.filter_role(
	role text,
	CONSTRAINT filter_role_pk PRIMARY KEY (role) WITH (fillfactor='10')
);

INSERT INTO roles.filter_role VALUES
	('postgres'),
	('pg_monitor'),
	('pg_read_all_settings'),
	('pg_read_all_stats'),
	('pg_signal_backend'),
	('pg_stat_scan_tables');


-- DROP TABLE IF EXISTS roles.filter_schema CASCADE;

CREATE TABLE roles.filter_schema(
	schema text,
	CONSTRAINT filter_schema_pk PRIMARY KEY (schema) WITH (fillfactor='10')
);

INSERT INTO roles.filter_schema VALUES
	('pg_catalog'),
	('information_schema'),
	('pg_toast_temp_1'),
	('pg_temp_1'),
	('pg_toast'),
	('roles');


-- DROP TABLE IF EXISTS roles.privilege CASCADE;

CREATE TABLE roles.privilege(
	privilege text,
	description text,
	sql text,
	_order integer,
	CONSTRAINT privilege_pk PRIMARY KEY (privilege) WITH (fillfactor='10')
);

INSERT INTO roles.privilege VALUES
	('r', 'SELECT', 'SELECT', 1),
	('r*', 'SELECT WITH GRANT OPTION', 'SELECT', 1),
	('w', 'UPDATE', 'UPDATE', 3),
	('w*', 'UPDATE WITH GRANT OPTION', 'UPDATE', 3),
	('a', 'INSERT', 'INSERT', 2),
	('a*', 'INSERT WITH GRANT OPTION', 'INSERT', 2),
	('d', 'DELETE', 'DELETE', 4),
	('d*', 'DELETE WITH GRANT OPTION', 'DELETE', 4),
	('D', 'TRUNCATE', 'TRUNCATE', 5),
	('D*', 'TRUNCATE WITH GRANT OPTION', 'TRUNCATE', 5),
	('x', 'REFERENCES', 'REFERENCES', 6),
	('x*', 'REFERENCES WITH GRANT OPTION', 'REFERENCES', 6),
	('t', 'TRIGGER', 'TRIGGER', 7),
	('t*', 'TRIGGER WITH GRANT OPTION', 'TRIGGER', 7),
	('X', 'EXECUTE', 'EXECUTE', 8),
	('X*', 'EXECUTE WITH GRANT OPTION', 'EXECUTE', 8),
	('U', 'USAGE', 'USAGE', 15),
	('U*', 'USAGE WITH GRANT OPTION', 'USAGE', 15),
	('C', 'CREATE', 'CREATE', 10),
	('C*', 'CREATE WITH GRANT OPTION', 'CREATE', 10),
	('c', 'CONNECT', 'CONNECT', 20),
	('c*', 'CONNECT WITH GRANT OPTION', 'CONNECT', 20),
	('T', 'TEMPORARY', 'TEMPORARY', 30),
	('T*', 'TEMPORARY WITH GRANT OPTION', 'TEMPORARY', 30);


--
-- VIEWS
--


-- DROP VIEW IF EXISTS roles.o_columns CASCADE;

CREATE VIEW roles.o_columns AS

SELECT
	table_schema AS _schema,
	table_name AS _table,
	column_name AS _column,
	table_schema || '.' || table_name || '.' || column_name AS val
FROM information_schema.columns
WHERE table_schema NOT IN(SELECT schema FROM roles.filter_schema)
ORDER BY table_schema, table_name, column_name;


-- DROP VIEW IF EXISTS roles.o_domains CASCADE;

CREATE VIEW roles.o_domains AS

SELECT
	b.nspname::text AS _schema,
	a.typname::text AS _domain,
	b.nspname || '.' || a.typname AS val
FROM pg_catalog.pg_type a
LEFT JOIN pg_catalog.pg_namespace b ON a.typnamespace = b.oid
WHERE a.typtype = 'd' AND b.nspname NOT IN(SELECT schema FROM roles.filter_schema EXCEPT SELECT 'public')

UNION

SELECT
	'ALL' AS _schema,
	'ALL' AS _domain,
	'ALL' AS val

ORDER BY 1, 2;


-- DROP VIEW IF EXISTS roles.o_fdws CASCADE;

CREATE VIEW roles.o_fdws AS

SELECT
	foreign_data_wrapper_name::text AS val
FROM information_schema.foreign_data_wrappers

UNION

SELECT
	'ALL' AS val

ORDER BY 1;


-- DROP VIEW IF EXISTS roles.o_foreign_servers CASCADE;

CREATE VIEW roles.o_foreign_servers AS

SELECT
	foreign_server_name::text AS val
FROM information_schema.foreign_servers

UNION

SELECT
	'ALL' AS val

ORDER BY 1;


-- DROP VIEW IF EXISTS roles.o_functions CASCADE;

CREATE VIEW roles.o_functions AS

SELECT
				b.nspname::text AS _schema,
				a.proname::text || '(' || COALESCE(pg_catalog.pg_get_function_identity_arguments(a.oid), '') || ')'  AS _function,
				'(' || COALESCE(pg_catalog.pg_get_function_identity_arguments(a.oid), '') || ')' AS _arguments,
				b.nspname::text || '.' || a.proname:: text || '(' || COALESCE(pg_catalog.pg_get_function_identity_arguments(a.oid), '') || ')' AS val
			FROM pg_catalog.pg_proc a
			LEFT JOIN pg_catalog.pg_namespace b ON a.pronamespace = b.oid
			WHERE b.nspname NOT IN(SELECT schema FROM roles.filter_schema UNION SELECT 'public') -- Only schemas not present in the filter

UNION

SELECT
	'ALL' AS _schema,
	'ALL' AS _function,
	'ALL' AS _arguments,
	'ALL' AS val

ORDER BY 4;


-- DROP VIEW IF EXISTS roles.o_languages CASCADE;

CREATE VIEW roles.o_languages AS

SELECT
	lanname::text AS val
FROM pg_catalog.pg_language
WHERE lanispl IS TRUE

UNION

SELECT
	'ALL' AS val

ORDER BY 1;


-- DROP VIEW IF EXISTS roles.o_loids CASCADE;

CREATE VIEW roles.o_loids AS

SELECT
	loid::text AS val
FROM pg_catalog.pg_largeobject

UNION

SELECT
	'ALL' AS val

ORDER BY 1;


-- DROP VIEW IF EXISTS roles.o_schemas CASCADE;

CREATE VIEW roles.o_schemas AS

SELECT
	nspname::text AS _schema
FROM pg_catalog.pg_namespace
WHERE nspname NOT IN(SELECT schema FROM roles.filter_schema EXCEPT SELECT 'public')

UNION

SELECT
	'ALL' AS schema

ORDER BY 1;


-- DROP VIEW IF EXISTS roles.o_sequences CASCADE;

CREATE VIEW roles.o_sequences AS

SELECT
	sequence_schema AS _schema,
	sequence_name AS _sequence,
	sequence_schema || '.' || sequence_name AS val
FROM information_schema.sequences
WHERE sequence_schema NOT IN(SELECT schema FROM roles.filter_schema)

UNION

SELECT
	'ALL' AS _schema,
	'ALL' AS _sequence,
	'ALL' AS val

ORDER BY 1, 2;


-- DROP VIEW IF EXISTS roles.o_tables CASCADE;

CREATE VIEW roles.o_tables AS

SELECT
	table_schema AS _schema,
	table_name AS _table,
	table_schema || '.' || table_name AS val
FROM information_schema.tables
WHERE table_schema NOT IN(SELECT schema FROM roles.filter_schema)

UNION

SELECT
	'ALL' AS _schema,
	'ALL' AS _table,
	'ALL' AS val

ORDER BY 1, 2;


-- DROP VIEW IF EXISTS roles.o_tablespaces CASCADE;

CREATE VIEW roles.o_tablespaces AS

SELECT
	spcname::text AS val
FROM pg_catalog.pg_tablespace

UNION

SELECT
	'ALL' AS val

ORDER BY 1;


-- DROP VIEW IF EXISTS roles.op_db CASCADE;

CREATE VIEW roles.op_db AS

SELECT
	privilege
FROM (
	VALUES ('ALL'), ('CREATE'), ('CONNECT'), ('TEMPORARY')
) i(privilege);


-- DROP VIEW IF EXISTS roles.op_function CASCADE;

CREATE VIEW roles.op_function AS

SELECT
	privilege
FROM (
	VALUES ('ALL'), ('EXECUTE')
) i(privilege);


-- DROP VIEW IF EXISTS roles.op_schema CASCADE;

CREATE VIEW roles.op_schema AS

SELECT
	privilege
FROM (
	VALUES ('ALL'), ('USAGE'), ('CREATE')
) i(privilege);


-- DROP VIEW IF EXISTS roles.op_sequence CASCADE;

CREATE VIEW roles.op_sequence AS

SELECT
	privilege
FROM (
	VALUES ('ALL'), ('USAGE'), ('SELECT'), ('UPDATE')
) i(privilege);


-- DROP VIEW IF EXISTS roles.op_table CASCADE;

CREATE VIEW roles.op_table AS

SELECT
	privilege
FROM (
	VALUES ('ALL'), ('SELECT'), ('INSERT'), ('UPDATE'), ('DELETE'), ('TRUNCATE'), ('REFERENCES'), ('TRIGGER')
) i(privilege);


-- DROP VIEW IF EXISTS roles.operations CASCADE;

CREATE VIEW roles.operations AS

SELECT
	operation,
	label
FROM (
	VALUES ('GRANT', 'GRANT TO'), ('REVOKE', 'REVOKE FROM')
) i(operation, label);


-- DROP VIEW IF EXISTS roles.v_membership CASCADE;

CREATE VIEW roles.v_membership AS

WITH

	cte1 AS(
		SELECT
			c.rolname,
			b.rolname AS grouprol,
			a.admin_option
		FROM pg_catalog.pg_auth_members a
		LEFT JOIN pg_catalog.pg_authid b ON a.roleid = b.oid -- The group
		LEFT JOIN pg_catalog.pg_authid c ON a.member = c.oid -- The role that is the member of the group
		WHERE c.rolname NOT IN(SELECT role FROM roles.filter_role)
		ORDER BY grouprol
	)

SELECT
	rolname,
	(('{' || string_agg(grouprol, ',') || '}')::text[]) AS grouprol,
	admin_option
FROM cte1
GROUP BY rolname, admin_option
ORDER BY rolname, grouprol;


-- DROP VIEW IF EXISTS roles.v_membership_rev CASCADE;

CREATE VIEW roles.v_membership_rev AS

WITH

	cte1 AS(
		SELECT
			c.rolname,
			b.rolname AS grouprol,
			a.admin_option
		FROM pg_catalog.pg_auth_members a
		LEFT JOIN pg_catalog.pg_authid b ON a.roleid = b.oid
		LEFT JOIN pg_catalog.pg_authid c ON a.member = c.oid
		WHERE c.rolname NOT IN(SELECT role FROM roles.filter_role)
		ORDER BY c.rolname
	)

SELECT
	grouprol,
	(('{' || string_agg(rolname, ',') || '}')::text[]) AS rolname,
	admin_option
FROM cte1
GROUP BY grouprol, admin_option
ORDER BY grouprol, rolname;


-- DROP VIEW IF EXISTS roles.v_roles CASCADE;

CREATE VIEW roles.v_roles AS

WITH

	cte1 AS(
		SELECT
			rolname::text
		FROM pg_catalog.pg_authid

		UNION

		SELECT
			'PUBLIC'::text AS rolname
	)

SELECT
	COALESCE(b.oid::int, -1) AS pkid,
	CASE
		WHEN rolpassword IS NULL AND rolcanlogin IS FALSE
		THEN 'Group role'
		ELSE 'Role'
	END AS roltype,
	a.rolname,
	b.rolsuper,
	b.rolinherit,
	b.rolcreaterole,
	b.rolcreatedb,
	b.rolcanlogin,
	b.rolreplication,
	b.rolbypassrls,
	b.rolconnlimit,
	CASE
		WHEN b.oid IS NOT NULL
		THEN '*******'::text
		ELSE NULL::text
	END AS rolpassword,
	b.rolvaliduntil::timestamp,
	-- IN ROLE --
	COALESCE(c.grouprol, '{}'::text[]) AS in_role,
	COALESCE(d.grouprol, '{}'::text[]) AS in_role_admin,
	-- ROLE --
	COALESCE(e.rolname, '{}'::text[]) AS role,
	COALESCE(f.rolname, '{}'::text[]) AS adminrole,
	FALSE AS disconnect_members
FROM cte1 a
LEFT JOIN pg_catalog.pg_authid b ON a.rolname = b.rolname::text
LEFT JOIN roles.v_membership c ON a.rolname = c.rolname AND c.admin_option IS FALSE
LEFT JOIN roles.v_membership d ON a.rolname = d.rolname AND d.admin_option IS TRUE

LEFT JOIN roles.v_membership_rev e ON a.rolname = e.grouprol AND e.admin_option IS FALSE
LEFT JOIN roles.v_membership_rev f ON a.rolname = f.grouprol AND f.admin_option IS TRUE

WHERE a.rolname NOT IN(SELECT role FROM roles.filter_role)
ORDER BY roltype, rolname;

CREATE TRIGGER v_roles_trg_iud INSTEAD OF INSERT OR DELETE OR UPDATE ON roles.v_roles FOR EACH ROW EXECUTE PROCEDURE roles.v_roles_trg();


-- DROP VIEW IF EXISTS roles.v_roles_overview CASCADE;

CREATE VIEW roles.v_roles_overview AS

WITH

	cte1 AS(
		SELECT
			rolname::text
		FROM pg_catalog.pg_authid

		UNION

		SELECT
			'PUBLIC'::text AS rolname
	)

SELECT
	COALESCE(b.oid::int, -1) AS pkid,
	CASE
		WHEN rolpassword IS NULL AND rolcanlogin IS FALSE
		THEN 'Group role'
		ELSE 'Role'
	END AS roltype,
	a.rolname,
	-- OVERVIEW --
	(SELECT * FROM roles.privilege_overview(a.rolname)) AS _privileges
FROM cte1 a
LEFT JOIN pg_catalog.pg_authid b ON a.rolname = b.rolname::text
WHERE a.rolname NOT IN(SELECT role FROM roles.filter_role)
ORDER BY roltype, rolname;


-- DROP VIEW IF EXISTS roles.v_roles_privileges CASCADE;

CREATE VIEW roles.v_roles_privileges AS

WITH

	cte1 AS(
		SELECT
			rolname::text
		FROM pg_catalog.pg_authid

		UNION

		SELECT
			'PUBLIC'::text AS rolname
	)

SELECT
	COALESCE(b.oid::int, -1) AS pkid,
	CASE
		WHEN rolpassword IS NULL AND rolcanlogin IS FALSE
		THEN 'Group role'
		ELSE 'Role'
	END AS roltype,
	a.rolname,
	NULL::text AS _operation,
	-- DATABASE PRIVILEGES --
	'{}'::text[] AS db_privilege,

	'{}'::text[] AS db_privilege_grant,
	-- SCHEMA PRIVILEGES --
	'{}'::text[] AS schema_create,
	'{}'::text[] AS schema_usage,
	'{}'::text[] AS schema_all,

	'{}'::text[] AS schema_create_grant,
	'{}'::text[] AS schema_usage_grant,
	'{}'::text[] AS schema_all_grant,
	-- TABLE PRIVILEGES --
	'{}'::text[] AS table_select,
	'{}'::text[] AS table_insert,
	'{}'::text[] AS table_update,
	'{}'::text[] AS table_delete,
	'{}'::text[] AS table_truncate,
	'{}'::text[] AS table_references,
	'{}'::text[] AS table_trigger,
	'{}'::text[] AS table_all,

	'{}'::text[] AS table_select_grant,
	'{}'::text[] AS table_insert_grant,
	'{}'::text[] AS table_update_grant,
	'{}'::text[] AS table_delete_grant,
	'{}'::text[] AS table_truncate_grant,
	'{}'::text[] AS table_references_grant,
	'{}'::text[] AS table_trigger_grant,
	'{}'::text[] AS table_all_grant,
	-- COLUMN PRIVILEGES --
	'{}'::text[] AS column_select,
	'{}'::text[] AS column_insert,
	'{}'::text[] AS column_update,
	'{}'::text[] AS column_references,
	'{}'::text[] AS column_all,

	'{}'::text[] AS column_select_grant,
	'{}'::text[] AS column_insert_grant,
	'{}'::text[] AS column_update_grant,
	'{}'::text[] AS column_references_grant,
	'{}'::text[] AS column_all_grant,
	-- FUNCTION PRIVILEGES --
	'{}'::text[] AS function_execute,
	'{}'::text[] AS function_all,

	'{}'::text[] AS function_execute_grant,
	'{}'::text[] AS function_all_grant,
	-- SEQUENCE PRIVILEGES --
	'{}'::text[] AS sequence_usage,
	'{}'::text[] AS sequence_select,
	'{}'::text[] AS sequence_update,
	'{}'::text[] AS sequence_all,

	'{}'::text[] AS sequence_usage_grant,
	'{}'::text[] AS sequence_select_grant,
	'{}'::text[] AS sequence_update_grant,
	'{}'::text[] AS sequence_all_grant,
	-- DOMAIN PRIVILEGES --
	'{}'::text[] AS domain_usage,
	'{}'::text[] AS domain_all,

	'{}'::text[] AS domain_usage_grant,
	'{}'::text[] AS domain_all_grant,
	-- FDW PRIVILEGES --
	'{}'::text[] AS fdw_usage,
	'{}'::text[] AS fdw_all,

	'{}'::text[] AS fdw_usage_grant,
	'{}'::text[] AS fdw_all_grant,
	-- FOREIGN SERVER PRIVILEGES --
	'{}'::text[] AS f_server_usage,
	'{}'::text[] AS f_server_all,

	'{}'::text[] AS f_server_usage_grant,
	'{}'::text[] AS f_server_all_grant,
	-- LANGUAGE PRIVILEGES --
	'{}'::text[] AS language_usage,
	'{}'::text[] AS language_all,

	'{}'::text[] AS language_usage_grant,
	'{}'::text[] AS language_all_grant,
	-- LOID PRIVILEGES --
	'{}'::text[] AS loid_select,
	'{}'::text[] AS loid_update,
	'{}'::text[] AS loid_all,

	'{}'::text[] AS loid_select_grant,
	'{}'::text[] AS loid_update_grant,
	'{}'::text[] AS loid_all_grant,
	-- TABLESPACE PRIVILEGES --
	'{}'::text[] AS tablespace_create,
	'{}'::text[] AS tablespace_all,

	'{}'::text[] AS tablespace_create_grant,
	'{}'::text[] AS tablespace_all_grant,
	-- DEFAULT PRIVILEGES --
	'{}'::text[] AS default_schema,
	'{}'::text[] AS default_schema_grant,

	'{}'::text[] AS default_table,
	'{}'::text[] AS default_table_grant,

	'{}'::text[] AS default_function,
	'{}'::text[] AS default_function_grant,

	'{}'::text[] AS default_sequence,
	'{}'::text[] AS default_sequence_grant,

	FALSE AS _global,

	'{}'::text[] AS virtual_schema,
	'{}'::text[] AS virtual_table
FROM cte1 a
LEFT JOIN pg_catalog.pg_authid b ON a.rolname = b.rolname::text
WHERE a.rolname NOT IN(SELECT role FROM roles.filter_role)
ORDER BY roltype, rolname;

CREATE TRIGGER v_roles_privileges_trg_iud INSTEAD OF INSERT OR DELETE OR UPDATE ON roles.v_roles_privileges FOR EACH ROW EXECUTE PROCEDURE roles.v_roles_privileges_trg();





--
--
--
--
--


--
-- SCHEMAS
--

DROP SCHEMA IF EXISTS styles CASCADE;

CREATE SCHEMA styles;
COMMENT ON SCHEMA styles IS 'Styling Management Module for PostgreSQL/QGIS 3.';


--
-- FUNCTIONS
--


-- DROP FUNCTION IF EXISTS styles.auto_check_update(from_schema_name text, from_table_name text, into_schema_name text, into_table_name text) CASCADE;

CREATE OR REPLACE FUNCTION styles.auto_check_update(from_schema_name text, from_table_name text, into_schema_name text DEFAULT NULL, into_table_name text DEFAULT NULL)
	RETURNS text
	LANGUAGE plpgsql AS
$$

DECLARE

	_ret text;

BEGIN

	WITH

		cte1 AS(
			SELECT
				quote_ident(a.attname) AS attname,
				a.attnum
			FROM pg_catalog.pg_attribute a
			LEFT JOIN pg_catalog.pg_class b ON a.attrelid = b.oid
			LEFT JOIN pg_catalog.pg_namespace c ON b.relnamespace = c.oid
			WHERE
				attnum > 0 AND
				(c.nspname, b.relname) = (COALESCE($3, $1), COALESCE($4, $2)) AND
				attname IN(SELECT styles.common_columns($1, $2, COALESCE($3, $1), COALESCE($4, $2)))
			ORDER BY a.attnum
		)

	SELECT
		FORMAT(
			$qt$
SELECT
	'1'
WHERE %s;
			$qt$, E'(\n\t' || string_agg('$1.' || attname || '::text', E',\n\t' ORDER BY attnum) || E'\n) = (\n\t' || string_agg('$2.' || attname || '::text', E',\n\t' ORDER BY attnum) || E'\n)'
		)
	FROM cte1
	INTO _ret;

	RETURN _ret;

-------------------
/*
	SELECT
		FORMAT(
			$qt$
SELECT
	'1'
WHERE %s;
			$qt$, E'(\n\t' || string_agg('$1.' || attname || ' = $2.' || attname, E' AND\n\t' ORDER BY attnum) || E'\n)'
		)
	FROM cte1
	INTO _ret;

	RETURN _ret;
*/
-------------------

END $$;

COMMENT ON FUNCTION styles.auto_check_update(from_schema_name text, from_table_name text, into_schema_name text, into_table_name text) IS 'SQL: SELECT ''1'' if all NEW values are equal all OLD values for all common columns.';


-- DROP FUNCTION IF EXISTS styles.auto_delete(schema_name text, table_name text) CASCADE;

CREATE OR REPLACE FUNCTION styles.auto_delete(schema_name text, table_name text)
	RETURNS text
	LANGUAGE plpgsql AS
$$

DECLARE

	_ret text;

BEGIN

	SELECT
		FORMAT(
			$qt$
DELETE FROM
	%1$I.%2$I
WHERE %3$s = $1.%3$s;
			$qt$, $1, $2, styles.primary_key($1, $2)
		)
	INTO _ret;

	RETURN _ret;

END $$;

COMMENT ON FUNCTION styles.auto_delete(schema_name text, table_name text) IS 'SQL: DELETE from table where PK equals OLD PK. Only one column as PK.';


-- DROP FUNCTION IF EXISTS styles.auto_insert(from_schema_name text, from_table_name text, into_schema_name text, into_table_name text) CASCADE;

CREATE OR REPLACE FUNCTION styles.auto_insert(from_schema_name text, from_table_name text, into_schema_name text DEFAULT NULL, into_table_name text DEFAULT NULL)
	RETURNS text
	LANGUAGE plpgsql AS
$$

DECLARE

	_ret text;

BEGIN

	WITH

		cte1 AS( -- Get columns and default values
			SELECT
				quote_ident(a.attname) AS attname,
				a.attnum,
				COALESCE(d.adsrc, e.typdefault) AS adsrc
			FROM pg_catalog.pg_attribute a
			LEFT JOIN pg_catalog.pg_class b ON a.attrelid = b.oid
			LEFT JOIN pg_catalog.pg_namespace c ON b.relnamespace = c.oid
			LEFT JOIN pg_catalog.pg_attrdef d ON (d.adrelid, d.adnum) = (b.oid, a.attnum)
			LEFT JOIN pg_catalog.pg_type e ON a.atttypid = e.oid
			WHERE
				attnum > 0 AND
				(c.nspname, b.relname) = (COALESCE($3, $1), COALESCE($4, $2)) AND
				attname IN(SELECT styles.common_columns($1, $2, COALESCE($3, $1), COALESCE($4, $2)))
			ORDER BY a.attnum
		)

	SELECT
		FORMAT(
			$qt$
INSERT INTO %I.%I(
	%s
) VALUES(
	%s
);
			$qt$, COALESCE($3, $1), COALESCE($4, $2), string_agg(attname, E',\n\t' ORDER BY attnum), string_agg(COALESCE('COALESCE($1.' || attname || ',' || adsrc || ')', '$1.' || attname), E',\n\t' ORDER BY attnum)
		)
	FROM cte1
	INTO _ret;

	RETURN _ret;

END $$;

COMMENT ON FUNCTION styles.auto_insert(from_schema_name text, from_table_name text, into_schema_name text, into_table_name text) IS 'SQL: INSERT common columns into table.';


-- DROP FUNCTION IF EXISTS styles.auto_update(from_schema_name text, from_table_name text, into_schema_name text, into_table_name text) CASCADE;

CREATE OR REPLACE FUNCTION styles.auto_update(from_schema_name text, from_table_name text, into_schema_name text DEFAULT NULL, into_table_name text DEFAULT NULL)
	RETURNS text
	LANGUAGE plpgsql AS
$$

DECLARE

	_ret text;

BEGIN

	WITH

		cte1 AS( -- Get columns and default values
			SELECT
				quote_ident(a.attname) AS attname,
				a.attnum,
				d.adsrc
			FROM pg_catalog.pg_attribute a
			LEFT JOIN pg_catalog.pg_class b ON a.attrelid = b.oid
			LEFT JOIN pg_catalog.pg_namespace c ON b.relnamespace = c.oid
			LEFT JOIN pg_catalog.pg_attrdef d ON (d.adrelid, d.adnum) = (b.oid, a.attnum)
			WHERE
				attnum > 0 AND
				(c.nspname, b.relname) = (COALESCE($3, $1), COALESCE($4, $2)) AND
				attname IN(SELECT styles.common_columns($1, $2, COALESCE($3, $1), COALESCE($4, $2)))
			ORDER BY a.attnum
		)

	SELECT
		FORMAT(
			$qt$
UPDATE %1$I.%2$I
	SET
		%3$s
WHERE %4$I = $2.%4$I;
			$qt$,  COALESCE($3, $1), COALESCE($4, $2), string_agg(attname || ' = $1.' || attname, E',\n\t\t' ORDER BY attnum), styles.primary_key(COALESCE($3, $1), COALESCE($4, $2))
		)
	FROM cte1
	INTO _ret;

	RETURN _ret;

END $$;

COMMENT ON FUNCTION styles.auto_update(from_schema_name text, from_table_name text, into_schema_name text, into_table_name text) IS 'SQL: UPDATE common columns to NEW values where PK equals OLD PK. Only one column as PK.';


-- DROP FUNCTION IF EXISTS styles.check_tbl(schema_name text, table_name text, column_name text) CASCADE;

CREATE OR REPLACE FUNCTION styles.check_tbl(schema_name text DEFAULT NULL, table_name text DEFAULT NULL, column_name text DEFAULT NULL)
	RETURNS BOOLEAN
	LANGUAGE plpgsql AS
$$

BEGIN

--
-- If name of column has been specified
-- Check for column
--

	IF
		$1 IS NOT NULL AND
		$2 IS NOT NULL AND
		$3 IS NOT NULL
	THEN

		IF EXISTS(SELECT
					'1'
				FROM pg_catalog.pg_attribute a
				LEFT JOIN pg_catalog.pg_class b ON a.attrelid = b.oid
				LEFT JOIN pg_catalog.pg_namespace c ON b.relnamespace = c.oid
				WHERE a.attnum > 0 AND c.nspname = $1 AND b.relname = $2 AND a.attname = $3
		) THEN

			RETURN TRUE;

		ELSE

			RETURN NULL;

		END IF;

--
-- If only name of schema and table has been specified
-- Check for table
--

	ELSIF
		$1 IS NOT NULL AND
		$2 IS NOT NULL
	THEN

		IF EXISTS(SELECT
					'1'
				FROM pg_catalog.pg_class a
				LEFT JOIN pg_catalog.pg_namespace b ON a.relnamespace = b.oid
				WHERE b.nspname = $1 AND a.relname = $2
		) THEN

			RETURN TRUE;

		ELSE

			RETURN NULL;

		END IF;

--
-- If only name of schema has been specified
-- Check for schema
--

	ELSIF $1 IS NOT NULL THEN

		IF EXISTS(SELECT
					'1'
				FROM pg_catalog.pg_namespace
				WHERE nspname = $1
		) THEN

			RETURN TRUE;

		ELSE

			RETURN NULL;

		END IF;

	ELSE

		RETURN NULL;

	END IF;

END $$;

COMMENT ON FUNCTION styles.check_tbl(schema_name text, table_name text, column_name text) IS 'Checks for existing objects:
If only schema is specified then schema,
if table.. then table and
if column.. then column.
Returns NULL if the object doesn''t exist';


-- DROP FUNCTION IF EXISTS styles.common_columns(schema_name_1 text, table_name_1 text, schema_name_2 text, table_name_2 text) CASCADE;

CREATE OR REPLACE FUNCTION styles.common_columns(schema_name_1 text, table_name_1 text, schema_name_2 text, table_name_2 text)
	RETURNS TABLE (
		column_name text
	)
	LANGUAGE plpgsql AS
$$

BEGIN

	RETURN QUERY
	SELECT
		a.attname::text
	FROM (
		SELECT
			a.attname
		FROM pg_catalog.pg_attribute a
		LEFT JOIN pg_catalog.pg_class b ON a.attrelid = b.oid
		LEFT JOIN pg_catalog.pg_namespace c ON b.relnamespace = c.oid
		WHERE attnum > 0 AND (c.nspname, b.relname) = ($1, $2)

		INTERSECT

		SELECT
			a.attname
		FROM pg_catalog.pg_attribute a
		LEFT JOIN pg_catalog.pg_class b ON a.attrelid = b.oid
		LEFT JOIN pg_catalog.pg_namespace c ON b.relnamespace = c.oid
		WHERE attnum > 0 AND (c.nspname, b.relname) = ($3, $4)
	) a
	LEFT JOIN (
		SELECT
			a.attname,
			a.attnum
		FROM pg_catalog.pg_attribute a
		LEFT JOIN pg_catalog.pg_class b ON a.attrelid = b.oid
		LEFT JOIN pg_catalog.pg_namespace c ON b.relnamespace = c.oid
		WHERE (c.nspname, b.relname) = ($3, $4)
	) b ON a.attname = b.attname
	ORDER BY b.attnum;

END $$;

COMMENT ON FUNCTION styles.common_columns(schema_name_1 text, table_name_1 text, schema_name_2 text, table_name_2 text) IS 'Finds common columns between tables/views based on the name of the column';


-- DROP FUNCTION IF EXISTS styles.create_auto_trigger(view_schema text, view_name text, table_schema text, table_name text) CASCADE;

CREATE OR REPLACE FUNCTION styles.create_auto_trigger(view_schema text, view_name text, table_schema text, table_name text)
	RETURNS VOID
	LANGUAGE plpgsql AS
$$

BEGIN

	EXECUTE FORMAT(
		$qt$
			CREATE TRIGGER %2$s_iud INSTEAD OF INSERT OR DELETE OR UPDATE ON %1$I.%2$I FOR EACH ROW EXECUTE PROCEDURE styles.auto_update(%3$s, %4$s);
		$qt$, $1, $2, $3, $4
	);

END $$;

COMMENT ON FUNCTION styles.create_auto_trigger(view_schema text, view_name text, table_schema text, table_name text) IS 'Creates a trigger with the necessary parameters to enable automatic update of views.';


-- DROP FUNCTION IF EXISTS styles.geometry_of(schema_name text, table_name text) CASCADE;

CREATE OR REPLACE FUNCTION styles.geometry_of(schema_name text, table_name text)
	RETURNS TABLE (
		geom_column text,
		srid integer,
		type text,
		geom_type integer
	)
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	_schema_postgis text;

BEGIN

--
-- Find schema of extension PostGIS
--

	SELECT
		b.nspname::text
	FROM pg_catalog.pg_extension a
	LEFT JOIN pg_catalog.pg_namespace b ON a.extnamespace = b.oid
	WHERE extname = 'postgis'
	INTO _schema_postgis;

--
-- Select information for the given table from metadata-table
--

	RETURN QUERY
	EXECUTE FORMAT(
		$$
			SELECT
				f_geometry_column::text,
				srid,
				type::text,
				CASE
					WHEN type ILIKE '%%POLYGON'
					THEN 2
					WHEN type ILIKE '%%LINESTRING'
					THEN 1
					WHEN type ILIKE '%%POINT'
					THEN 0
				END AS geom_type
			FROM %s.geometry_columns
			WHERE f_table_schema = '%s' AND f_table_name = '%s'
		$$, _schema_postgis, $1, $2
	);

END $BODY$;

COMMENT ON FUNCTION styles.geometry_of(schema_name text, table_name text) IS 'Finds geometry information for a given table.';


-- DROP FUNCTION IF EXISTS styles.primary_key(schema_name text, table_name text, _prefix text) CASCADE;

CREATE OR REPLACE FUNCTION styles.primary_key(schema_name text, table_name text, _prefix text DEFAULT '')
	RETURNS text
	LANGUAGE plpgsql AS
$$

DECLARE

	_ret text;

BEGIN

	WITH

		cte1 AS(
			SELECT
				UNNEST(a.conkey) AS conkey
			FROM pg_catalog.pg_constraint a
			LEFT JOIN pg_catalog.pg_class b ON a.conrelid = b.oid
			LEFT JOIN pg_catalog.pg_namespace c ON b.relnamespace = c.oid
			WHERE (c.nspname, b.relname, a.contype) = ($1, $2, 'p')
		)

	SELECT
		string_agg(_prefix || a.attname, ',')
	FROM pg_catalog.pg_attribute a
	LEFT JOIN pg_catalog.pg_class b ON a.attrelid = b.oid
	LEFT JOIN pg_catalog.pg_namespace c ON b.relnamespace = c.oid
	WHERE (c.nspname, b.relname) = ($1, $2) AND attnum IN(SELECT conkey FROM cte1)
	INTO _ret;

	RETURN _ret;

END $$;

COMMENT ON FUNCTION styles.primary_key(schema_name text, table_name text, _prefix text) IS 'Lists PK column names of the specified table.';


-- DROP FUNCTION IF EXISTS styles.convert_hex_rgb(hex_code text) CASCADE;

CREATE OR REPLACE FUNCTION styles.convert_hex_rgb(hex_code text)
	RETURNS TABLE (
		rgb text
	)
	LANGUAGE plpgsql AS
$BODY$

BEGIN

	RETURN QUERY
	SELECT
		('x'||substr(hex_code,2,2))::bit(8)::int ||','||
		('x'||substr(hex_code,4,2))::bit(8)::int ||','||
		('x'||substr(hex_code,6,2))::bit(8)::int;

END $BODY$;

COMMENT ON FUNCTION styles.convert_hex_rgb(hex_code text) IS 'Converts hex colour (#0000FF) to RGB (0,0,255).';


-- DROP FUNCTION IF EXISTS styles.convert_rgb_hex(rgb text) CASCADE;

CREATE OR REPLACE FUNCTION styles.convert_rgb_hex(rgb text)
	RETURNS TABLE (
		hex_code text
	)
	LANGUAGE plpgsql AS
$BODY$

BEGIN

	RETURN QUERY
	SELECT
		'#' ||
		to_hex(SUBSTRING($1, '(.+),.+,.+')) ||
		to_hex(SUBSTRING($1, '.+,(.+),.+')) ||
		to_hex(SUBSTRING($1, '.+,.+,(.+)'));

END $BODY$;

COMMENT ON FUNCTION styles.convert_rgb_hex(rgb text) IS 'Converts RGB (0,0,255 to hex colour (#0000FF)).';


-- DROP FUNCTION IF EXISTS styles.random_hex() CASCADE;

CREATE OR REPLACE FUNCTION styles.random_hex()
	RETURNS TABLE (
		hex_code text
	)
	LANGUAGE plpgsql AS
$BODY$

BEGIN

	RETURN QUERY
	WITH
		conversion(val, code) AS( -- Conversion af numbers from 10-15 to corresponding letters
			VALUES (10, 'A'), (11, 'B'), (12, 'C'), (13, 'D'), (14, 'E'), (15, 'F')
		),

		cte1 AS( -- Create six random numbers from 0-15
			SELECT
				round(random()::numeric*15) AS hex1,
				round(random()::numeric*15) AS hex2,
				round(random()::numeric*15) AS hex3,
				round(random()::numeric*15) AS hex4,
				round(random()::numeric*15) AS hex5,
				round(random()::numeric*15) AS hex6
		)

		SELECT
			'#' ||
			CASE
				WHEN hex1 >= 10
				THEN (SELECT code FROM conversion WHERE hex1 = val)
				ELSE hex1::text
			END ||
			CASE
				WHEN hex2 >= 10
				THEN (SELECT code FROM conversion WHERE hex2 = val)
				ELSE hex2::text
			END ||
			CASE
				WHEN hex3 >= 10
				THEN (SELECT code FROM conversion WHERE hex3 = val)
				ELSE hex3::text
			END ||
			CASE
				WHEN hex4 >= 10
				THEN (SELECT code FROM conversion WHERE hex4 = val)
				ELSE hex4::text
			END ||
			CASE
				WHEN hex5 >= 10
				THEN (SELECT code FROM conversion WHERE hex5 = val)
				ELSE hex5::text
			END ||
			CASE
				WHEN hex6 >= 10
				THEN (SELECT code FROM conversion WHERE hex6 = val)
				ELSE hex6::text
			END
		FROM cte1;

END $BODY$;

COMMENT ON FUNCTION styles.random_hex() IS 'Generate a random hex colour (#0000FF).';


-- DROP FUNCTION IF EXISTS styles.random_rgb() CASCADE;

CREATE OR REPLACE FUNCTION styles.random_rgb()
	RETURNS TABLE (
		rgb text
	)
	LANGUAGE plpgsql AS
$BODY$

BEGIN

	RETURN QUERY
	SELECT
				round(random()::numeric * 255) || ',' ||
				round(random()::numeric * 255) || ',' ||
				round(random()::numeric * 255);

END $BODY$;

COMMENT ON FUNCTION styles.random_rgb() IS 'Generate a random RGB colour (0,0,255).';


-- DROP FUNCTION IF EXISTS styles._entity_ref(text) CASCADE;

CREATE OR REPLACE FUNCTION styles._entity_ref(text)
	RETURNS TEXT
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	_rec record;
	_ret text;

BEGIN

--
-- SELECT style into _ret to be used in LOOP
--

	SELECT
		$1
	INTO _ret;

--
-- LOOP through known XML entity references
-- Replace each occourence with respective reference
--

	FOR _rec IN(
		SELECT
			_val,
			_ref
		FROM (
			VALUES
				('&', '&amp;'), -- & FIRST OR IT WILL REPLACE THE OTHER REFERENCES!
				('<', '&lt;'),
				('>', '&gt;'),
				('''', '&apos;'),
				('"', '&quot;')
		) t(_val, _ref)
	) LOOP

		EXECUTE FORMAT(
			$$
				SELECT
					regexp_replace($a$%1$s$a$, $a$%2$s$a$, $a$%3$s$a$, 'g')
			$$,
			_ret, _rec._val, _rec._ref
		)
		INTO _ret;

	END LOOP;

	RETURN _ret;

END $BODY$;

COMMENT ON FUNCTION styles._entity_ref(text) IS 'Convert known entity references of XML.';


-- DROP FUNCTION IF EXISTS styles._qgis(text) CASCADE;

CREATE OR REPLACE FUNCTION styles._qgis(text)
	RETURNS XML
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	_ret xml;

BEGIN

	SELECT
		regexp_replace($1, '<!DOCTYPE.*?>\n', '')::xml
	INTO _ret;

	RETURN _ret;

END $BODY$;

COMMENT ON FUNCTION styles._qgis(text) IS 'Convert QGIS-style to well formed XML.';


-- DROP FUNCTION IF EXISTS styles._qgis(xml) CASCADE;

CREATE OR REPLACE FUNCTION styles._qgis(xml)
	RETURNS XML
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	_ret xml;

BEGIN

	SELECT
		regexp_replace($1::text, '<!DOCTYPE.*?>\n', '')::xml
	INTO _ret;

	RETURN _ret;

END $BODY$;

COMMENT ON FUNCTION styles._qgis(xml) IS 'Convert QGIS-style to well formed XML.';


-- DROP FUNCTION IF EXISTS styles._regex(text) CASCADE;

CREATE OR REPLACE FUNCTION styles._regex(text)
	RETURNS TEXT
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	_rec record;
	_ret text;

BEGIN

--
-- SELECT style into _ret to be used in LOOP
--

	SELECT
		$1
	INTO _ret;

--
-- LOOP through known XML entity references
-- Replace each occourence with respective reference
--

	FOR _rec IN(
		SELECT
			_val,
			_ref
		FROM (
			VALUES
				('\\', '\\\\'), -- \ FIRST OR IT WILL REPLACE THE OTHER REFERENCES!
				('\.', '\.'),
				('\*', '\*'),
				('\+', '\+'),
				('\?', '\?')
		) t(_val, _ref)
	) LOOP

		EXECUTE FORMAT(
			$$
				SELECT
					regexp_replace($a$%1$s$a$, $a$%2$s$a$, $a$%3$s$a$, 'g')
			$$,
			_ret, _rec._val, _rec._ref
		)
		INTO _ret;

	END LOOP;

	RETURN _ret;

END $BODY$;

COMMENT ON FUNCTION styles._regex(text) IS 'Convert regex quantifiers into literals.';


-- DROP FUNCTION IF EXISTS styles.gt_style_id(xml) CASCADE;

CREATE OR REPLACE FUNCTION styles.gt_style_id(xml)
	RETURNS INTEGER
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	_ret text;

BEGIN

	WITH

		cte1 AS(
			SELECT
				*
			FROM UNNEST(xpath('//customproperties/property[@key="variableNames"]/value/text()', $1),
				xpath('//customproperties/property[@key="variableValues"]/value/text()', $1),
				xpath('//customproperties/property[@key="variableNames"]/@value', $1),
				xpath('//customproperties/property[@key="variableValues"]/@value', $1)
			) AS t(name, _val, _attrib_name, _attrib_val)
		)

	SELECT
		COALESCE((_val::text)::int, (_attrib_val::text)::int)
	FROM cte1
	WHERE name::text = 'style_id' OR _attrib_name::text = 'style_id'
	INTO _ret;

	RETURN _ret;

END $BODY$;

COMMENT ON FUNCTION styles.gt_style_id(xml) IS 'Retrieve style_id variable.';


-- DROP FUNCTION IF EXISTS styles.gt_symbols(xml, schema_name text, table_name text, column_name text) CASCADE;

CREATE OR REPLACE FUNCTION styles.gt_symbols(xml, schema_name text DEFAULT NULL, table_name text DEFAULT NULL, column_name text DEFAULT NULL)
	RETURNS TABLE(
		_schema text,
		_table text,
		_column text,
		_xml text
	)
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	_where text;

BEGIN

	SELECT
		COALESCE('WHERE a._attrib_val IN(SELECT ' || column_name || '::text FROM ' || schema_name || '.' || table_name || ')', '')
	INTO _where;

	RETURN QUERY
	EXECUTE FORMAT(
		$$
			WITH

				cte1 AS(
					SELECT
						UNNEST(xpath('//symbols/symbol', $a$%1$s$a$::xml)) AS symbol,
						UNNEST(xpath('//symbols/symbol/@name', $a$%1$s$a$::xml))::text AS _attrib_sym
				),

				cte2 AS(
					SELECT
						UNNEST(xpath('//categories/category/@value', $a$%1$s$a$::xml))::text AS _attrib_val,
						UNNEST(xpath('//categories/category/@symbol', $a$%1$s$a$::xml))::text AS _attrib_sym
				)

				SELECT
					'%2$s'::text,
					'%3$s'::text,
					CASE
						WHEN '%4$s' != ''
						THEN a._attrib_val
						ELSE NULL
					END,
					b.symbol::text
				FROM cte2 a
				INNER JOIN cte1 b ON a._attrib_sym = b._attrib_sym
				%5$s
		$$, $1, COALESCE(schema_name, 'NULL'), COALESCE(table_name, 'NULL'), column_name,_where
	);

END $BODY$;

COMMENT ON FUNCTION styles.gt_symbols(xml, schema_name text, table_name text, column_name text) IS 'Retrieve symbols (based on table values).';


-- DROP FUNCTION IF EXISTS styles.name_replace(text, name text) CASCADE;

CREATE OR REPLACE FUNCTION styles.name_replace(text, name text)
	RETURNS TEXT
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	_ret text := $1;

BEGIN

	SELECT
		regexp_replace(_ret, 'name="[0-9]*"', 'name="' || name || '"')
	INTO _ret;

	SELECT
		regexp_replace(_ret, 'name="@[0-9]*@', 'name="@' || name || '@')
	INTO _ret;

	RETURN _ret;

END $BODY$;

COMMENT ON FUNCTION styles.name_replace(text, name text) IS 'Convert table inputs into SQL selection.';


-- DROP FUNCTION IF EXISTS styles._retrieve(xml, xml_tag text, xml_attribute text) CASCADE;

CREATE OR REPLACE FUNCTION styles._retrieve(xml, xml_tag text, xml_attribute text DEFAULT NULL)
	RETURNS TABLE(
		_tag text,
		_xml text
	)
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	_style xml := $1;
	_attrib text := COALESCE('[@' || xml_attribute || ']', '');

BEGIN

	RETURN QUERY
	WITH

		cte1 AS(
			SELECT
				UNNEST(xpath('//' || xml_tag || _attrib, _style))::text AS _body
		)

	SELECT
		SUBSTRING(_body, '^<.*?>') AS _tag,
		_body AS _xml
	FROM cte1;

END $BODY$;

COMMENT ON FUNCTION styles._retrieve(xml, xml_tag text, xml_attribute text) IS 'Retrieve part of XML.';


-- DROP FUNCTION IF EXISTS styles._select(schema_name text, table_name text, _sql text, _label text, filter_clause text, order_clause text) CASCADE;

CREATE OR REPLACE FUNCTION styles._select(schema_name text, table_name text, _sql text, _label text DEFAULT NULL, filter_clause text DEFAULT 'TRUE', order_clause text DEFAULT NULL)
	RETURNS TABLE (
		_row int,
		_ret text,
		_lab text
	)
	LANGUAGE plpgsql AS
$BODY$

BEGIN

	RETURN QUERY
	EXECUTE FORMAT(
		$$
			WITH

				cte1 AS(
					SELECT DISTINCT ON (%3$s)
						(ROW_NUMBER() OVER(ORDER BY %6$s))::int AS _row,
						(%3$s)::text AS _ret,
						(%4$s)::text AS _lab
					FROM %1$I.%2$I
					WHERE (%3$s)::text IS NOT NULL AND %5$s
					--ORDER BY %6$s
				)

			SELECT
				(ROW_NUMBER() OVER(ORDER BY _row))::int AS _row,
				_ret,
				_lab
			FROM cte1 a
			ORDER BY a._row
		$$,
		$1, $2, $3, COALESCE($4, $3), COALESCE($5, 'TRUE'), COALESCE($6, $3)
	);

END $BODY$;

COMMENT ON FUNCTION styles._select(schema_name text, table_name text, _sql text, _label text, filter_clause text, order_clause text) IS 'Convert table inputs into SQL selection.';


-- DROP FUNCTION IF EXISTS styles._remove(xml, xml_tag text, xml_attribute text ) CASCADE;

CREATE OR REPLACE FUNCTION styles._remove(xml, xml_tag text, xml_attribute text DEFAULT NULL)
	RETURNS TABLE(
		_tag text[],
		_attribute text[],
		_xml xml
	)
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	_rec record;
	_end_tag text;
	_tg text[];
	_attrib text[];
	_style xml := $1;

BEGIN

	SELECT
		regexp_replace(xml_tag, '^.*/', '') -- Find the last //tag1/tag2
	INTO _end_tag;

--
-- Find all instances of the given tag
-- Into LOOP
--

	FOR _rec IN(
		SELECT
			a._tag,
			a._xml
		FROM styles._retrieve(_style, xml_tag, xml_attribute) a
	) LOOP

--
-- Add tag to array
--

		SELECT
			_tg || _rec._tag
		INTO _tg;

--
-- Add attributes to array
--

		SELECT
			_attrib || NULLIF(SUBSTRING(_rec._tag, '^<' || _end_tag || '\s(.*?)/?>'), '')
		INTO _attrib;

		SELECT
			replace(_style::text, _rec._xml, '<' || _end_tag || '/>')::xml
		INTO _style;

	END LOOP;

	RETURN QUERY
	SELECT
		_tg AS _tag,
		_attrib AS _attribute,
		_style;

END $BODY$;

COMMENT ON FUNCTION styles._remove(xml, xml_tag text, xml_attribute text ) IS 'Remove part of XML.';


-- DROP FUNCTION IF EXISTS styles.rem_variables(xml) CASCADE;

CREATE OR REPLACE FUNCTION styles.rem_variables(xml)
	RETURNS XML
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	_ret xml := $1;
BEGIN

	SELECT
		(styles._remove(
			(styles._remove(_ret, '//customproperties/property', 'key="variableNames"'))._xml, '//customproperties/property', 'key="variableValues"'
		))._xml
	INTO _ret;

	RETURN _ret;

END $BODY$;

COMMENT ON FUNCTION styles.rem_variables(xml) IS 'Remove existing variables of XML.';


-- DROP FUNCTION IF EXISTS styles.random_style() CASCADE;

CREATE OR REPLACE FUNCTION styles.random_style()
	RETURNS XML
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	_ret xml;

BEGIN

	SELECT
$$<qgis>
 <renderer-v2 enableorderby="0" symbollevels="0" forceraster="0">
  <symbols/>
  <rotation/>
  <sizescale/>
 </renderer-v2>
 <labeling>
  <settings/>
 </labeling>
 <customproperties/>
 <blendMode/>
 <featureBlendMode/>
 <layerOpacity>1</layerOpacity>
 <fieldConfiguration/>
 <aliases/>
 <excludeAttributesWMS/>
 <excludeAttributesWFS/>
 <defaults/>
 <constraints/>
 <constraintExpressions/>
 <attributeactions/>
 <attributetableconfig>
  <columns/>
 </attributetableconfig>
 <editform/>
 <editforminit/>
 <editforminitcodesource/>
 <editforminitfilepath/>
 <editforminitcode/>
 <featformsuppress/>
 <editorlayout/>
 <editable/>
 <labelOnTop/>
 <widgets/>
 <conditionalstyles>
  <rowstyles/>
  <fieldstyles/>
 </conditionalstyles>
 <expressionfields/>
 <previewExpression/>
 <mapTip/>
 <layerGeometryType/>
</qgis>$$::xml
	INTO _ret;

	RETURN _ret;

END $BODY$;

COMMENT ON FUNCTION styles.random_style() IS 'Generates cleared QGIS-style.';


-- DROP FUNCTION IF EXISTS styles.random_symbol(geom_type integer) CASCADE;

CREATE OR REPLACE FUNCTION styles.random_symbol(geom_type integer)
	RETURNS TEXT
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	_ret text;

BEGIN

--
-- POINT
--

	IF geom_type = 0 THEN

		SELECT
			FORMAT(
$$   <symbol alpha="1" clip_to_extent="1" name="0" type="marker">
    <layer pass="0" class="SimpleMarker" enabled="1" locked="0">
     <prop v="0" k="angle"/>
     <prop v="%1$s,255" k="color"/>
     <prop v="1" k="horizontal_anchor_point"/>
     <prop v="bevel" k="joinstyle"/>
     <prop v="circle" k="name"/>
     <prop v="0,0" k="offset"/>
     <prop v="3x:0,0,0,0,0,0" k="offset_map_unit_scale"/>
     <prop v="MM" k="offset_unit"/>
     <prop v="35,35,35,255" k="outline_color"/>
     <prop v="solid" k="outline_style"/>
     <prop v="0" k="outline_width"/>
     <prop v="3x:0,0,0,0,0,0" k="outline_width_map_unit_scale"/>
     <prop v="MM" k="outline_width_unit"/>
     <prop v="diameter" k="scale_method"/>
     <prop v="2" k="size"/>
     <prop v="3x:0,0,0,0,0,0" k="size_map_unit_scale"/>
     <prop v="MM" k="size_unit"/>
     <prop v="1" k="vertical_anchor_point"/>
     <data_defined_properties>
      <Option type="Map">
       <Option name="name" type="QString" value=""/>
       <Option name="properties"/>
       <Option name="type" type="QString" value="collection"/>
      </Option>
     </data_defined_properties>
    </layer>
   </symbol>$$, styles.random_rgb()
			)
		INTO _ret;

		RETURN _ret;

--
-- LINESTRING
--

	ELSIF geom_type = 1 THEN

		SELECT
			FORMAT(
$$   <symbol alpha="1" clip_to_extent="1" name="0" type="line">
    <layer pass="0" class="SimpleLine" enabled="1" locked="0">
     <prop v="square" k="capstyle"/>
     <prop v="5;2" k="customdash"/>
     <prop v="3x:0,0,0,0,0,0" k="customdash_map_unit_scale"/>
     <prop v="MM" k="customdash_unit"/>
     <prop v="0" k="draw_inside_polygon"/>
     <prop v="bevel" k="joinstyle"/>
     <prop v="%1$s,255" k="line_color"/>
     <prop v="solid" k="line_style"/>
     <prop v="0.26" k="line_width"/>
     <prop v="MM" k="line_width_unit"/>
     <prop v="0" k="offset"/>
     <prop v="3x:0,0,0,0,0,0" k="offset_map_unit_scale"/>
     <prop v="MM" k="offset_unit"/>
     <prop v="0" k="use_custom_dash"/>
     <prop v="3x:0,0,0,0,0,0" k="width_map_unit_scale"/>
     <data_defined_properties>
      <Option type="Map">
       <Option name="name" type="QString" value=""/>
       <Option name="properties"/>
       <Option name="type" type="QString" value="collection"/>
      </Option>
     </data_defined_properties>
    </layer>
   </symbol>$$, styles.random_rgb()
			)
		INTO _ret;

		RETURN _ret;

--
-- POLYGON
--

	ELSIF geom_type = 2 THEN

		SELECT
			FORMAT(
$$   <symbol alpha="1" clip_to_extent="1" name="0" type="fill">
    <layer pass="0" class="SimpleFill" enabled="1" locked="0">
     <prop v="3x:0,0,0,0,0,0" k="border_width_map_unit_scale"/>
     <prop v="%1$s,255" k="color"/>
     <prop v="bevel" k="joinstyle"/>
     <prop v="0,0" k="offset"/>
     <prop v="3x:0,0,0,0,0,0" k="offset_map_unit_scale"/>
     <prop v="MM" k="offset_unit"/>
     <prop v="35,35,35,255" k="outline_color"/>
     <prop v="solid" k="outline_style"/>
     <prop v="0.26" k="outline_width"/>
     <prop v="MM" k="outline_width_unit"/>
     <prop v="solid" k="style"/>
     <data_defined_properties>
      <Option type="Map">
       <Option name="name" type="QString" value=""/>
       <Option name="properties"/>
       <Option name="type" type="QString" value="collection"/>
      </Option>
     </data_defined_properties>
    </layer>
   </symbol>$$, styles.random_rgb()
			)
		INTO _ret;

		RETURN _ret;

	END IF;

	RETURN NULL;

END $BODY$;

COMMENT ON FUNCTION styles.random_symbol(geom_type integer) IS 'Generates symbols with random colours.';


-- DROP FUNCTION IF EXISTS styles.alias(xml, _col text[]) CASCADE;

CREATE OR REPLACE FUNCTION styles.alias(xml, _col text[])
	RETURNS XML
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	_ret xml := $1;
	_alias text;

BEGIN

	SELECT
		(styles._remove(_ret, '//aliases'))._xml
	INTO _ret;

	WITH

		cte1 AS(
			SELECT
				_order-1 AS _row,
				_val
			FROM UNNEST(_col) WITH ORDINALITY AS t(_val, _order)
		),

		cte2 AS(
			SELECT DISTINCT ON(a._row) -- If _val is present more than once in column_modif
				a._row,
				a._val,
				COALESCE(styles._entity_ref(b.alias), '') AS alias
			FROM cte1 a
			LEFT JOIN styles.column_modif b ON a._val = ANY(b.column_name)
		)

	SELECT
		E'<aliases>\n' || string_agg(
			FORMAT($$  <alias index="%1$s" name="%3$s" field="%2$s"/>$$, _row, _val, alias), E'\n' ORDER BY _row
		) || E'\n </aliases>'
	FROM cte2
	INTO _alias;

	SELECT
		regexp_replace(_ret::text, '<aliases/>', _alias)::xml
	INTO _ret;

	RETURN _ret;

END $BODY$;

COMMENT ON FUNCTION styles.alias(xml, _col text[]) IS '';


-- DROP FUNCTION IF EXISTS styles._default(xml, _col text[]) CASCADE;

CREATE OR REPLACE FUNCTION styles._default(xml, _col text[])
	RETURNS XML
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	_ret xml := $1;
	_default text;

BEGIN

	SELECT
		(styles._remove(_ret, '//defaults'))._xml
	INTO _ret;

	WITH

		cte1 AS(
			SELECT
				_order-1 AS _row,
				_val
			FROM UNNEST(_col) WITH ORDINALITY AS t(_val, _order)
		),

		cte2 AS(
			SELECT DISTINCT ON(a._row) -- If _val is present more than once in column_modif
				a._row,
				a._val,
				COALESCE(styles._entity_ref(b._default), '') AS _default,
				COALESCE(_default_apply_on_update::int, 0) AS _default_apply_on_update
			FROM cte1 a
			LEFT JOIN styles.column_modif b ON a._val = ANY(b.column_name)
		)

	SELECT
		E'<defaults>\n' || string_agg(
			FORMAT($$  <default field="%1$s" expression="%2$s" applyOnUpdate="%3$s"/>$$, a._val, a._default, a._default_apply_on_update), E'\n' ORDER BY _row
		) || E'\n </defaults>'
	FROM cte2 a
	INTO _default;

	SELECT
		regexp_replace(_ret::text, '<defaults/>', _default)::xml
	INTO _ret;

	RETURN _ret;

END $BODY$;

COMMENT ON FUNCTION styles._default(xml, _col text[]) IS '';


-- DROP FUNCTION IF EXISTS styles.renderer_v2(xml, _type integer, _attr text) CASCADE;

CREATE OR REPLACE FUNCTION styles.renderer_v2(xml, _type integer, _attr text DEFAULT '')
	RETURNS XML
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	_ret xml := $1;
	_val text;
	render_old text;
	render_new text;

BEGIN

--
-- Retrieve renderer-v2 tag
--

	SELECT
		(styles._retrieve(_ret, '//renderer-v2'))._xml
	INTO render_old;

	SELECT -- Modification
		render_old
	INTO render_new;

--
-- Prepare tag
--

	IF SUBSTRING(render_new, '</renderer-v2>') IS NULL THEN -- Self-closing tag

		IF render_new = '<renderer-v2/>' THEN -- Empty tag

			SELECT -- Retrieve renderer-v2 from random_style()
				(styles._retrieve(styles.random_style(), '//renderer-v2'))._xml
			INTO render_new;

		ELSE

			WITH

				cte1 AS(
					SELECT -- Retrieve renderer-v2 from random_style()
						(styles._retrieve(styles.random_style(), '//renderer-v2'))._xml AS _style
				),

				cte2 AS(
					SELECT -- "Open" self-closing tag
						regexp_replace(render_new, '/>', '>') AS _rep
				)

			SELECT -- Replace tag from random_style() with existing tag
				regexp_replace(a._style, '<renderer-v2.*?>', b._rep)
			FROM cte1 a, cte2 b
			INTO render_new;

		END IF;

	END IF;

--
-- Type of style
-- Categorized, rule-based
--

	WITH

		cte1(id, val) AS(
			VALUES
				(0, 'type="nullSymbol"'),
				(1, 'type="singleSymbol"'),
				(2, 'type="categorizedSymbol"'),
				(3, 'type="graduatedSymbol"'),
				(4, 'type="RuleRenderer"')
		)

	SELECT
		a.val
	FROM cte1 a
	WHERE a.id = _type
	INTO _val;

--
-- If renderer-v2 contains attribute: type
-- Simply replace that
--

	IF SUBSTRING(
		(styles._retrieve(render_new::xml, '//renderer-v2'))._tag, 'type='
	) IS NOT NULL THEN

		SELECT
			regexp_replace(render_new, 'type=".*?"', _val)
		INTO render_new;

--
-- Otherwise attach attribute: type
--

	ELSE

		SELECT
			regexp_replace(render_new, 'renderer-v2', 'renderer-v2 ' || _val)
		INTO render_new;

	END IF;

--
-- Attribute: attr
-- Same procedure as before
--

	IF _type IN(2, 3) THEN

		SELECT
			'attr="' || styles._entity_ref(_attr) || '"'
		INTO _attr;

		IF SUBSTRING(
			(styles._retrieve(render_new::xml, '//renderer-v2'))._tag, 'attr='
		) IS NOT NULL THEN

			SELECT
				regexp_replace(render_new, 'attr=".*?"', _attr)
			INTO render_new;

		ELSE

			SELECT
				regexp_replace(render_new, 'renderer-v2', 'renderer-v2 ' || _attr)
			INTO render_new;

		END IF;

	ELSE

		SELECT
			regexp_replace(render_new, 'attr=".*?"', '')
		INTO render_new;

	END IF;

--
-- Replace XML with new renderer-v2
--

	SELECT
		replace(_ret::text, render_old, render_new)::xml
	INTO _ret;

	RETURN _ret;

END $BODY$;

COMMENT ON FUNCTION styles.renderer_v2(xml, _type integer, _attr text) IS '';


-- DROP FUNCTION IF EXISTS styles.column_modif(xml, id integer) CASCADE;

CREATE OR REPLACE FUNCTION styles.column_modif(xml, id integer)
	RETURNS XML
	LANGUAGE plpgsql AS
$$

DECLARE

	_ret xml := $1;
	_col text[];

	_schema text;
	_table text;

BEGIN

--
-- Find columns
--

	SELECT
		a.schema_name,
		a.table_name
	FROM styles.inputs a
	WHERE a.id = $2
	INTO
		_schema,
		_table;

	SELECT
		array_agg(a.attname::text ORDER BY a.attnum)
	FROM pg_catalog.pg_attribute a
	LEFT JOIN pg_catalog.pg_class b ON a.attrelid = b.oid
	LEFT JOIN pg_catalog.pg_namespace c ON b.relnamespace = c.oid
	WHERE a.attnum > 0 AND a.attisdropped IS FALSE AND (c.nspname, b.relname) = (_schema, _table)
	INTO _col;

--
-- Alias
--

	SELECT
		styles.alias(_ret, _col)
	INTO _ret;

--
-- Default
--

	SELECT
		styles._default(_ret, _col)
	INTO _ret;

	RETURN _ret;

END $$;

COMMENT ON FUNCTION styles.column_modif(xml, id integer) IS 'Sets specific variables for auto-generated styles.';


-- DROP FUNCTION IF EXISTS styles.gen_symbology(xml, id integer, style_type text) CASCADE;

CREATE OR REPLACE FUNCTION styles.gen_symbology(xml, id integer, style_type text)
	RETURNS XML
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	_ret xml := $1;
	_symbols text;

	_schema text;
	_table text;
	_column text;
	_label text;
	_where text;
	_order text;

	_geom_type integer;

	_attr text;

	_cat text;
	_sym text;

	_atlas_wrap text;
	_text_col boolean;

BEGIN

	SELECT
		a.column_name,
		a.styling_schema_name,
		a.styling_table_name,
		a.styling_column_name,
		a.styling_label_exp,
		a.styling_filter_clause,
		a.styling_order_by,
		(styles.geometry_of(schema_name, table_name)).geom_type,
		styles._entity_ref(a.atlas_wrap_rule),
		text_column
	FROM styles.inputs a
	WHERE a.id = $2
	INTO
		_attr,
		_schema,
		_table,
		_column,
		_label,
		_where,
		_order,
		_geom_type,
		_atlas_wrap,
		_text_col;

--
-- TYPE: Category
--

	IF style_type = 'category' THEN

		SELECT
			(styles._remove(_ret, '//categories'))._xml
		INTO _ret;

		SELECT
			(styles._remove(_ret, '//symbols'))._xml
		INTO _ret;

--
-- Replace renderer to categorized
--

		SELECT
			styles.renderer_v2(_ret, 2, _attr)
		INTO _ret;

--
-- Generate style
--

		WITH

			cte1 AS(
				SELECT
					_row,
					a._ret AS _val,
					_lab
				FROM styles._select(_schema, _table, _column, _label, _where, _order) a
			),

			cte2 AS(
				SELECT
					_row,
					_val,
					_lab
				FROM cte1

				UNION ALL

				SELECT
					(SELECT MAX(_row)+1 FROM cte1) AS _row,
					NULL::text AS _val,
					'Ikke klassificeret' AS _lab
			),

			cte3 AS(
				SELECT
					a._row,
					styles._entity_ref(a._val) AS _val,
					styles._entity_ref(a._lab) AS _lab,
					styles.name_replace(COALESCE(b._xml, styles.random_symbol(_geom_type)), _row::text) AS _symbol
				FROM cte2 a
				LEFT JOIN styles.symbols b ON(_schema, _table, _column, a._val, _geom_type) = (b.styling_schema_name, b.styling_table_name, b.styling_column_name, b._value, b.geom_type)
			)

		SELECT
			E'<categories>\n' || string_agg(
				FORMAT(
					$$   <category render="true" label="%3$s" symbol="%1$s" value="%2$s"/>$$, _row, _val, _lab
				), E'\n' ORDER BY _row
			) || E'\n  </categories>' AS _cat,
			E'  <symbols>\n' || string_agg(
				_symbol, E'\n' ORDER BY _row::text
			) || E'\n  </symbols>' AS _sym
		FROM cte3
		INTO
			_cat,
			_sym;

		IF XMLEXISTS('//categories' PASSING BY REF _ret) THEN

			SELECT
				regexp_replace(_ret::text, '<categories/>', _cat)::xml
			INTO _ret;

		ELSE

			SELECT
				regexp_replace(_ret::text, '<symbols/>', _cat || E'\n  <symbols/>')::xml
			INTO _ret;

		END IF;

		SELECT
			regexp_replace(_ret::text, '<symbols/>', _sym)::xml
		INTO _ret;



	ELSIF style_type = 'atlas_wrap' THEN



		SELECT
			(styles._remove(_ret, '//categories'))._xml
		INTO _ret;

		SELECT
			(styles._remove(_ret, '//symbols'))._xml
		INTO _ret;

--
-- Replace renderer to rule-based
--

		SELECT
			styles.renderer_v2(_ret, 4)
		INTO _ret;

--
-- Generate style
--

		WITH

			cte1 AS(
				SELECT
					_row,
					a._ret AS _val,
					_lab
				FROM styles._select(_schema, _table, _column, _label, _where, _order) a
			),

			cte2 AS(
				SELECT
					a._row,
					a._val AS _val_sym,
					CASE
						WHEN _text_col IS TRUE
						THEN _attr || ' = ''' || a._val || ''''
						ELSE _attr || ' = ' || a._val
					END AS _val,
					a._lab
				FROM cte1 a

				UNION ALL

				SELECT
					(SELECT MAX(_row)+1 FROM cte1) AS _row,
					NULL AS _val_sym,
					'ELSE' AS _val,
					'Ikke klassificeret' AS _lab
			),

			cte3 AS(
				SELECT
					a._row,
					styles._entity_ref(a._val) AS _val,
					styles._entity_ref(a._lab) AS _lab,
					styles.name_replace(COALESCE(b._xml, styles.random_symbol(_geom_type)), _row::text) AS _symbol
				FROM cte2 a
				LEFT JOIN styles.symbols b ON(_schema, _table, _column, a._val_sym, _geom_type) = (b.styling_schema_name, b.styling_table_name, b.styling_column_name, b._value, b.geom_type)
			)

		SELECT
			FORMAT($$  <rules key="{%s}">$$						, public.uuid_generate_v1()) || E'\n' ||
			FORMAT($$   <rule key="{%1$s}" filter="%2$s">$$		, public.uuid_generate_v1(), _atlas_wrap) || E'\n' ||
			string_agg(
				FORMAT(
					$$    <rule label="%3$s" key="{%1$s}" filter="%2$s" symbol="%1$s"/>$$, _row, _val, _lab
				), E'\n' ORDER BY _row
			) ||
			E'\n   </rule>\n  </rules>' AS _cat,
			E'  <symbols>\n' || string_agg(
				_symbol, E'\n' ORDER BY _row::text
			) || E'\n  </symbols>' AS _sym
		FROM cte3
		INTO
			_cat,
			_sym;

		IF XMLEXISTS('//categories' PASSING BY REF _ret) THEN

			SELECT
				regexp_replace(_ret::text, '<categories/>', _cat)::xml
			INTO _ret;

		ELSE

			SELECT
				regexp_replace(_ret::text, '<symbols/>', _cat || E'\n  <symbols/>')::xml
			INTO _ret;

		END IF;

		SELECT
			regexp_replace(_ret::text, '<symbols/>', _sym)::xml
		INTO _ret;

	END IF;

	RETURN _ret;

END $BODY$;

COMMENT ON FUNCTION styles.gen_symbology(xml, id integer, style_type text) IS 'Generate symbology.';


-- DROP FUNCTION IF EXISTS styles.set_variables(xml, id integer) CASCADE;

CREATE OR REPLACE FUNCTION styles.set_variables(xml, id integer)
	RETURNS XML
	LANGUAGE plpgsql AS
$$

DECLARE

	_ret xml := $1;
	_prop_old text;
	_prop_new text;
	_val text;

BEGIN

	SELECT
		styles.rem_variables(_ret)
	INTO _ret;

	SELECT
		(styles._retrieve(_ret, '//customproperties'))._xml
	INTO _prop_old;

	SELECT
		_prop_old
	INTO _prop_new;

--
-- List and aggregate variables into XML
--

	WITH

		variables(_order, name, val) AS( -- Variables to apply
			VALUES
				(1, 'METADATA', '-- DO NOT ALTER! --'),
				(2, 'save symbols', 'Type SYMBOLS when saving the style to update symbology in the database.'),
				(3, 'save style', 'Type STYLE when saving the style to update the style corresponding with the given style_id.'),
				(4, 'style_id', $2::text)
		)

	SELECT
		E'  <property key="variableNames">\n' || string_agg('   <value>' || name || '</value>', E'\n' ORDER BY _order) || E'\n  </property>' || E'\n' ||
		E'  <property key="variableValues">\n' || string_agg('   <value>' || val || '</value>', E'\n' ORDER BY _order) || E'\n  </property>' AS val
	FROM variables
	INTO _val;

--
-- Manipulate style if no custom properties are present
--

	IF _prop_new = '<customproperties/>' THEN -- If no properties are present at all

		SELECT
			E'<customproperties>\n</customproperties>'
		INTO _prop_new;

	END IF;

--
-- Insert variables
--

	SELECT
		regexp_replace(_prop_new, '</customproperties>', _val || E'\n</customproperties>')
	INTO _prop_new;

--
-- Replace in style
--

	SELECT
		regexp_replace(_ret::text, _prop_old, _prop_new)::xml
	INTO _ret;

	RETURN _ret;

END $$;

COMMENT ON FUNCTION styles.set_variables(xml, id integer) IS 'Sets specific variables for auto-generated styles.';


-- DROP FUNCTION IF EXISTS styles.prep_style(id integer, style_type text) CASCADE;

CREATE OR REPLACE FUNCTION styles.prep_style(id integer, style_type text DEFAULT 'category')
	RETURNS XML
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	_ret xml;

BEGIN

--
-- Find style
--

	SELECT
		COALESCE(b.styleqml, a.custom_styleqml, styles.random_style()) -- Master > Custom > Random
	FROM styles.inputs a
	LEFT JOIN styles.master_templates b ON (a.schema_name, a.table_name) = (b.schema_name, b.table_name) AND a.custom_style IS FALSE -- If custom_style is true Master will be NULL
	WHERE a.id = $1
	INTO _ret;

--
-- Replace styling
--

	SELECT
		styles.gen_symbology(_ret, id, style_type)
	INTO _ret;

--
-- Set variables
--

	SELECT
		styles.set_variables(_ret, id)
	INTO _ret;

--
-- Column mofication
--
/*
	SELECT
		styles.column_modif(_ret, id)
	INTO _ret;*/

	RETURN _ret;

END $BODY$;

COMMENT ON FUNCTION styles.prep_style(id integer, style_type text) IS 'Generate style.';


--
-- TRIGGER FUNCTIONS
--


-- DROP FUNCTION IF EXISTS styles.auto_update() CASCADE;

CREATE OR REPLACE FUNCTION styles.auto_update()
	RETURNS trigger
	LANGUAGE plpgsql AS
$$

DECLARE

	schema_name text := TG_ARGV[0];
	table_name text := TG_ARGV[1];
	boolean_var text;

BEGIN

	IF TG_ARGV[0] IS NULL OR TG_ARGV[1] IS NULL THEN

		RAISE EXCEPTION 'Arguments for trigger function are missing!';

	END IF;

	IF (TG_OP = 'DELETE') THEN

		EXECUTE FORMAT(
			'%s', styles.auto_delete(schema_name, table_name)
		)
		USING OLD;

		RETURN NULL;

	ELSIF (TG_OP = 'UPDATE') THEN

		EXECUTE FORMAT(
			'%s', styles.auto_check_update(TG_TABLE_SCHEMA, TG_TABLE_NAME, schema_name, table_name)
		)
		USING NEW, OLD
		INTO boolean_var;

		IF boolean_var = '1' THEN

			RETURN NULL;

		END IF;

		EXECUTE FORMAT(
			'%s', styles.auto_update(TG_TABLE_SCHEMA, TG_TABLE_NAME, schema_name, table_name)
		)
		USING NEW, OLD;

		RETURN NULL;

	ELSIF (TG_OP = 'INSERT') THEN

		EXECUTE FORMAT(
			'%s', styles.auto_insert(TG_TABLE_SCHEMA, TG_TABLE_NAME, schema_name, table_name)
		)
		USING NEW;

		RETURN NULL;

	END IF;

END $$;


-- DROP FUNCTION IF EXISTS styles.layer_styles_trg_iu() CASCADE;

CREATE OR REPLACE FUNCTION styles.layer_styles_trg_iu()
	RETURNS trigger
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	_rec record;
	_input_id integer;
	_schema_name text;
	_table_name text;
	_column_name text;
	_geom_type integer;
	_style_setting boolean;

BEGIN

--
-- GET XML of style
--

	SELECT
		UNNEST(xpath('/qgis', styles._qgis(NEW.styleqml)))
	INTO NEW.styleqml;

--
-- KEYWORD: SYMBOLS
--

	IF NEW.stylename = 'SYMBOLS' THEN

		SELECT
			val
		FROM styles.gt_style_id(NEW.styleqml) AS t(val)
		INTO _input_id;

		IF EXISTS( -- The style corresponds with an input
			SELECT
				'1'
			FROM styles.inputs a
			WHERE a.id = _input_id
		) THEN

			SELECT
				a.styling_schema_name,
				a.styling_table_name,
				a.styling_column_name,
				(styles.geometry_of(a.schema_name, a.table_name)).geom_type
			FROM styles.inputs a
			WHERE a.id = _input_id
			INTO
				_schema_name,
				_table_name,
				_column_name,
				_geom_type;

			FOR _rec IN(
				SELECT
					*
				FROM styles.gt_symbols(NEW.styleqml, _schema_name, _table_name, _column_name)
			) LOOP

				IF EXISTS(
					SELECT
						'1'
					FROM styles.symbols a
					WHERE (a.styling_schema_name, a.styling_table_name, a.styling_column_name, a.geom_type, a._value) = (_schema_name, _table_name, _column_name, _geom_type, _rec._column)
				) THEN

					UPDATE styles.symbols a
						SET
							_xml = _rec._xml
					WHERE (a.styling_schema_name, a.styling_table_name, a.styling_column_name, a.geom_type, a._value) = (_schema_name, _table_name, _column_name, _geom_type, _rec._column);

				ELSE

					INSERT INTO styles.symbols(
						styling_schema_name,
						styling_table_name,
						styling_column_name,
						geom_type,
						_value,
						_xml
					) VALUES (
						_schema_name,
						_table_name,
						_column_name,
						_geom_type,
						_rec._column,
						_rec._xml
					);

				END IF;

			END LOOP;

		ELSE

			RAISE EXCEPTION 'style_id NOT FOUND'; -- Will raise error in QGIS, but won't actually show the text. Instead something with permission to user will be shown

		END IF;

		RETURN NULL;

--
-- KEYWORD: STYLE
--

	ELSIF NEW.stylename = 'STYLE' THEN

		SELECT
			val
		FROM styles.gt_style_id(NEW.styleqml) AS t(val)
		INTO _input_id;

		IF EXISTS( -- The style corresponds with an input
			SELECT
				'1'
			FROM styles.inputs a
			WHERE a.id = _input_id
		) THEN

			SELECT
				a.schema_name,
				a.table_name,
				a.custom_style
			FROM styles.inputs a
			WHERE a.id = _input_id
			INTO
				_schema_name,
				_table_name,
				_style_setting;

			IF _style_setting IS FALSE THEN -- Style used by several inputs

				IF EXISTS(
					SELECT
						'1'
					FROM styles.master_templates a
					WHERE (a.schema_name, a.table_name) = (_schema_name, _table_name)
				) THEN

					UPDATE styles.master_templates a
						SET
							styleqml = NEW.styleqml
					WHERE (a.schema_name, a.table_name) = (_schema_name, _table_name);

				ELSE

					INSERT INTO styles.master_templates(
						schema_name,
						table_name,
						styleqml
					) VALUES(
						_schema_name,
						_table_name,
						NEW.styleqml
					);

				END IF;

			END IF;

			UPDATE styles.inputs a
				SET
					custom_styleqml = NEW.styleqml
			WHERE a.id = _input_id;

		ELSE

			RAISE EXCEPTION 'style_id NOT FOUND'; -- Will raise error in QGIS, but won't actually show the text. Instead something with permission to user will be shown

		END IF;

		RETURN NULL;

--
-- REGULAR STYLE
--

	ELSE

--
-- Remove variables
--

		SELECT
			styles.rem_variables(NEW.styleqml)
		INTO NEW.styleqml;

		RETURN NEW;

	END IF;

END $BODY$;


--
-- TABLES
--


-- DROP TABLE IF EXISTS styles.inputs CASCADE;

CREATE TABLE styles.inputs(
	id serial NOT NULL,
	stylename character varying(30) NOT NULL,
	schema_name character varying(30) NOT NULL,
	table_name character varying(30) NOT NULL,
	column_name text NOT NULL,
	ref_schema_name character varying(30),
	ref_table_name character varying(30),
	styling_schema_name character varying(30) NOT NULL,
	styling_table_name character varying(30) NOT NULL,
	styling_column_name character varying(30) NOT NULL,
	styling_label_exp text,
	styling_filter_clause text,
	styling_order_by text,
	text_column boolean DEFAULT TRUE NOT NULL,
	custom_style boolean DEFAULT FALSE NOT NULL,
	custom_styleqml xml,
	active boolean DEFAULT TRUE NOT NULL,
	-- Alternative styles --
	op_styles text[],
	atlas_wrap_rule text,
	CONSTRAINT inputs_pk PRIMARY KEY (id) WITH (fillfactor='10'),
	CONSTRAINT inputs_ck_ref CHECK(
		(ref_schema_name IS NULL AND ref_table_name IS NULL) OR
		(ref_schema_name IS NOT NULL AND ref_table_name IS NOT NULL)
	)
);

COMMENT ON TABLE styles.inputs IS 'Inputs for automatic styling.';
COMMENT ON COLUMN styles.inputs.stylename IS 'Title of the style that is to be shown in menu.';
COMMENT ON COLUMN styles.inputs.ref_schema_name IS 'OPTIONAL: Reference schema to find information in case table is a view.';
COMMENT ON COLUMN styles.inputs.ref_table_name IS 'OPTIONAL: Reference table to find information in case table is a view.';
COMMENT ON COLUMN styles.inputs.styling_schema_name IS 'Values in styling is based on this schema.';
COMMENT ON COLUMN styles.inputs.styling_table_name IS 'Values in styling is based on this table.';
COMMENT ON COLUMN styles.inputs.styling_column_name IS 'Values in styling is based on this column.';
COMMENT ON COLUMN styles.inputs.styling_label_exp IS 'SQL to be shown (Legend).';
COMMENT ON COLUMN styles.inputs.styling_filter_clause IS 'SQL to filter values to be used.';
COMMENT ON COLUMN styles.inputs.styling_order_by IS 'SQL to order values to be used.';
COMMENT ON COLUMN styles.inputs.custom_style IS 'The style should only be applied to this input only - Instead of using a style across several inputs using the same table.';
COMMENT ON COLUMN styles.inputs.op_styles IS 'Additional (special) styles to be added in the future can be selected here.';


-- layer_styles

CREATE TABLE styles.layer_styles (
	id serial NOT NULL,
	f_table_catalog character varying,
	f_table_schema character varying,
	f_table_name character varying,
	f_geometry_column character varying,
	stylename character varying(30),
	styleqml xml,
	stylesld text,
	useasdefault boolean,
	description text,
	owner character varying(30),
	ui text,
	update_time timestamp with time zone,
	CONSTRAINT layer_styles_pk PRIMARY KEY (id) WITH (fillfactor='10')
);

COMMENT ON TABLE styles.layer_styles IS 'Storage of styles generated and saved in QGIS.';

CREATE TRIGGER layer_styles_trg_iu BEFORE INSERT OR UPDATE ON styles.layer_styles FOR EACH ROW EXECUTE PROCEDURE styles.layer_styles_trg_iu();


-- DROP TABLE IF EXISTS styles.column_modif CASCADE;

CREATE TABLE styles.column_modif(
	id serial NOT NULL,
	column_name text[] DEFAULT '{}',
	alias text,
	_default text,
	_default_apply_on_update boolean DEFAULT FALSE,
	CONSTRAINT column_modif_pk PRIMARY KEY (id) WITH (fillfactor='10')
);

COMMENT ON TABLE styles.column_modif IS 'Column modification.';



-- master_templates

CREATE TABLE styles.master_templates(
	schema_name character varying(30) NOT NULL,
	table_name character varying(30) NOT NULL,
	styleqml xml NOT NULL,
	CONSTRAINT master_templates_pk PRIMARY KEY (schema_name, table_name) WITH (fillfactor='10')
);

COMMENT ON TABLE styles.master_templates IS 'Master templates, used by all inputs of the same table.';


-- DROP TABLE styles.optional_styles CASCADE;

CREATE TABLE styles.optional_styles(
	id serial NOT NULL,
	stylename character varying(30) NOT NULL,
	label character varying(150) NOT NULL,
	extension character varying NOT NULL,
	CONSTRAINT optional_styles_pk PRIMARY KEY (id) WITH (fillfactor='10')
);

COMMENT ON TABLE styles.optional_styles IS 'Input values for easy management of additional styles.';

INSERT INTO styles.optional_styles(stylename, label, extension) VALUES
	('atlas_wrap', 'Kategorisering under regel, eks atlas', 'Rule/Category');


-- DROP TABLE IF EXISTS styles.symbols CASCADE;

CREATE TABLE styles.symbols(
	id serial NOT NULL,
	styling_schema_name character varying(30) NOT NULL,
	styling_table_name character varying(30) NOT NULL,
	styling_column_name character varying(30) NOT NULL,
	geom_type integer NOT NULL,
	_value text NOT NULL,
	_xml text NOT NULL,
	CONSTRAINT symbols_pk PRIMARY KEY (id) WITH (fillfactor='10'),
	CONSTRAINT symbols_ck_geom_type CHECK (geom_type BETWEEN 0 AND 2)
);

COMMENT ON TABLE styles.symbols IS 'Storage of symbology for all values.';
COMMENT ON COLUMN styles.symbols.styling_schema_name IS 'Symbols for values in specified schema.';
COMMENT ON COLUMN styles.symbols.styling_table_name IS 'Symbols for values in specified table.';
COMMENT ON COLUMN styles.symbols.styling_column_name IS 'Symbols for values in specified column.';
COMMENT ON COLUMN styles.symbols.geom_type IS 'Geometry type of style.';
COMMENT ON COLUMN styles.symbols._value IS 'The specified value.';


--
-- VIEWS
--


-- DROP VIEW IF EXISTS public.layer_styles CASCADE;

DO $$

BEGIN

	IF EXISTS (SELECT '1' FROM information_schema.tables a WHERE a.table_schema = 'public' AND a.table_name = 'layer_styles' AND a.table_type = 'BASE TABLE') THEN

		INSERT INTO styles.layer_styles(
			f_table_catalog,
			f_table_schema,
			f_table_name,
			f_geometry_column,
			stylename,
			styleqml,
			stylesld,
			useasdefault,
			description,
			owner,
			ui,
			update_time
		) SELECT
				f_table_catalog,
				f_table_schema,
				f_table_name,
				f_geometry_column,
				stylename,
				styleqml,
				stylesld,
				useasdefault,
				description,
				owner,
				ui,
				update_time
			FROM public.layer_styles;

		DROP TABLE public.layer_style;

		RAISE NOTICE 'public.layer_styles has been DROPPED.';

	ELSIF EXISTS(SELECT '1' FROM information_schema.tables a WHERE a.table_schema = 'public' AND a.table_name = 'layer_styles' AND a.table_type = 'VIEW') THEN

		DROP VIEW public.layer_styles;

	END IF;

END $$;

CREATE VIEW public.layer_styles AS

SELECT
	id,
	f_table_catalog,
	f_table_schema,
	f_table_name,
	f_geometry_column,
	stylename,
	styleqml,
	stylesld,
	useasdefault,
	description,
	owner,
	ui,
	update_time
FROM styles.layer_styles
WHERE styleqml IS NOT NULL

UNION ALL

SELECT
	a.id + (SELECT COALESCE(MAX(id), 0) FROM styles.layer_styles)::int AS id,
	current_database() AS f_table_catalog,
	a.schema_name AS f_table_schema,
	a.table_name AS f_table_name,
	(styles.geometry_of(a.schema_name, a.table_name)).geom_column AS f_geometry_column,
	stylename,
	styles.prep_style(a.id) AS styleqml,
	NULL::text AS stylesld,
	FALSE AS useasdefault,
	E'This style is generated automatically in PostgreSQL.\nTo save the individual symbols, simply type SYMBOLS and save\nTo save the style (symbols will be updated as new values are added), simply type STYLE' AS description,
	'postgres' AS owner,
	NULL::text AS ui,
	current_timestamp AS update_time
FROM styles.inputs a
WHERE active IS TRUE AND styles.check_tbl(a.styling_schema_name, a.styling_table_name) IS TRUE

UNION ALL

(WITH
	
	cte1 AS(
		SELECT
			id,
			a.stylename,
			a.schema_name,
			a.table_name,
			a.column_name,
			a.styling_schema_name,
			a.styling_table_name,
			a.active,
			UNNEST(a.op_styles::text[]) AS op_style
		FROM styles.inputs a
		WHERE a.op_styles != '{}'
	)

SELECT
	((SELECT COALESCE(MAX(id), 0) FROM styles.layer_styles) + (SELECT COALESCE(MAX(id), 0) FROM styles.inputs) + ROW_NUMBER() OVER())::int AS id,
	current_database() AS f_table_catalog,
	a.schema_name AS f_table_schema,
	a.table_name AS f_table_name,
	(styles.geometry_of(a.schema_name, a.table_name)).geom_column AS f_geometry_column,
	a.stylename || ' - ' || b.extension AS stylename,
	styles.prep_style(a.id, a.op_style) AS styleqml,
	NULL::text AS stylesld,
	FALSE AS useasdefault,
	E'Optional style' AS description,
	'postgres' AS owner,
	NULL::text AS ui,
	current_timestamp AS update_time
FROM cte1 a
LEFT JOIN styles.optional_styles b ON a.op_style = b.stylename
WHERE a.active IS TRUE AND styles.check_tbl(a.styling_schema_name, a.styling_table_name) IS TRUE);

CREATE TRIGGER layer_styles_trg_iud INSTEAD OF INSERT OR DELETE OR UPDATE ON public.layer_styles FOR EACH ROW EXECUTE PROCEDURE styles.auto_update('styles', 'layer_styles');


-- DROP VIEW IF EXISTS styles.v_inputs CASCADE;

CREATE VIEW styles.v_inputs AS

SELECT
	*,
	NULL::public.geometry('Point', 25832) AS geom
FROM styles.inputs;

CREATE TRIGGER v_inputs_trg_iud INSTEAD OF INSERT OR DELETE OR UPDATE ON styles.v_inputs FOR EACH ROW EXECUTE PROCEDURE styles.auto_update('styles', 'inputs');


--
-- INDEXES
--


--
-- TRIGGERS
--


--
-- INSERTS
--


-- layer_styles

INSERT INTO styles.layer_styles(
	f_table_catalog,
	f_table_schema,
	f_table_name,
	f_geometry_column,
	stylename,
	useasdefault,
	owner,
	styleqml
) VALUES (
	current_database(),
	'styles',
	'v_inputs',
	'geom',
	'Input til stilarter',
	FALSE,
	'postgres',
	$$<qgis version="3.2.3-Bonn" minScale="1e+8" readOnly="0" simplifyDrawingHints="0" maxScale="0" simplifyMaxScale="1" hasScaleBasedVisibilityFlag="0" labelsEnabled="0" simplifyDrawingTol="1" simplifyAlgorithm="0" simplifyLocal="1">
 <renderer-v2 type="nullSymbol"/>
 <customproperties>
  <property key=""/>
  <property key="dualview/previewExpressions">
   <value> "id"  || ' ' ||  "stylename" </value>
   <value>id</value>
  </property>
  <property key="embeddedWidgets/count" value="0"/>
  <property key="variableNames"/>
  <property key="variableValues"/>
 </customproperties>
 <blendMode>0</blendMode>
 <featureBlendMode>0</featureBlendMode>
 <layerOpacity>1</layerOpacity>
 <SingleCategoryDiagramRenderer attributeLegend="1" diagramType="Histogram">
  <DiagramCategory penWidth="0" opacity="1" diagramOrientation="Up" width="15" lineSizeScale="3x:0,0,0,0,0,0" penAlpha="255" height="15" rotationOffset="270" penColor="#000000" maxScaleDenominator="1e+8" lineSizeType="MM" minimumSize="0" backgroundAlpha="255" backgroundColor="#ffffff" minScaleDenominator="0" sizeScale="3x:0,0,0,0,0,0" scaleDependency="Area" sizeType="MM" enabled="0" barWidth="5" labelPlacementMethod="XHeight" scaleBasedVisibility="0">
   <fontProperties style="" description="MS Shell Dlg 2,8.25,-1,5,50,0,0,0,0,0"/>
   <attribute color="#000000" field="" label=""/>
  </DiagramCategory>
 </SingleCategoryDiagramRenderer>
 <DiagramLayerSettings priority="0" zIndex="0" placement="0" obstacle="0" dist="0" showAll="1" linePlacementFlags="18">
  <properties>
   <Option type="Map">
    <Option name="name" type="QString" value=""/>
    <Option name="properties"/>
    <Option name="type" type="QString" value="collection"/>
   </Option>
  </properties>
 </DiagramLayerSettings>
 <fieldConfiguration>
  <field name="id">
   <editWidget type="Hidden">
    <config>
     <Option/>
    </config>
   </editWidget>
  </field>
  <field name="stylename">
   <editWidget type="TextEdit">
    <config>
     <Option type="Map">
      <Option name="IsMultiline" type="bool" value="false"/>
      <Option name="UseHtml" type="bool" value="false"/>
     </Option>
    </config>
   </editWidget>
  </field>
  <field name="schema_name">
   <editWidget type="TextEdit">
    <config>
     <Option type="Map">
      <Option name="IsMultiline" type="bool" value="false"/>
      <Option name="UseHtml" type="bool" value="false"/>
     </Option>
    </config>
   </editWidget>
  </field>
  <field name="table_name">
   <editWidget type="TextEdit">
    <config>
     <Option type="Map">
      <Option name="IsMultiline" type="bool" value="false"/>
      <Option name="UseHtml" type="bool" value="false"/>
     </Option>
    </config>
   </editWidget>
  </field>
  <field name="column_name">
   <editWidget type="TextEdit">
    <config>
     <Option type="Map">
      <Option name="IsMultiline" type="bool" value="false"/>
      <Option name="UseHtml" type="bool" value="false"/>
     </Option>
    </config>
   </editWidget>
  </field>
  <field name="ref_schema_name">
   <editWidget type="TextEdit">
    <config>
     <Option type="Map">
      <Option name="IsMultiline" type="bool" value="false"/>
      <Option name="UseHtml" type="bool" value="false"/>
     </Option>
    </config>
   </editWidget>
  </field>
  <field name="ref_table_name">
   <editWidget type="TextEdit">
    <config>
     <Option type="Map">
      <Option name="IsMultiline" type="bool" value="false"/>
      <Option name="UseHtml" type="bool" value="false"/>
     </Option>
    </config>
   </editWidget>
  </field>
  <field name="styling_schema_name">
   <editWidget type="TextEdit">
    <config>
     <Option type="Map">
      <Option name="IsMultiline" type="bool" value="false"/>
      <Option name="UseHtml" type="bool" value="false"/>
     </Option>
    </config>
   </editWidget>
  </field>
  <field name="styling_table_name">
   <editWidget type="TextEdit">
    <config>
     <Option type="Map">
      <Option name="IsMultiline" type="bool" value="false"/>
      <Option name="UseHtml" type="bool" value="false"/>
     </Option>
    </config>
   </editWidget>
  </field>
  <field name="styling_column_name">
   <editWidget type="TextEdit">
    <config>
     <Option type="Map">
      <Option name="IsMultiline" type="bool" value="false"/>
      <Option name="UseHtml" type="bool" value="false"/>
     </Option>
    </config>
   </editWidget>
  </field>
  <field name="styling_label_exp">
   <editWidget type="TextEdit">
    <config>
     <Option type="Map">
      <Option name="IsMultiline" type="bool" value="true"/>
      <Option name="UseHtml" type="bool" value="false"/>
     </Option>
    </config>
   </editWidget>
  </field>
  <field name="styling_filter_clause">
   <editWidget type="TextEdit">
    <config>
     <Option type="Map">
      <Option name="IsMultiline" type="bool" value="true"/>
      <Option name="UseHtml" type="bool" value="false"/>
     </Option>
    </config>
   </editWidget>
  </field>
  <field name="styling_order_by">
   <editWidget type="TextEdit">
    <config>
     <Option type="Map">
      <Option name="IsMultiline" type="bool" value="true"/>
      <Option name="UseHtml" type="bool" value="false"/>
     </Option>
    </config>
   </editWidget>
  </field>
  <field name="text_column">
   <editWidget type="CheckBox">
    <config>
     <Option type="Map">
      <Option name="CheckedState" type="QString" value=""/>
      <Option name="UncheckedState" type="QString" value=""/>
     </Option>
    </config>
   </editWidget>
  </field>
  <field name="custom_style">
   <editWidget type="CheckBox">
    <config>
     <Option type="Map">
      <Option name="CheckedState" type="QString" value=""/>
      <Option name="UncheckedState" type="QString" value=""/>
     </Option>
    </config>
   </editWidget>
  </field>
  <field name="custom_styleqml">
   <editWidget type="Hidden">
    <config>
     <Option/>
    </config>
   </editWidget>
  </field>
  <field name="active">
   <editWidget type="CheckBox">
    <config>
     <Option type="Map">
      <Option name="CheckedState" type="QString" value=""/>
      <Option name="UncheckedState" type="QString" value=""/>
     </Option>
    </config>
   </editWidget>
  </field>
  <field name="op_styles">
   <editWidget type="List">
    <config>
     <Option/>
    </config>
   </editWidget>
  </field>
  <field name="atlas_wrap_rule">
   <editWidget type="TextEdit">
    <config>
     <Option type="Map">
      <Option name="IsMultiline" type="bool" value="false"/>
      <Option name="UseHtml" type="bool" value="false"/>
     </Option>
    </config>
   </editWidget>
  </field>
 </fieldConfiguration>
 <aliases>
  <alias index="0" name="" field="id"/>
  <alias index="1" name="Navn" field="stylename"/>
  <alias index="2" name="Skema" field="schema_name"/>
  <alias index="3" name="Tabel" field="table_name"/>
  <alias index="4" name="Kolonne/Expression (til kategorisering)" field="column_name"/>
  <alias index="5" name="Skema (Reference)" field="ref_schema_name"/>
  <alias index="6" name="Tabel (Reference)" field="ref_table_name"/>
  <alias index="7" name="Skema" field="styling_schema_name"/>
  <alias index="8" name="Tabel" field="styling_table_name"/>
  <alias index="9" name="Kolonne (Værdi-)" field="styling_column_name"/>
  <alias index="10" name="Signaturforkalring/Legend (SQL)" field="styling_label_exp"/>
  <alias index="11" name="Filtrering (SQL)" field="styling_filter_clause"/>
  <alias index="12" name="Sortering (SQL)" field="styling_order_by"/>
  <alias index="13" name="Tekst?" field="text_column"/>
  <alias index="14" name="Stilarten skal kun af dette input?" field="custom_style"/>
  <alias index="15" name="" field="custom_styleqml"/>
  <alias index="16" name="Aktiv" field="active"/>
  <alias index="17" name="Alternative stilarter (Evt. opsætning af Value Relation til styles.optional_styles)" field="op_styles"/>
  <alias index="18" name="Overordnet regel" field="atlas_wrap_rule"/>
 </aliases>
 <excludeAttributesWMS/>
 <excludeAttributesWFS/>
 <defaults>
  <default field="id" expression="" applyOnUpdate="0"/>
  <default field="stylename" expression="" applyOnUpdate="0"/>
  <default field="schema_name" expression="" applyOnUpdate="0"/>
  <default field="table_name" expression="" applyOnUpdate="0"/>
  <default field="column_name" expression="" applyOnUpdate="0"/>
  <default field="ref_schema_name" expression="" applyOnUpdate="0"/>
  <default field="ref_table_name" expression="" applyOnUpdate="0"/>
  <default field="styling_schema_name" expression="" applyOnUpdate="0"/>
  <default field="styling_table_name" expression="" applyOnUpdate="0"/>
  <default field="styling_column_name" expression="" applyOnUpdate="0"/>
  <default field="styling_label_exp" expression="" applyOnUpdate="0"/>
  <default field="styling_filter_clause" expression="" applyOnUpdate="0"/>
  <default field="styling_order_by" expression="" applyOnUpdate="0"/>
  <default field="text_column" expression="TRUE" applyOnUpdate="0"/>
  <default field="custom_style" expression="" applyOnUpdate="0"/>
  <default field="custom_styleqml" expression="" applyOnUpdate="0"/>
  <default field="active" expression="TRUE" applyOnUpdate="0"/>
  <default field="op_styles" expression="" applyOnUpdate="0"/>
  <default field="atlas_wrap_rule" expression="" applyOnUpdate="0"/>
 </defaults>
 <constraints>
  <constraint unique_strength="1" notnull_strength="1" field="id" exp_strength="0" constraints="3"/>
  <constraint unique_strength="0" notnull_strength="0" field="stylename" exp_strength="0" constraints="0"/>
  <constraint unique_strength="0" notnull_strength="0" field="schema_name" exp_strength="0" constraints="0"/>
  <constraint unique_strength="0" notnull_strength="0" field="table_name" exp_strength="0" constraints="0"/>
  <constraint unique_strength="0" notnull_strength="0" field="column_name" exp_strength="0" constraints="0"/>
  <constraint unique_strength="0" notnull_strength="0" field="ref_schema_name" exp_strength="0" constraints="0"/>
  <constraint unique_strength="0" notnull_strength="0" field="ref_table_name" exp_strength="0" constraints="0"/>
  <constraint unique_strength="0" notnull_strength="0" field="styling_schema_name" exp_strength="0" constraints="0"/>
  <constraint unique_strength="0" notnull_strength="0" field="styling_table_name" exp_strength="0" constraints="0"/>
  <constraint unique_strength="0" notnull_strength="0" field="styling_column_name" exp_strength="0" constraints="0"/>
  <constraint unique_strength="0" notnull_strength="0" field="styling_label_exp" exp_strength="0" constraints="0"/>
  <constraint unique_strength="0" notnull_strength="0" field="styling_filter_clause" exp_strength="0" constraints="0"/>
  <constraint unique_strength="0" notnull_strength="0" field="styling_order_by" exp_strength="0" constraints="0"/>
  <constraint unique_strength="0" notnull_strength="0" field="text_column" exp_strength="0" constraints="0"/>
  <constraint unique_strength="0" notnull_strength="0" field="custom_style" exp_strength="0" constraints="0"/>
  <constraint unique_strength="0" notnull_strength="0" field="custom_styleqml" exp_strength="0" constraints="0"/>
  <constraint unique_strength="0" notnull_strength="0" field="active" exp_strength="0" constraints="0"/>
  <constraint unique_strength="0" notnull_strength="0" field="op_styles" exp_strength="0" constraints="0"/>
  <constraint unique_strength="0" notnull_strength="0" field="atlas_wrap_rule" exp_strength="0" constraints="0"/>
 </constraints>
 <constraintExpressions>
  <constraint exp="" field="id" desc=""/>
  <constraint exp="" field="stylename" desc=""/>
  <constraint exp="" field="schema_name" desc=""/>
  <constraint exp="" field="table_name" desc=""/>
  <constraint exp="" field="column_name" desc=""/>
  <constraint exp="" field="ref_schema_name" desc=""/>
  <constraint exp="" field="ref_table_name" desc=""/>
  <constraint exp="" field="styling_schema_name" desc=""/>
  <constraint exp="" field="styling_table_name" desc=""/>
  <constraint exp="" field="styling_column_name" desc=""/>
  <constraint exp="" field="styling_label_exp" desc=""/>
  <constraint exp="" field="styling_filter_clause" desc=""/>
  <constraint exp="" field="styling_order_by" desc=""/>
  <constraint exp="" field="text_column" desc=""/>
  <constraint exp="" field="custom_style" desc=""/>
  <constraint exp="" field="custom_styleqml" desc=""/>
  <constraint exp="" field="active" desc=""/>
  <constraint exp="" field="op_styles" desc=""/>
  <constraint exp="" field="atlas_wrap_rule" desc=""/>
 </constraintExpressions>
 <attributeactions>
  <defaultAction key="Canvas" value="{00000000-0000-0000-0000-000000000000}"/>
 </attributeactions>
 <attributetableconfig actionWidgetStyle="dropDown" sortExpression="" sortOrder="0">
  <columns>
   <column name="id" hidden="0" type="field" width="-1"/>
   <column name="stylename" hidden="0" type="field" width="-1"/>
   <column name="schema_name" hidden="0" type="field" width="-1"/>
   <column name="table_name" hidden="0" type="field" width="-1"/>
   <column name="column_name" hidden="0" type="field" width="-1"/>
   <column name="ref_schema_name" hidden="0" type="field" width="-1"/>
   <column name="ref_table_name" hidden="0" type="field" width="-1"/>
   <column name="styling_schema_name" hidden="0" type="field" width="-1"/>
   <column name="styling_table_name" hidden="0" type="field" width="-1"/>
   <column name="styling_column_name" hidden="0" type="field" width="-1"/>
   <column name="styling_label_exp" hidden="0" type="field" width="-1"/>
   <column name="styling_filter_clause" hidden="0" type="field" width="-1"/>
   <column name="styling_order_by" hidden="0" type="field" width="-1"/>
   <column name="custom_style" hidden="0" type="field" width="-1"/>
   <column name="custom_styleqml" hidden="0" type="field" width="-1"/>
   <column name="op_styles" hidden="0" type="field" width="-1"/>
   <column name="atlas_wrap_rule" hidden="0" type="field" width="-1"/>
   <column hidden="1" type="actions" width="-1"/>
   <column name="text_column" hidden="0" type="field" width="-1"/>
   <column name="active" hidden="0" type="field" width="-1"/>
  </columns>
 </attributetableconfig>
 <editform tolerant="1"></editform>
 <editforminit/>
 <editforminitcodesource>0</editforminitcodesource>
 <editforminitfilepath></editforminitfilepath>
 <editforminitcode><![CDATA[# -*- coding: utf-8 -*-
"""
QGIS forms can have a Python function that is called when the form is
opened.

Use this function to add extra logic to your forms.

Enter the name of the function in the "Python Init function"
field.
An example follows:
"""
from qgis.PyQt.QtWidgets import QWidget

def my_form_open(dialog, layer, feature):
	geom = feature.geometry()
	control = dialog.findChild(QWidget, "MyLineEdit")
]]></editforminitcode>
 <featformsuppress>0</featformsuppress>
 <editorlayout>tablayout</editorlayout>
 <attributeEditorForm>
  <attributeEditorContainer groupBox="1" visibilityExpression="" name="" visibilityExpressionEnabled="0" columnCount="1" showLabel="1">
   <attributeEditorContainer groupBox="1" visibilityExpression="" name="Stilart" visibilityExpressionEnabled="0" columnCount="3" showLabel="1">
    <attributeEditorField index="1" name="stylename" showLabel="1"/>
    <attributeEditorField index="14" name="custom_style" showLabel="1"/>
    <attributeEditorField index="16" name="active" showLabel="1"/>
   </attributeEditorContainer>
   <attributeEditorContainer groupBox="1" visibilityExpression="" name="Stilarten tilhører følgende tabel:" visibilityExpressionEnabled="0" columnCount="3" showLabel="1">
    <attributeEditorField index="2" name="schema_name" showLabel="1"/>
    <attributeEditorField index="3" name="table_name" showLabel="1"/>
    <attributeEditorField index="4" name="column_name" showLabel="1"/>
   </attributeEditorContainer>
   <attributeEditorContainer groupBox="1" visibilityExpression="" name="Stilarten skal kategoriseres med værdier fra følgende tabel:" visibilityExpressionEnabled="0" columnCount="1" showLabel="1">
    <attributeEditorContainer groupBox="1" visibilityExpression="" name="" visibilityExpressionEnabled="0" columnCount="4" showLabel="1">
     <attributeEditorField index="7" name="styling_schema_name" showLabel="1"/>
     <attributeEditorField index="8" name="styling_table_name" showLabel="1"/>
     <attributeEditorField index="9" name="styling_column_name" showLabel="1"/>
     <attributeEditorField index="13" name="text_column" showLabel="1"/>
    </attributeEditorContainer>
    <attributeEditorField index="10" name="styling_label_exp" showLabel="1"/>
    <attributeEditorField index="11" name="styling_filter_clause" showLabel="1"/>
    <attributeEditorField index="12" name="styling_order_by" showLabel="1"/>
   </attributeEditorContainer>
   <attributeEditorContainer groupBox="1" visibilityExpression="" name="" visibilityExpressionEnabled="0" columnCount="1" showLabel="1">
    <attributeEditorField index="17" name="op_styles" showLabel="1"/>
    <attributeEditorField index="18" name="atlas_wrap_rule" showLabel="1"/>
   </attributeEditorContainer>
  </attributeEditorContainer>
 </attributeEditorForm>
 <editable>
  <field name="active" editable="1"/>
  <field name="atlas_wrap_rule" editable="1"/>
  <field name="column_name" editable="1"/>
  <field name="custom_style" editable="1"/>
  <field name="custom_styleqml" editable="1"/>
  <field name="id" editable="1"/>
  <field name="op_styles" editable="1"/>
  <field name="ref_schema_name" editable="1"/>
  <field name="ref_table_name" editable="1"/>
  <field name="schema_name" editable="1"/>
  <field name="stylename" editable="1"/>
  <field name="styling_column_name" editable="1"/>
  <field name="styling_filter_clause" editable="1"/>
  <field name="styling_label_exp" editable="1"/>
  <field name="styling_order_by" editable="1"/>
  <field name="styling_schema_name" editable="1"/>
  <field name="styling_table_name" editable="1"/>
  <field name="table_name" editable="1"/>
  <field name="text_column" editable="1"/>
 </editable>
 <labelOnTop>
  <field labelOnTop="1" name="active"/>
  <field labelOnTop="1" name="atlas_wrap_rule"/>
  <field labelOnTop="1" name="column_name"/>
  <field labelOnTop="1" name="custom_style"/>
  <field labelOnTop="1" name="custom_styleqml"/>
  <field labelOnTop="0" name="id"/>
  <field labelOnTop="1" name="op_styles"/>
  <field labelOnTop="1" name="ref_schema_name"/>
  <field labelOnTop="1" name="ref_table_name"/>
  <field labelOnTop="1" name="schema_name"/>
  <field labelOnTop="1" name="stylename"/>
  <field labelOnTop="1" name="styling_column_name"/>
  <field labelOnTop="1" name="styling_filter_clause"/>
  <field labelOnTop="1" name="styling_label_exp"/>
  <field labelOnTop="1" name="styling_order_by"/>
  <field labelOnTop="1" name="styling_schema_name"/>
  <field labelOnTop="1" name="styling_table_name"/>
  <field labelOnTop="1" name="table_name"/>
  <field labelOnTop="1" name="text_column"/>
 </labelOnTop>
 <widgets/>
 <conditionalstyles>
  <rowstyles/>
  <fieldstyles/>
 </conditionalstyles>
 <expressionfields/>
 <previewExpression> "id"  || ' ' ||  "stylename" </previewExpression>
 <mapTip></mapTip>
 <layerGeometryType>0</layerGeometryType>
</qgis>
$$
);








--
-- DOMAINS
--

DROP DOMAIN IF EXISTS custom.telefon CASCADE;
CREATE DOMAIN custom.telefon numeric(8,0)
	CONSTRAINT length_8 CHECK (LENGTH(VALUE::text) = 8);

DROP DOMAIN IF EXISTS custom.email CASCADE;
CREATE DOMAIN custom.email character varying
	CONSTRAINT at_sign_dot_somthing CHECK (VALUE ~* '.+@.+\..+');

DROP DOMAIN IF EXISTS custom.pris CASCADE;
CREATE DOMAIN custom.pris numeric(10,2)
	DEFAULT 0.00
	NOT NULL
	CONSTRAINT zero_or_more CHECK (VALUE >= 0.00);

DROP DOMAIN IF EXISTS custom.dato CASCADE;
CREATE DOMAIN custom.dato date
	CONSTRAINT val_1800_2500 CHECK (VALUE BETWEEN '1800-01-01' AND '2500-12-31');

DROP DOMAIN IF EXISTS custom.maal CASCADE;
CREATE DOMAIN custom.maal numeric(10,1)
	CONSTRAINT zero_or_more CHECK (VALUE >= 0.0);

/*
DROP DOMAIN IF EXISTS custom. CASCADE;
CREATE DOMAIN custom. ;*/


--
-- SCHEMA grunddata
--

CREATE SCHEMA grunddata;
COMMENT ON SCHEMA grunddata IS 'Skema indeholdende praktiske grunddata uden direkte relation til de resterende data.';


--
-- TABLES
--


DO $$

DECLARE

	i record;
	geom_type text;

BEGIN

	FOR i IN (
		SELECT
			*
		FROM (
			VALUES
				('bygning', 2, NULL),
				('bygraense', 2, NULL),
				('kommune', 2, NULL),
				('kyst', 1, NULL),
--				('label', 2, NULL),
				('postdistrikt', 2, 'postnr integer'),
				('skov', 2, NULL),
				('soe', 2, NULL),
				('veje', 1, 'vejkode integer'),
				('vejkant', 1, NULL)
		) AS i(name, _type, _add)
	) LOOP

		SELECT
			CASE
				WHEN i._type = 0
				THEN 'Point'
				WHEN i._type = 1
				THEN 'LineString'
				WHEN i._type = 2
				THEN 'Polygon'
			END
		INTO geom_type;

		EXECUTE FORMAT(
			$qt$
				 DROP TABLE IF EXISTS grunddata.%1$I CASCADE;

				CREATE TABLE grunddata.%1$I (
					id serial NOT NULL,
					%3$s
					geom public.geometry('Multi%2$s', 25832),
					CONSTRAINT %1$s_pkey PRIMARY KEY (id) WITH (fillfactor='10')
				);

				CREATE INDEX %1$s_gist_geom ON grunddata.%1$I USING gist(geom);
			$qt$, i.name, geom_type, COALESCE(i._add || ',', '')
		);

	END LOOP;

END $$;


--
-- VIEWS
--


DO $$

DECLARE

	i record;
	geom_type text;

BEGIN

	FOR i IN (
		SELECT
			*
		FROM (
			VALUES
				('bygning', 2, NULL),
				('bygraense', 2, NULL),
				('kommune', 2, NULL),
				('kyst', 1, NULL),
--				('label', 2, NULL),
				('postdistrikt', 2, 'postnr integer'),
				('skov', 2, NULL),
				('soe', 2, NULL),
				('veje', 1, 'vejkode integer'),
				('vejkant', 1, NULL)
		) AS i(name, _type, _add)
	) LOOP

		SELECT
			CASE
				WHEN i._type = 0
				THEN 'Point'
				WHEN i._type = 1
				THEN 'LineString'
				WHEN i._type = 2
				THEN 'Polygon'
			END
		INTO geom_type;

		EXECUTE FORMAT(
			$qt$
				 DROP VIEW IF EXISTS grunddata.v_%1$s CASCADE;

				CREATE VIEW grunddata.v_%1$s AS

				SELECT
					*
				FROM grunddata.%1$I;
			$qt$, i.name
		);

	END LOOP;

END $$;


-- DROP VIEW IF EXISTS grunddata.v_label
/*
CREATE VIEW grunddata.v_label AS

SELECT
	a.id,
	a.geom,
	(SELECT filter.label()) || (SELECT COALESCE(E'\n' || repeat('_', LENGTH('Login: ' || _value)) || E'\n' || 'Login: ' || _value, '') FROM filter._value('rolname')) AS label
FROM grunddata.label a;
*/


--
-- INSERTS
--

--
-- grunddata.label
--
/*
INSERT INTO grunddata.label(geom)
	SELECT
		ST_Multi('0103000020E8640000010000000500000000000000A0EA1A41000000003A1357410000000090402B41000000003A1357410000000090402B4100000000EE6C584100000000A0EA1A4100000000EE6C584100000000A0EA1A41000000003A135741'::geometry('Polygon', 25832));
*/


--
-- SCHEMA basis
--

CREATE SCHEMA basis;
COMMENT ON SCHEMA basis IS 'Skema indeholdende opslagstabeller.';


--
-- FUNCTIONS
--


-- DROP FUNCTION IF EXISTS basis.element(anyarray) CASCADE;

CREATE OR REPLACE FUNCTION basis.element(anyarray)
	RETURNS text
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	_col text;
	_ret text;

BEGIN

	WITH

--
-- Each element from the array as a row
--

		cte1 AS(
			SELECT
				val,
				num
			FROM UNNEST($1) WITH ORDINALITY t(val, num)
		)

--
-- Aggregate rows
--

	SELECT
		string_agg(val, basis.separator() ORDER BY num)
	FROM cte1
	WHERE val IS NOT NULL
	INTO _ret;

	RETURN _ret;

END $BODY$;

COMMENT ON FUNCTION basis.element(anyarray) IS 'Konverter en ARRAY af en elementkode til en egentlig elementkode.';


-- DROP FUNCTION IF EXISTS basis.element(VARIADIC element_kode text[]) CASCADE;

CREATE OR REPLACE FUNCTION basis.element(VARIADIC element_kode text[])
	RETURNS text
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	_col text;
	_ret text;

BEGIN

	WITH

--
-- Each element from the array as a row
--

		cte1 AS(
			SELECT
				val,
				num
			FROM UNNEST($1) WITH ORDINALITY t(val, num)
		)

--
-- Aggregate rows
--

	SELECT
		string_agg(val, basis.separator() ORDER BY num)
	FROM cte1
	WHERE val IS NOT NULL
	INTO _ret;

	RETURN _ret;

END $BODY$;

COMMENT ON FUNCTION basis.element(VARIADIC element_kode text[]) IS 'Generer elementkode ved at angive alle værdier (NULL inklusive).';


-- DROP FUNCTION IF EXISTS basis.element_arr(anyarray) CASCADE;

CREATE OR REPLACE FUNCTION basis.element_arr(anyarray)
	RETURNS text[]
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	_col text;
	_ret text;

BEGIN

	WITH

--
-- Each element from the array as a row, covnertet 
--

		cte1 AS(
			SELECT
				basis.element(val::text[]) AS val,
				num
			FROM UNNEST($1) WITH ORDINALITY t(val, num)
		)

--
-- Aggregate rows
--

	SELECT
		array_agg(val ORDER BY num)
	FROM cte1
	INTO _ret;

	RETURN COALESCE(_ret, '{}');

END $BODY$;

COMMENT ON FUNCTION basis.element_arr(anyarray) IS 'Konverter en ARRAY af array-elementkoder til en egentlig elementkode.';


-- DROP FUNCTION IF EXISTS basis.prisregulering(dato date) CASCADE;

CREATE OR REPLACE FUNCTION basis.prisregulering(dato date)
	RETURNS FLOAT
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	_ret FLOAT;

BEGIN

--
-- Multiply values from basis.prisregulering until and included a given day
--

	SELECT
		custom.multiply(1 + a.aendring_pct / 100)
	FROM basis.prisregulering a
	WHERE a.dato <= $1
	INTO _ret;

	RETURN _ret;

END $BODY$;

COMMENT ON FUNCTION basis.prisregulering(dato date) IS 'Prisregulering frem til en given dato.';


-- DROP FUNCTION IF EXISTS basis.separator() CASCADE;

CREATE OR REPLACE FUNCTION basis.separator()
	RETURNS text
	LANGUAGE plpgsql AS
$BODY$

BEGIN

	RETURN '-';

END $BODY$;

COMMENT ON FUNCTION basis.separator() IS 'Separator mellem elementniveauer.';


-- DROP FUNCTION IF EXISTS basis.telefon(text) CASCADE;

CREATE OR REPLACE FUNCTION basis.telefon(text)
	RETURNS text
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	_ret text;

BEGIN

--
-- Format 12345678 => 1234 5678
--

	SELECT
		FORMAT('%s %s', LEFT($1::text, 4), RIGHT($1::text, 4))
	INTO _ret;

	RETURN _ret;

END $BODY$;

COMMENT ON FUNCTION basis.telefon(text) IS 'Formatering af telefonnummer.';


--
-- TRIGGER FUNCTIONS
--


-- DROP FUNCTION IF EXISTS basis.afdeling_nr_trg() CASCADE;

CREATE OR REPLACE FUNCTION basis.afdeling_nr_trg()
	RETURNS trigger
	LANGUAGE plpgsql AS
$BODY$

BEGIN

	IF NEW.afdeling_nr != OLD.afdeling_nr THEN

		UPDATE basis.omraader a
			SET
				afdeling_nr = NEW.afdeling_nr
		WHERE a.afdeling_nr = OLD.afdeling_nr;
	END IF;

	RETURN NULL;

END $BODY$;


-- DROP FUNCTION IF EXISTS basis.element_kode_trg_iu() CASCADE;

CREATE OR REPLACE FUNCTION basis.element_kode_trg_iu()
	RETURNS trigger
	LANGUAGE plpgsql AS
$$

BEGIN

	NEW.element_kode = COALESCE((SELECT a.element_kode FROM basis.v_elementer a WHERE a.element_kode_def = NEW.element_kode_tdl), '{}'::text[]) || NEW.element_kode_niv;

	RETURN NEW;

END $$;

COMMENT ON FUNCTION basis.element_kode_trg_iu() IS 'Generér element_kode-array ved INSERT/UPDATE.';


-- DROP FUNCTION IF EXISTS basis.maengder_quote_trg() CASCADE;

CREATE OR REPLACE FUNCTION basis.maengder_quote_trg()
	RETURNS trigger
	LANGUAGE plpgsql AS
$BODY$

BEGIN

	SELECT
		COALESCE(array_agg(b.element_kode::text ORDER BY b.element_kode), '{}'::text[])
	FROM UNNEST(NEW.element_kode_def) t(val)
	LEFT JOIN basis.v_elementer b ON t.val = b.element_kode_def
	INTO NEW.element_kode;

	RETURN NEW;

END $BODY$;


-- DROP FUNCTION IF EXISTS basis.pg_distrikt_nr_trg() CASCADE;

CREATE OR REPLACE FUNCTION basis.pg_distrikt_nr_trg()
	RETURNS trigger
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	boolean_var text;

BEGIN

	IF NEW.pg_distrikt_nr != OLD.pg_distrikt_nr THEN

--
-- Disable geometry-updates
--

		IF filter._value('omr_red')::boolean THEN

			boolean_var = '1';

			UPDATE filter.settings
				SET
					omr_red = FALSE
			WHERE rolname = current_user;

		END IF;

--
-- Update tables
--

		UPDATE greg.flader a
			SET
				pg_distrikt_nr = NEW.pg_distrikt_nr
		WHERE a.systid_til IS NULL AND a.pg_distrikt_nr = OLD.pg_distrikt_nr;

		UPDATE greg.linier a
			SET
				pg_distrikt_nr = NEW.pg_distrikt_nr
		WHERE a.systid_til IS NULL AND a.pg_distrikt_nr = OLD.pg_distrikt_nr;

		UPDATE greg.punkter a
			SET
				pg_distrikt_nr = NEW.pg_distrikt_nr
		WHERE a.systid_til IS NULL AND a.pg_distrikt_nr = OLD.pg_distrikt_nr;

		UPDATE basis.delomraader a
			SET
				pg_distrikt_nr = NEW.pg_distrikt_nr
		WHERE a.pg_distrikt_nr = OLD.pg_distrikt_nr;

--
-- If geometry-updates were on, enable
--

		IF boolean_var THEN

			UPDATE filter.settings
				SET
					omr_red = TRUE
			WHERE rolname = current_user;

		END IF;

	END IF;

	RETURN NULL;

END $BODY$;


--
-- TABLES
--


-- DROP TABLE IF EXISTS basis.ansvarlig_myndighed CASCADE;

CREATE TABLE basis.ansvarlig_myndighed (
	cvr_kode integer NOT NULL,
	cvr_navn character varying(128) NOT NULL,
	kommunekode integer,
	aktiv boolean DEFAULT TRUE NOT NULL,
	CONSTRAINT ansvarlig_myndighed_pk PRIMARY KEY (cvr_kode) WITH (fillfactor='10')
);

COMMENT ON TABLE basis.ansvarlig_myndighed IS 'Opslagstabel, ansvarlig myndighed for elementet (FKG).';


-- DROP TABLE IF EXISTS basis.offentlig CASCADE;

CREATE TABLE basis.offentlig (
	off_kode integer NOT NULL,
	offentlig character varying(60) NOT NULL,
	aktiv boolean DEFAULT TRUE NOT NULL,
	CONSTRAINT offentlig_pk PRIMARY KEY (off_kode) WITH (fillfactor='10')
);

COMMENT ON TABLE basis.offentlig IS 'Opslagstabel, offentlighedsstatus (FKG).';


-- DROP TABLE IF EXISTS basis.oprindelse CASCADE;

CREATE TABLE basis.oprindelse (
	oprindkode integer NOT NULL,
	oprindelse character varying(35) NOT NULL,
	aktiv boolean DEFAULT TRUE NOT NULL,
	begrebsdefinition character varying,
	CONSTRAINT oprindelse_pk PRIMARY KEY (oprindkode) WITH (fillfactor='10')
);

COMMENT ON TABLE basis.oprindelse IS 'Opslagstabel, oprindelse (FKG).';


-- DROP TABLE IF EXISTS basis.status CASCADE;

CREATE TABLE basis.status (
	statuskode integer NOT NULL,
	status character varying(30) NOT NULL,
	aktiv boolean DEFAULT TRUE NOT NULL,
	CONSTRAINT status_pk PRIMARY KEY (statuskode) WITH (fillfactor='10')
);

COMMENT ON TABLE basis.status IS 'Opslagstabel, gyldighedsstatus (FKG).';


-- DROP TABLE IF EXISTS basis.driftniv CASCADE;

CREATE TABLE basis.driftniv (
	driftniv_kode integer NOT NULL,
	driftniv character varying(10) NOT NULL,
	aktiv boolean NOT NULL DEFAULT TRUE,
	begrebsdefinition character varying,
	enhedspris_f custom.pris,
	enhedspris_l custom.pris,
	enhedspris_p custom.pris,
	CONSTRAINT driftniv_pk PRIMARY KEY (driftniv_kode) WITH (fillfactor='10')
);

COMMENT ON TABLE basis.driftniv IS 'Opslagstabel, driftsniveau for elementet mht. renhold (FKG).';


-- DROP TABLE IF EXISTS basis.postnr CASCADE;

CREATE TABLE basis.postnr (
	postnr integer NOT NULL,
	postnr_by character varying(128) NOT NULL,
	aktiv boolean DEFAULT TRUE NOT NULL,
	CONSTRAINT postnr_pk		PRIMARY KEY (postnr) WITH (fillfactor='10'),
	CONSTRAINT postnr_ck_postnr	CHECK (LENGTH(postnr::text) = 4)
);

COMMENT ON TABLE basis.postnr IS 'Opslagstabel, postdistrikter (FKG).';


-- DROP TABLE IF EXISTS basis.tilstand CASCADE;

CREATE TABLE basis.tilstand (
	tilstand_kode integer NOT NULL,
	tilstand character varying(25) NOT NULL,
	aktiv boolean DEFAULT TRUE NOT NULL,
	begrebsdefinition character varying,
	CONSTRAINT tilstand_pk PRIMARY KEY (tilstand_kode) WITH (fillfactor='10')
);

COMMENT ON TABLE basis.tilstand IS 'Opslagstabel, tilstand (FKG).';


-- DROP TABLE IF EXISTS basis.ukrudtsbek CASCADE;

CREATE TABLE basis.ukrudtsbek (
	ukrudtsbek_kode integer NOT NULL,
	ukrudtsbek character varying(20) NOT NULL,
	aktiv BOOLEAN NOT NULL DEFAULT TRUE,
	begrebsdefinition character varying,
	CONSTRAINT ukrudtsbek_pk PRIMARY KEY (ukrudtsbek_kode) WITH (fillfactor='10')
);

COMMENT ON TABLE basis.ukrudtsbek IS 'Opslagstabel, ukrudtsbekæmpelsesmetode, tilladt/afvigelse fra norm (FKG).';


-- DROP TABLE IF EXISTS basis.vejnavn CASCADE;

CREATE TABLE basis.vejnavn (
	vejkode integer NOT NULL,
	vejnavn character varying(40) NOT NULL,
	aktiv boolean DEFAULT TRUE NOT NULL,
	cvf_vejkode character varying(7),
	postnr integer,
	kommunekode integer,
	CONSTRAINT vejnavn_pk			PRIMARY KEY (vejkode) WITH (fillfactor='10'),
	CONSTRAINT vejnavn_fk_postnr	FOREIGN KEY (postnr) REFERENCES basis.postnr(postnr) MATCH FULL
);

COMMENT ON TABLE basis.vejnavn IS 'Opslagstabel, vejnavne (FKG).';


-- DROP TABLE IF EXISTS basis.klip_sider CASCADE;

CREATE TABLE basis.klip_sider (
	klip_sider_kode integer NOT NULL,
	klip_sider character varying(40) NOT NULL,
	aktiv boolean DEFAULT TRUE NOT NULL,
	CONSTRAINT klip_sider_pk							PRIMARY KEY (klip_sider_kode) WITH (fillfactor='10'),
	CONSTRAINT klip_sider_ck_klip_sider_kode_min_1_to_2	CHECK (klip_sider_kode BETWEEN -1 AND 2)
);

COMMENT ON TABLE basis.klip_sider IS 'Antal klippesider.';


-- DROP TABLE IF EXISTS basis.kommunal_kontakt CASCADE;

CREATE TABLE basis.kommunal_kontakt (
	kommunal_kontakt_kode serial NOT NULL,
	navn character varying(100) NOT NULL,
	telefon custom.telefon NOT NULL,
	email custom.email NOT NULL,
	aktiv boolean DEFAULT TRUE NOT NULL,
	CONSTRAINT kommunal_kontakt_pk PRIMARY KEY (kommunal_kontakt_kode) WITH (fillfactor='10')
);

COMMENT ON TABLE basis.kommunal_kontakt IS 'Opslagstabel, kommunal kontakt for element / område (FKG).';


-- DROP TABLE IF EXISTS basis.udfoerer CASCADE;

CREATE TABLE basis.udfoerer (
	udfoerer_kode serial NOT NULL,
	udfoerer character varying(50) NOT NULL,
	aktiv boolean DEFAULT TRUE NOT NULL,
	CONSTRAINT udfoerer_pk PRIMARY KEY (udfoerer_kode) WITH (fillfactor='10')
);

COMMENT ON TABLE basis.udfoerer IS 'Opslagstabel, ansvarlig udførende for entrepriseområde (FKG).';


-- DROP TABLE IF EXISTS basis.udfoerer_entrep CASCADE;

CREATE TABLE basis.udfoerer_entrep (
	udfoerer_entrep_kode serial NOT NULL,
	udfoerer_entrep character varying(50) NOT NULL,
	aktiv boolean DEFAULT TRUE NOT NULL,
	CONSTRAINT udfoerer_entrep_pk PRIMARY KEY (udfoerer_entrep_kode) WITH (fillfactor='10')
);

COMMENT ON TABLE basis.udfoerer_entrep IS 'Opslagstabel, ansvarlig udførerende entreprenør for element (FKG).';


-- DROP TABLE IF EXISTS basis.udfoerer_kontakt CASCADE;

CREATE TABLE basis.udfoerer_kontakt (
	udfoerer_kode integer NOT NULL,
	udfoerer_kontakt_kode serial NOT NULL,
	navn character varying(100) NOT NULL,
	telefon custom.telefon NOT NULL,
	email custom.email NOT NULL,
	aktiv boolean DEFAULT TRUE NOT NULL,
	CONSTRAINT udfoerer_kontakt_pk			PRIMARY KEY (udfoerer_kontakt_kode) WITH (fillfactor='10'),
	CONSTRAINT udfoerer_kontakt_fk_udfoerer	FOREIGN KEY (udfoerer_kode) REFERENCES basis.udfoerer(udfoerer_kode) MATCH FULL
);

COMMENT ON TABLE basis.udfoerer_kontakt IS 'Opslagstabel, kontaktinformationer på ansvarlig udførende.';


-- DROP TABLE IF EXISTS basis.distrikt_type CASCADE;

CREATE TABLE basis.distrikt_type (
	pg_distrikt_type_kode serial NOT NULL,
	pg_distrikt_type character varying(30) NOT NULL,
	aktiv boolean DEFAULT TRUE NOT NULL,
	CONSTRAINT distrikt_type_pk PRIMARY KEY (pg_distrikt_type_kode) WITH (fillfactor='10')
);

COMMENT ON TABLE basis.distrikt_type IS 'Opslagstabel, områdetyper. Fx grønne områder, skoler mv. (FKG).';


-- DROP TABLE IF EXISTS basis.elementer CASCADE;

CREATE TABLE basis.elementer (
	element_kode text[] NOT NULL,
	elementnavn character varying(50) NOT NULL,
	objekt_type integer[] DEFAULT '{}'::int[] NOT NULL,
	tbl boolean DEFAULT TRUE NOT NULL,
	enhedspris custom.pris,
	aktiv boolean DEFAULT TRUE NOT NULL,
	CONSTRAINT elementer_pk				PRIMARY KEY (element_kode) WITH (fillfactor='10'),
	CONSTRAINT elementer_ck_element_kode CHECK (cardinality(element_kode) > 0),
	CONSTRAINT elementer_ck_objekt_type	CHECK (objekt_type <@ ARRAY[0,1,2])
);

COMMENT ON TABLE basis.elementer IS 'Opslagstabel, elementtyper, som beskriver driften.';

CREATE TRIGGER hierarchy_elementer_iud BEFORE INSERT OR UPDATE OR DELETE
	ON basis.elementer
	FOR EACH ROW
	EXECUTE PROCEDURE custom.hierarchy('element_kode');


-- DROP TABLE IF EXISTS basis.objekt_typer CASCADE;

CREATE TABLE basis.objekt_typer(
	dimension integer NOT NULL,
	beskrivelse character varying(10) NOT NULL,
	mgd_besk character varying(20) NOT NULL,
	enhed character varying(10) NOT NULL,
	mgd_sql text NOT NULL,
	_decimal integer NOT NULL,
	CONSTRAINT objekt_typer_pk					PRIMARY KEY (dimension) WITH (fillfactor='10'),
	CONSTRAINT objekt_typer_ck_dimension_0_to_2	CHECK (dimension BETWEEN 0 AND 2)
);

COMMENT ON TABLE basis.objekt_typer IS 'Opslagstabel til QGIS vedr. objekttyper for elementer.';


-- DROP TABLE IF EXISTS basis.prisregulering CASCADE;

CREATE TABLE basis.prisregulering (
	dato date NOT NULL,
	aendring_pct numeric(10,2),
	CONSTRAINT prisregulering_pk PRIMARY KEY (dato) WITH (fillfactor='10')
);

COMMENT ON TABLE basis.prisregulering IS 'Løbende regulering af grundpriser.';


-- DROP TABLE IF EXISTS basis.afdelinger CASCADE;

CREATE TABLE basis.afdelinger(
	geometri public.geometry('MultiPolygon', 25832) NOT NULL,
	afdeling_nr serial NOT NULL,
	afdeling_tekst character varying(150) NOT NULL,
	CONSTRAINT afdelinger_pk			PRIMARY KEY (afdeling_nr) WITH (fillfactor='10'),
	CONSTRAINT afdelinger_ck_geometri	CHECK (public.ST_IsValid(geometri) IS TRUE AND public.ST_IsEmpty(geometri) IS FALSE)
);

COMMENT ON TABLE basis.afdelinger IS 'Højere geografisk inddeling, afdelinger.';

CREATE TRIGGER c_geom_check_iu
	BEFORE INSERT OR UPDATE ON basis.afdelinger
	FOR EACH ROW
	EXECUTE PROCEDURE custom.geom_check_passive('TRUE', '(SELECT geometri_tjek_2 FROM filter.v_settings) AND NOT (SELECT geometri_aggresive FROM filter.v_settings)');

CREATE TRIGGER c_geom_check_aggr_iu
	BEFORE INSERT ON basis.afdelinger
	FOR EACH ROW
	EXECUTE PROCEDURE custom.geom_check_aggressive('TRUE', '(SELECT geometri_tjek_2 FROM filter.v_settings) AND (SELECT geometri_aggresive FROM filter.v_settings)');

CREATE TRIGGER a_afdeling_nr_u
	AFTER UPDATE ON basis.afdelinger
	FOR EACH ROW
	WHEN (NEW.afdeling_nr != OLD.afdeling_nr)
	EXECUTE PROCEDURE basis.afdeling_nr_trg();


CREATE INDEX afdelinger_gist ON basis.afdelinger USING gist (geometri);


-- DROP TABLE IF EXISTS basis.maengder CASCADE;

CREATE TABLE basis.maengder(
	id serial NOT NULL,
	element_kode text[] NOT NULL,
	beskrivelse text NOT NULL,
	alt_enhed text,
	objekt_type integer[] DEFAULT '{}'::int[] NOT NULL,
	enhedspris custom.pris,
	aktiv boolean DEFAULT TRUE NOT NULL,
	maengde_sql text,
	source_schema text,
	source_table text,
	source_column text,
	source_label text,
	source_column_pris text,
	source_where_clause text,
	target_column text,
	CONSTRAINT maengder_pk				PRIMARY KEY (id) WITH (fillfactor='10'),
	CONSTRAINT maengder_ck_objekt_type	CHECK (objekt_type <@ ARRAY[0,1,2]),
	CONSTRAINT maengder_ck_sql			CHECK (maengde_sql IS NOT NULL OR (source_schema IS NOT NULL AND source_table IS NOT NULL AND source_column IS NOT NULL))
);


-- DROP TABLE IF EXISTS basis.omraader CASCADE;

CREATE TABLE basis.omraader(
	geometri public.geometry('MultiPolygon', 25832) NOT NULL,
	pg_distrikt_nr integer NOT NULL,
	pg_distrikt_tekst character varying(150) NOT NULL,
	afdeling_nr integer,
	pg_distrikt_type_kode integer NOT NULL,
	vejkode integer,
	vejnr character varying(20),
	postnr integer NOT NULL,
	kommunal_kontakt_kode integer,
	udfoerer_kode integer,
	udfoerer_kontakt_kode1 integer,
	udfoerer_kontakt_kode2 integer,
	note character varying(254),
	link character varying(1024),
	aktiv boolean DEFAULT TRUE NOT NULL,
	synlig boolean DEFAULT TRUE NOT NULL,
	-- User-defined

	-- Constraints
	CONSTRAINT omraader_pk						PRIMARY KEY (pg_distrikt_nr) WITH (fillfactor='10'),
	CONSTRAINT omraader_fk_distrikt_type		FOREIGN KEY (pg_distrikt_type_kode) REFERENCES basis.distrikt_type(pg_distrikt_type_kode) MATCH FULL,
	CONSTRAINT omraader_fk_vejnavn				FOREIGN KEY (vejkode) REFERENCES basis.vejnavn(vejkode) MATCH FULL,
	CONSTRAINT omraader_fk_postnr				FOREIGN KEY (postnr) REFERENCES basis.postnr(postnr) MATCH FULL,
	CONSTRAINT omraader_fk_kommunal_kontakt		FOREIGN KEY (kommunal_kontakt_kode) REFERENCES basis.kommunal_kontakt(kommunal_kontakt_kode) MATCH FULL,
	CONSTRAINT omraader_fk_udfoerer				FOREIGN KEY (udfoerer_kode) REFERENCES basis.udfoerer(udfoerer_kode) MATCH FULL,
	CONSTRAINT omraader_fk_udfoerer_kontakt1	FOREIGN KEY (udfoerer_kontakt_kode1) REFERENCES basis.udfoerer_kontakt(udfoerer_kontakt_kode) MATCH FULL,
	CONSTRAINT omraader_fk_udfoerer_kontakt2	FOREIGN KEY (udfoerer_kontakt_kode2) REFERENCES basis.udfoerer_kontakt(udfoerer_kontakt_kode) MATCH FULL,
	CONSTRAINT omraader_ck_geometri				CHECK (public.ST_IsValid(geometri) IS TRUE)
);

COMMENT ON TABLE basis.omraader IS 'Specifik geografisk inddeling, områder.';

CREATE TRIGGER b_geom_check_i
	BEFORE INSERT ON basis.omraader
	FOR EACH ROW
	EXECUTE PROCEDURE custom.geom_check_passive('TRUE', '(SELECT geometri_tjek_2 FROM filter.v_settings) AND NOT (SELECT geometri_aggresive FROM filter.v_settings)');

CREATE TRIGGER b_geom_check_aggr_i
	BEFORE INSERT ON basis.omraader
	FOR EACH ROW
	EXECUTE PROCEDURE custom.geom_check_aggressive('TRUE', '(SELECT geometri_tjek_2 FROM filter.v_settings) AND (SELECT geometri_aggresive FROM filter.v_settings)');

CREATE TRIGGER a_pg_distrikt_nr_u
	AFTER UPDATE ON basis.omraader
	FOR EACH ROW
	WHEN (NEW.pg_distrikt_nr != OLD.pg_distrikt_nr)
	EXECUTE PROCEDURE basis.pg_distrikt_nr_trg();

CREATE INDEX omraader_gist ON basis.omraader USING gist(geometri);


-- DROP TABLE IF EXISTS basis.delomraader CASCADE;

CREATE TABLE basis.delomraader(
	geometri public.geometry('MultiPolygon', 25832) NOT NULL,
	id serial NOT NULL,
	pg_distrikt_nr integer NOT NULL,
	beskrivelse character varying(150) NOT NULL,
	CONSTRAINT delomraader_pk PRIMARY KEY (id) WITH (fillfactor='10'),
	CONSTRAINT delomraader_ck_geometri CHECK (public.ST_IsValid(geometri) IS TRUE AND public.ST_IsEmpty(geometri) IS FALSE)
);

COMMENT ON TABLE basis.delomraader IS 'Fokusområder i atlas.';

CREATE INDEX delomraader_gist ON basis.delomraader USING gist(geometri);


--
-- VIEWS
--


-- DROP VIEW IF EXISTS basis.v_ansvarlig_myndighed CASCADE;

CREATE VIEW basis.v_ansvarlig_myndighed AS

SELECT
	a.cvr_navn || COALESCE(' (' || a.kommunekode || ')', '') AS label,
	a.*
FROM basis.ansvarlig_myndighed a
ORDER BY 1;

COMMENT ON VIEW basis.v_ansvarlig_myndighed IS 'Opdaterbar view. Look-up for basis.ansvarlig_myndighed.';

SELECT custom.create_auto_trigger('basis', 'v_ansvarlig_myndighed', 'basis', 'ansvarlig_myndighed');


-- DROP VIEW IF EXISTS basis.v_driftniv CASCADE;

CREATE VIEW basis.v_driftniv AS

SELECT
	a.driftniv::text AS label,
	a.*
FROM basis.driftniv a
ORDER BY 2;

COMMENT ON VIEW basis.v_driftniv IS 'Opdaterbar view. Look-up for basis.driftniv.';

SELECT custom.create_auto_trigger('basis', 'v_driftniv', 'basis', 'driftniv');


-- DROP VIEW IF EXISTS basis.v_kommunal_kontakt CASCADE;

CREATE VIEW basis.v_kommunal_kontakt AS

SELECT
	a.navn || ', tlf: ' || basis.telefon(a.telefon::text) || ', ' || email AS label,
	a.*
FROM basis.kommunal_kontakt a
ORDER BY 1;

COMMENT ON VIEW basis.v_kommunal_kontakt IS 'Opdaterbar view. Look-up for basis.kommunal_kontakt.';

SELECT custom.create_auto_trigger('basis', 'v_kommunal_kontakt', 'basis', 'kommunal_kontakt');


-- DROP VIEW IF EXISTS basis.v_offentlig CASCADE;

CREATE VIEW basis.v_offentlig AS

SELECT
	a.offentlig::text AS label,
	a.*
FROM basis.offentlig a
ORDER BY 2;

COMMENT ON VIEW basis.v_offentlig IS 'Opdaterbar view. Look-up for basis.offentlig.';

SELECT custom.create_auto_trigger('basis', 'v_offentlig', 'basis', 'offentlig');


-- DROP VIEW IF EXISTS basis.v_oprindelse CASCADE;

CREATE VIEW basis.v_oprindelse AS

SELECT
	a.oprindelse::text AS label,
	a.*
FROM basis.oprindelse a
ORDER BY 2;

COMMENT ON VIEW basis.v_oprindelse IS 'Opdaterbar view. Look-up for basis.oprindelse.';

SELECT custom.create_auto_trigger('basis', 'v_oprindelse', 'basis', 'oprindelse');


-- DROP VIEW IF EXISTS basis.v_postnr CASCADE;

CREATE VIEW basis.v_postnr AS

SELECT
	a.postnr || ' ' || a.postnr_by AS label,
	a.*
FROM basis.postnr a;

COMMENT ON VIEW basis.v_postnr IS 'Look-up for basis.postnr.';

SELECT custom.create_auto_trigger('basis', 'v_postnr', 'basis', 'postnr');


-- DROP VIEW IF EXISTS basis.v_status CASCADE;

CREATE VIEW basis.v_status AS

SELECT
	a.status::text AS label,
	a.*
FROM basis.status a
ORDER BY 2;

COMMENT ON VIEW basis.v_status IS 'Opdaterbar view. Look-up for basis.status.';

SELECT custom.create_auto_trigger('basis', 'v_status', 'basis', 'status');


-- DROP VIEW IF EXISTS basis.v_tilstand CASCADE;

CREATE VIEW basis.v_tilstand AS

SELECT
	a.tilstand::text AS label,
	a.*
FROM basis.tilstand a
ORDER BY 2;

COMMENT ON VIEW basis.v_tilstand IS 'Opdaterbar view. Look-up for basis.tilstand.';

SELECT custom.create_auto_trigger('basis', 'v_tilstand', 'basis', 'tilstand');


-- DROP VIEW IF EXISTS basis.v_udfoerer CASCADE;

CREATE VIEW basis.v_udfoerer AS

SELECT
	a.udfoerer::text AS label,
	a.*
FROM basis.udfoerer a
ORDER BY 1;

COMMENT ON VIEW basis.v_udfoerer IS 'Opdaterbar view. Look-up for basis.udfoerer.';

SELECT custom.create_auto_trigger('basis', 'v_udfoerer', 'basis', 'udfoerer');


-- DROP VIEW IF EXISTS basis.v_udfoerer_entrep CASCADE;

CREATE VIEW basis.v_udfoerer_entrep AS

SELECT
	a.udfoerer_entrep::text AS label,
	a.*
FROM basis.udfoerer_entrep a
ORDER BY 1;

COMMENT ON VIEW basis.v_udfoerer_entrep IS 'Opdaterbar view. Look-up for basis.udfoerer_entrep.';

SELECT custom.create_auto_trigger('basis', 'v_udfoerer_entrep', 'basis', 'udfoerer_entrep');


-- DROP VIEW IF EXISTS basis.v_ukrudtsbek CASCADE;

CREATE VIEW basis.v_ukrudtsbek AS

SELECT
	a.ukrudtsbek::text AS label,
	a.*
FROM basis.ukrudtsbek a
ORDER BY 2;

COMMENT ON VIEW basis.v_ukrudtsbek IS 'Opdaterbar view. Look-up for basis.ukrudtsbek.';

SELECT custom.create_auto_trigger('basis', 'v_ukrudtsbek', 'basis', 'ukrudtsbek');


-- DROP VIEW IF EXISTS basis.v_vejnavn CASCADE;

CREATE VIEW basis.v_vejnavn AS

SELECT
	a.vejnavn || ', ' || b.label AS label,
	a.*
FROM basis.vejnavn a
LEFT JOIN basis.v_postnr b ON a.postnr = b.postnr;

COMMENT ON VIEW basis.v_vejnavn IS 'Look-up for basis.vejnavn.';

SELECT custom.create_auto_trigger('basis', 'v_vejnavn', 'basis', 'vejnavn');


-- DROP VIEW IF EXISTS basis.v_afdelinger CASCADE;

CREATE VIEW basis.v_afdelinger AS

SELECT
	a.afdeling_tekst::text AS label,
	a.*
FROM filter.filter('basis', 'afdelinger') tbl(afdeling_nr int)
INNER JOIN basis.afdelinger a ON tbl.afdeling_nr = a.afdeling_nr
ORDER BY 2;

COMMENT ON VIEW basis.v_afdelinger IS 'Opdaterbar view. Look-up for basis.afdelinger.';

SELECT custom.create_auto_trigger('basis', 'v_afdelinger', 'basis', 'afdelinger');


-- DROP VIEW IF EXISTS basis.v_look_afdelinger CASCADE;

CREATE VIEW basis.v_look_afdelinger AS

SELECT
	a.geometri,
	a.afdeling_nr,
	a.afdeling_tekst::text AS label
FROM basis.afdelinger a;


-- DROP VIEW IF EXISTS basis.v_distrikt_type CASCADE;

CREATE VIEW basis.v_distrikt_type AS

SELECT
	a.pg_distrikt_type::text AS label,
	a.*
FROM basis.distrikt_type a
ORDER BY 2;

COMMENT ON VIEW basis.v_distrikt_type IS 'Opdaterbar view. Look-up for basis.distrikt_type.';

SELECT custom.create_auto_trigger('basis', 'v_distrikt_type', 'basis', 'distrikt_type');


-- DROP VIEW IF EXISTS basis.v_klip_sider CASCADE;

CREATE VIEW basis.v_klip_sider AS

SELECT
	a.klip_sider::text AS label,
	a.*
FROM basis.klip_sider a
ORDER BY 2;

COMMENT ON VIEW basis.v_klip_sider IS 'Opdaterbar view. Look-up for basis.klip_sider.';

SELECT custom.create_auto_trigger('basis', 'v_klip_sider', 'basis', 'klip_sider');


-- DROP VIEW IF EXISTS basis.v_objekt_typer CASCADE;

CREATE VIEW basis.v_objekt_typer AS

SELECT
	a.beskrivelse::text AS label,
	a.*
FROM basis.objekt_typer a
ORDER BY 2;

COMMENT ON VIEW basis.v_objekt_typer IS 'Opdaterbar view. Look-up for basis.objekt_typer.';

SELECT custom.create_auto_trigger('basis', 'v_objekt_typer', 'basis', 'objekt_typer');


-- DROP VIEW IF EXISTS basis.v_prisregulering CASCADE;

CREATE VIEW basis.v_prisregulering AS

SELECT
	a.dato,
	a.aendring_pct,
	1 + a.aendring_pct / 100 AS prisregulering_faktor,
	basis.prisregulering(a.dato) AS samlet_prisreg
FROM basis.prisregulering a;

COMMENT ON VIEW basis.v_prisregulering IS 'Opdaterbar view. Look-up for basis.prisregulering.';

SELECT custom.create_auto_trigger('basis', 'v_prisregulering', 'basis', 'prisregulering');


-- DROP VIEW IF EXISTS basis.v_udfoerer_kontakt CASCADE;

CREATE VIEW basis.v_udfoerer_kontakt AS

SELECT
	a.navn || ', tlf: ' || basis.telefon(a.telefon::text) || ', ' || a.email AS label,
	b.udfoerer,
	a.*
FROM basis.udfoerer_kontakt a
LEFT JOIN basis.udfoerer b ON a.udfoerer_kode = b.udfoerer_kode;

COMMENT ON VIEW basis.v_udfoerer_kontakt IS 'Opdaterbar view. Look-up for basis.udfoerer_kontakt.';

SELECT custom.create_auto_trigger('basis', 'v_udfoerer_kontakt', 'basis', 'udfoerer_kontakt');


-- DROP VIEW IF EXISTS basis.v_elementer CASCADE;

CREATE VIEW basis.v_elementer AS

WITH

	cte1 AS(
		SELECT DISTINCT
			a.element_kode[1:t._val] AS element_kode,
			UNNEST(a.objekt_type) AS objekt_type,
			a.aktiv
		FROM basis.elementer a, generate_subscripts(element_kode, 1) AS t(_val)
	),

	cte2 AS(
		SELECT
			element_kode,
			array_agg(objekt_type ORDER BY objekt_type) AS objekt_type_vrt
		FROM cte1
		WHERE aktiv
		GROUP BY element_kode, aktiv
	)

SELECT
	basis.element(a.element_kode) || ' ' || a.elementnavn AS label,
	a.element_kode,
	basis.element(a.element_kode) AS element_kode_def,
	basis.element(a.element_kode[1:cardinality(a.element_kode)-1]) AS element_kode_tdl,
	a.element_kode[cardinality(a.element_kode)] AS element_kode_niv,
	a.elementnavn,
	a.objekt_type,
	COALESCE(b.objekt_type_vrt, '{}'::int[]) AS objekt_type_vrt,
	a.tbl,
	a.enhedspris,
	CASE
		WHEN a.tbl
		THEN (a.enhedspris * (SELECT basis.prisregulering(current_date)))::numeric(10,2)
		ELSE 0.00::numeric(10,2)
	END AS enhedspris_reg,
	CASE
		WHEN a.tbl
		THEN (a.enhedspris * (SELECT basis.prisregulering((SELECT _value::date FROM filter._value('historik')))))::numeric(10,2)
		ELSE 0.00::numeric(10,2)
	END AS enhedspris_reg_his,
	a.aktiv,
	custom.array_hierarchy(a.element_kode, basis.separator()) AS element_kode_all
FROM basis.elementer a
LEFT JOIN cte2 b ON a.element_kode = b.element_kode
ORDER BY a.element_kode;

CREATE TRIGGER element_kode_trg_iu INSTEAD OF INSERT OR UPDATE
	ON basis.v_elementer
	FOR EACH ROW
	EXECUTE PROCEDURE basis.element_kode_trg_iu();

SELECT custom.create_auto_trigger('basis', 'v_elementer', 'basis', 'elementer');

CREATE VIEW basis.v_elementer_1 AS

SELECT
	*
FROM basis.v_elementer
WHERE cardinality(element_kode) = 1;

CREATE VIEW basis.v_elementer_2 AS

SELECT
	*
FROM basis.v_elementer
WHERE cardinality(element_kode) = 2;

CREATE VIEW basis.v_elementer_3 AS

SELECT
	*
FROM basis.v_elementer
WHERE cardinality(element_kode) = 3;


-- DROP VIEW IF EXISTS basis.v_maengder CASCADE;

CREATE VIEW basis.v_maengder AS

SELECT
	a.id,
	--'{}'::text[] AS 
	a.element_kode,
	basis.element_arr(a.element_kode) AS element_kode_def,
	a.beskrivelse,
	a.alt_enhed,
	a.objekt_type,
	a.enhedspris,
	(a.enhedspris * (SELECT basis.prisregulering(current_date)))::numeric(10,2) AS enhedspris_reg,
	(a.enhedspris * (SELECT basis.prisregulering((SELECT _value::date FROM filter._value('historik')))))::numeric(10,2) AS enhedspris_reg_his,
	a.maengde_sql,
	a.source_schema,
	a.source_table,
	a.source_column,
	a.source_label,
	a.source_column_pris,
	a.source_where_clause,
	a.target_column
FROM basis.maengder a;

COMMENT ON VIEW basis.v_maengder IS 'Opdaterbar view. Look-up for basis.maengder.';

CREATE TRIGGER a_quote_iu
	INSTEAD OF INSERT OR UPDATE ON basis.v_maengder
	FOR EACH ROW
	EXECUTE PROCEDURE basis.maengder_quote_trg();

SELECT custom.create_auto_trigger('basis', 'v_maengder', 'basis', 'maengder');


-- DROP VIEW IF EXISTS basis.v_data_omraader CASCADE;

CREATE VIEW basis.v_omraader AS

SELECT
	a.geometri,
	a.pg_distrikt_nr || ' ' || a.pg_distrikt_tekst AS omraade,
	a.pg_distrikt_nr,
	a.pg_distrikt_tekst,
	a.afdeling_nr,
	COALESCE(afd.afdeling_tekst, 'Udenfor afdeling') AS afdeling,
	a.pg_distrikt_type_kode,
	dt.label AS pg_distrikt_type,
	a.vejkode,
	v.vejnavn,
	v.label AS vej,
	a.vejnr,
	a.postnr,
	p.label AS postdistrikt,
	a.kommunal_kontakt_kode,
	kk.label AS kommunal_kontakt,
	a.udfoerer_kode,
	u.label AS udfoerer,
	a.udfoerer_kontakt_kode1,
	uk1.label AS udfoerer_kontakt1,
	a.udfoerer_kontakt_kode2,
	uk2.label AS udfoerer_kontakt2,
	a.note,
	a.link,
	a.aktiv,
	a.synlig,
	public.ST_Area(a.geometri)::numeric(10, 1) AS areal
FROM filter.filter('basis', 'omraader') tbl(pg_distrikt_nr int)
INNER JOIN basis.omraader a ON tbl.pg_distrikt_nr = a.pg_distrikt_nr
LEFT JOIN basis.v_afdelinger afd ON a.afdeling_nr = afd.afdeling_nr
LEFT JOIN basis.v_distrikt_type dt ON a.pg_distrikt_type_kode = dt.pg_distrikt_type_kode
LEFT JOIN basis.v_vejnavn v ON a.vejkode = v.vejkode
LEFT JOIN basis.v_postnr p ON a.postnr = p.postnr
LEFT JOIN basis.v_udfoerer u ON a.udfoerer_kode = u.udfoerer_kode
LEFT JOIN basis.v_udfoerer_kontakt uk1 ON (a.udfoerer_kontakt_kode1) = (uk1.udfoerer_kontakt_kode)
LEFT JOIN basis.v_udfoerer_kontakt uk2 ON (a.udfoerer_kontakt_kode1) = (uk2.udfoerer_kontakt_kode)
LEFT JOIN basis.v_kommunal_kontakt kk ON a.kommunal_kontakt_kode = kk.kommunal_kontakt_kode
ORDER BY a.pg_distrikt_nr;

SELECT custom.create_auto_trigger('basis', 'v_omraader', 'basis', 'omraader');

-- DROP VIEW IF EXISTS basis.v_look_omraader CASCADE;
/*
CREATE VIEW basis.v_look_omraader AS

SELECT
	ST_Multi(ST_Buffer(a.geometri, 50))::geometry('MultiPolygon', 25832) AS geometri,
	a.pg_distrikt_nr,
	a.pg_distrikt_nr || ' ' || a.pg_distrikt_tekst AS omraade
FROM basis.omraader a
ORDER BY a.pg_distrikt_nr;
*/

CREATE VIEW basis.v_look_omraader AS

(WITH

	cte1 AS(
		SELECT
			ST_Envelope(
				ST_Buffer(
					ST_Envelope(
						(ST_Dump(a.geometri)).geom
					), 50, 1
				)
			) AS geometri,
			a.pg_distrikt_nr,
			a.pg_distrikt_tekst
		FROM basis.omraader a
	)

SELECT
	ST_Multi(
		ST_Union(a.geometri)
	)::geometry('MultiPolygon', 25832) AS geometri,
	a.pg_distrikt_nr,
	a.pg_distrikt_nr || ' ' || a.pg_distrikt_tekst AS omraade
FROM cte1 a
GROUP BY a.pg_distrikt_nr, a.pg_distrikt_tekst
ORDER BY a.pg_distrikt_nr)

UNION

SELECT
	a.geometri,
	a.pg_distrikt_nr,
	a.pg_distrikt_nr || ' ' || a.pg_distrikt_tekst AS omraade
FROM basis.omraader a
WHERE public.ST_IsEmpty(geometri) OR geometri IS NULL;


-- DROP VIEW IF EXISTS basis.v_prep_delomraader CASCADE;

CREATE VIEW basis.v_prep_delomraader AS

SELECT
	a.geometri,
	a.id,
	a.pg_distrikt_nr,
	a.beskrivelse,
	om.pg_distrikt_type_kode,
	om.postnr,
	om.udfoerer_kode,
	om.afdeling_nr,
	om.omraade || ' - ' || a.beskrivelse AS omraade,
	om.pg_distrikt_type,
	om.vejnavn,
	om.vejnr,
	om.postdistrikt
FROM basis.delomraader a
LEFT JOIN basis.v_omraader om ON a.pg_distrikt_nr = om.pg_distrikt_nr;

-- DROP VIEW IF EXISTS basis.v_delomraader CASCADE;

CREATE VIEW basis.v_delomraader AS

SELECT
	a.*,
	ROW_NUMBER() OVER(PARTITION BY a.pg_distrikt_nr ORDER BY a.beskrivelse)::int AS part
FROM filter.filter('basis', 'v_prep_delomraader') tbl(id int)
INNER JOIN basis.v_prep_delomraader a ON a.id = tbl.id;

SELECT custom.create_auto_trigger('basis', 'v_delomraader', 'basis', 'delomraader');



--
-- SCHEMA greg
--

CREATE SCHEMA greg;
COMMENT ON SCHEMA greg IS 'Skema indeholdende datatabeller.';


--
-- FUNCTIONS
--


-- DROP FUNCTION IF EXISTS greg.tbl(schema_name text, table_name text, type int) CASCADE;

CREATE OR REPLACE FUNCTION greg.tbl(schema_name text, table_name text, _type int DEFAULT 1)
	RETURNS TABLE(
		pg_distrikt_nr int,
		omraade text,
		element_kode text,
		element text,
		maengde_besk text,
		maengde numeric,
		enhedspris numeric(10, 2),
		pris numeric(10, 2)
	)
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	geom_type int;
	_besk text;
	_unit text;
	_mgd_sql text;
	_decimal int;
	_price text;
	_filter text;

BEGIN

--
-- TYPE
-- 0 = pris
-- 1 = pris_reg
-- 2 = pris_reg_his
--

	IF $3 = 1 THEN

		_price := '_reg';
		_filter := 'AND a.systid_til IS NULL';

	ELSIF $3 = 2 THEN

		_price := '_reg_his';
		_filter := $$AND	a.systid_fra <= (SELECT _value::timestamptz FROM filter._value('historik')) AND 
							(a.systid_til > (SELECT _value::timestamptz FROM filter._value('historik')) OR a.systid_til IS NULL)$$;

	ELSE

		_price := '';
		_filter := '';

	END IF;

--
-- Look up both geomtry type
--

	SELECT
		a.geom_type
	FROM custom.geometry_of($1, $2) a
	INTO geom_type;

--
-- Determine both command to use and number of decimals based on geometry type
--

	EXECUTE FORMAT(
		$qt$
			SELECT
				a.mgd_besk,
				a.enhed,
				a.mgd_sql,
				a._decimal
			FROM basis.objekt_typer a
			WHERE a.dimension = %s
		$qt$, geom_type
	)
	INTO _besk, _unit, _mgd_sql, _decimal;

--
-- Summarize
--

	RETURN QUERY
	EXECUTE FORMAT(
		$qt$
			(SELECT
				a.pg_distrikt_nr,
				a.omraade::text,
				b.element_kode_def AS element_kode,
				b.label::text AS element,
				('%5$s' || ' (' || '%6$s' || ')')::text AS mgd_besk,
				SUM(%3$s)::numeric(10, %4$s) AS mgd,
				NULLIF(b.enhedspris%7$s::numeric(10, 2), 0.00) AS enhedspris,
				NULLIF(SUM(%3$s * b.enhedspris%7$s)::numeric(10, 2), 0.00) AS pris
			FROM %1$I.%2$I a
			LEFT JOIN basis.v_elementer b ON a.element_kode = b.element_kode
			WHERE a.aktiv %8$s
			GROUP BY
				a.pg_distrikt_nr,
				a.omraade,
				b.element_kode_def,
				b.label,
				b.enhedspris%7$s
			ORDER BY
				3, 6)
		$qt$, $1, $2, '(' || _mgd_sql || ')', _decimal, _besk, _unit, _price, _filter
	);

END $BODY$;

COMMENT ON FUNCTION greg.tbl(schema_name text, table_name text, type int) IS 'Sammendrag af mængder baseret på element og pg_distrikt_nr ved angivelse af tabel (Alle mængder).
Derudover angives en type ift. priser:
0: Grundpriser
1: Prisregulering, dags dato
2: Prisregulering, historik';


-- DROP FUNCTION IF EXISTS greg.tbl_ext(schema_name text, table_name text, _type int) CASCADE;

CREATE OR REPLACE FUNCTION greg.tbl_ext(schema_name text, table_name text, _type int DEFAULT 1)
	RETURNS TABLE(
		pg_distrikt_nr int,
		omraade text,
		element_kode text,
		element text,
		maengde_besk text,
		maengde numeric,
		enhedspris numeric(10, 2),
		pris numeric(10, 2)
	)
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	geom_type int;
	_besk text;
	_unit text;
	_mgd_sql text;
	_decimal int;
	_price text;
	_price_2 text;
	_query text;
	_filter text;

BEGIN

--
-- TYPE
-- 0 = pris
-- 1 = pris_reg
-- 2 = pris_reg_his
--

	IF $3 = 1 THEN

		_price := '_reg';
		_price_2 := '* (SELECT basis.prisregulering(current_date))';
		_filter := 'AND a.systid_til IS NULL';

	ELSIF $3 = 2 THEN

		_price := '_reg_his';
		_price_2 := '* (SELECT basis.prisregulering((SELECT _value::date FROM filter._value(''historik''))))';
		_filter := $$AND	a.systid_fra <= (SELECT _value::timestamptz FROM filter._value('historik')) AND 
							(a.systid_til > (SELECT _value::timestamptz FROM filter._value('historik')) OR a.systid_til IS NULL)$$;

	ELSE

		_price := '';
		_price_2 := '';
		_filter := '';

	END IF;

--
-- Look up both geomtry type and column
--

	SELECT
		a.geom_type
	FROM custom.geometry_of($1, $2) a
	INTO geom_type;

--
-- Determine both command to use and number of decimals based on geometry type
--

	EXECUTE FORMAT(
		$qt$
			SELECT
				a.mgd_besk,
				a.enhed,
				a.mgd_sql,
				a._decimal
			FROM basis.objekt_typer a
			WHERE a.dimension = %s
		$qt$, geom_type
	)
	INTO _besk, _unit, _mgd_sql, _decimal;

--
-- The following query generates the SQL for the total amount based on specifications (basis.maengder)
-- The structure is two SELECT statements concatenated in a single SELECT
-- First SELECT retrives the base amounts based on the geometry (NumGeometries, Length and Area)
-- The second SELECT aggregates SQL generated from each row from basis.maengder (Returns empty string if no rows matching the geometry type)
--

	SELECT

--
-- First SELECT
--

		(SELECT
			FORMAT(
				$$
					(SELECT
						a.pg_distrikt_nr,
						a.omraade::text,
						b.element_kode_def AS element_kode,
						b.label::text AS element,
						('%5$s' || ' (' || '%6$s' || ')')::text AS mgd_besk,
						SUM(%3$s)::numeric(10, %4$s) AS mgd,
						NULLIF(b.enhedspris%7$s::numeric(10, 2), 0.00) AS enhedspris,
						NULLIF(SUM(%3$s * b.enhedspris%7$s)::numeric(10, 2), 0.00) AS pris
					FROM %1$I.%2$I a
					LEFT JOIN basis.v_elementer b ON a.element_kode = b.element_kode
					WHERE b.tbl AND a.aktiv %8$s
					GROUP BY
						a.pg_distrikt_nr,
						a.omraade,
						b.element_kode_def,
						b.label,
						b.enhedspris%7$s
					ORDER BY
						3, 6)
				$$, $1, $2, '(' || _mgd_sql || ')', _decimal, _besk, _unit, _price, _filter
			)
		) ||

--
-- Second SELECT
--

		(WITH

			cte1 AS(
				SELECT
					a.id,
					UNNEST(CASE WHEN a.element_kode != '{}' THEN a.element_kode ELSE '{-1}' END) AS element_kode,
					a.beskrivelse,
					a.alt_enhed,
					a.objekt_type,
					a.enhedspris,
					a.aktiv,
					a.maengde_sql,
					a.source_schema,
					a.source_table,
					a.source_column,
					a.source_label,
					a.source_column_pris,
					a.source_where_clause,
					a.target_column
				FROM basis.maengder a
			)

		SELECT
			COALESCE( -- COALESCE to ensure some sort of string
				'UNION ALL' || -- UNION with first SELECT
				string_agg( -- Aggregate of all the rows matching the geometry type
					FORMAT(
						$$
							(WITH

								cte1 AS(
									SELECT
										%2$s AS src_col, -- column_name or TRUE
										%3$s AS src_label, -- Label generated
										%4$s::numeric(10, 2) AS src_col_pris -- Price either from source table or from basis.maengder
									%1$s -- FROM source table or just a single row
									%5$s -- WHERE CLAUSE to exclude certain values in SELECT
								)

								SELECT
									a.pg_distrikt_nr,
									a.omraade::text,
									b.element_kode_def AS element_kode,
									b.label::text AS element,
									c.src_label AS mgd_besk,
									SUM(%6$s)::numeric(10, %11$s) AS mgd, -- SUM of specified SQL or the general SQL for the datatype (NumGeometries, Length and Area)
									NULLIF((c.src_col_pris %12$s)::numeric(10, 2), 0.00) AS enhedspris, -- Price, might be adjusted
									NULLIF(SUM(%6$s * c.src_col_pris %12$s)::numeric(10, 2), 0.00) AS pris -- Actual price, might be adjusted
								FROM %7$I.%8$I a -- Specified table
								LEFT JOIN basis.v_elementer b ON a.element_kode = b.element_kode
								INNER JOIN cte1 c ON %9$s -- Either join based on reference table or TRUE
								WHERE '%14$s'::bool AND b.tbl AND (a.element_kode::text = '%10$s' OR '%10$s' = '-1') AND a.aktiv %13$s -- b.tbl IS TRUE AND a.elementkode is either any of the elements chosen or no elements has been chosen
								GROUP BY
									a.pg_distrikt_nr,
									a.omraade,
									b.element_kode_def,
									b.label,
									mgd_besk,
									c.src_col_pris)
						$$,
							COALESCE('FROM ' || quote_ident(a.source_schema) || '.' || quote_ident(a.source_table), ''),
							COALESCE(a.source_column, 'TRUE'),
							'''' || a.beskrivelse || ''' ' || COALESCE('|| '' - '' ||' || a.source_label, '') || '|| '' (' || CASE WHEN a.alt_enhed IN(SELECT dimension::text FROM basis.objekt_typer) THEN (SELECT z.enhed FROM basis.objekt_typer z WHERE z.dimension::text = a.alt_enhed) ELSE COALESCE(a.alt_enhed, b.enhed) END || ')''',
							COALESCE('COALESCE(' || a.source_column_pris || ', 0.00)', a.enhedspris::text),
							COALESCE('WHERE ' || a.source_where_clause, ''),
							'(' || COALESCE(a.maengde_sql, b.mgd_sql) || ')',
							$1,
							$2,
							COALESCE('a.' || COALESCE(a.target_column, source_column) || ' = c.src_col', 'TRUE'),
							a.element_kode,
							b._decimal,
							_price_2,
							_filter,
							a.aktiv
					), E'\n\nUNION ALL\n\n'
				) || E'\nORDER BY 2, 4, 5', ''
			)
		FROM cte1 a
		LEFT JOIN basis.objekt_typer b ON b.dimension = geom_type
		WHERE ARRAY[geom_type] <@ objekt_type
		)
	INTO _query;

	RETURN QUERY
	EXECUTE FORMAT(
		'%s', _query
	);

END $BODY$;

COMMENT ON FUNCTION greg.tbl_ext(schema_name text, table_name text, type int) IS 'Udvidet sammendrag af mængder (basis.maengder inkluderet) baseret på element og pg_distrikt_nr ved angivelse af tabel.
Derudover angives en type ift. priser:
0: Grundpriser
1: Prisregulering, dags dato
2: Prisregulering, historik';


-- DROP FUNCTION IF EXISTS greg.tbl_ext_i(schema_name text, table_name text, _type int) CASCADE;

CREATE OR REPLACE FUNCTION greg.tbl_ext_i(schema_name text, table_name text, _type int DEFAULT 1)
	RETURNS TABLE(
		versions_id uuid,
		maengde_besk text,
		enhed text,
		maengde numeric,
		enhedspris numeric(10, 2),
		pris numeric(10, 2)
	)
	LANGUAGE plpgsql AS
$BODY$

DECLARE

	geom_type int;
	_besk text;
	_unit text;
	_mgd_sql text;
	_decimal int;
	_price text;
	_price_2 text;
	_query text;
	_filter text;

BEGIN

--
-- TYPE
-- 0 = pris
-- 1 = pris_reg
-- 2 = pris_reg_his
--

	IF $3 = 1 THEN

		_price := '_reg';
		_price_2 := '* (SELECT basis.prisregulering(current_date))';
		_filter := 'AND a.systid_til IS NULL';

	ELSIF $3 = 2 THEN

		_price := '_reg_his';
		_price_2 := '* (SELECT basis.prisregulering((SELECT _value::date FROM filter._value(''historik''))))';
		_filter := $$AND	a.systid_fra <= (SELECT _value::timestamptz FROM filter._value('historik')) AND 
							(a.systid_til > (SELECT _value::timestamptz FROM filter._value('historik')) OR a.systid_til IS NULL)$$;

	ELSE

		_price := '';
		_price_2 := '';
		_filter := '';

	END IF;

--
-- Look up both geomtry type and column
--

	SELECT
		a.geom_type
	FROM custom.geometry_of($1, $2) a
	INTO geom_type;

--
-- Determine both command to use and number of decimals based on geometry type
--

	EXECUTE FORMAT(
		$qt$
			SELECT
				a.mgd_besk,
				a.enhed,
				a.mgd_sql,
				a._decimal
			FROM basis.objekt_typer a
			WHERE a.dimension = %s
		$qt$, geom_type
	)
	INTO _besk, _unit, _mgd_sql, _decimal;

--
-- The following query generates the SQL for the total amount based on specifications (basis.maengder)
-- The structure is two SELECT statements concatenated in a single SELECT
-- First SELECT retrives the base amounts based on the geometry (NumGeometries, Length and Area)
-- The second SELECT aggregates SQL generated from each row from basis.maengder (Returns empty string if no rows matching the geometry type)
--

	SELECT

--
-- First SELECT
--

		(SELECT
			FORMAT(
				$$
					(SELECT
						a.versions_id,
						'%5$s'::text AS mgd_besk,
						'%6$s'::text AS enhed,
						SUM(%3$s)::numeric(10, %4$s) AS mgd,
						NULLIF(b.enhedspris%7$s::numeric(10, 2), 0.00) AS enhedspris,
						NULLIF(SUM(%3$s * b.enhedspris%7$s)::numeric(10, 2), 0.00) AS pris
					FROM %1$I.%2$I a
					LEFT JOIN basis.v_elementer b ON a.element_kode = b.element_kode
					WHERE b.tbl AND a.aktiv %8$s
					GROUP BY
						a.versions_id,
						b.enhedspris%7$s
					ORDER BY
						2)
				$$, $1, $2, '(' || _mgd_sql || ')', _decimal, _besk, _unit, _price, _filter
			)
		) ||

--
-- Second SELECT
--

		(WITH

			cte1 AS(
				SELECT
					a.id,
					UNNEST(CASE WHEN a.element_kode != '{}' THEN a.element_kode ELSE '{-1}' END) AS element_kode,
					a.beskrivelse,
					a.alt_enhed,
					a.objekt_type,
					a.enhedspris,
					a.aktiv,
					a.maengde_sql,
					a.source_schema,
					a.source_table,
					a.source_column,
					a.source_label,
					a.source_column_pris,
					a.source_where_clause,
					a.target_column
				FROM basis.maengder a
			)

		SELECT
			COALESCE( -- COALESCE to ensure some sort of string
				'UNION ALL' || -- UNION with first SELECT
				string_agg( -- Aggregate of all the rows matching the geometry type
					FORMAT(
						$$
							(WITH

								cte1 AS(
									SELECT
										%2$s AS src_col, -- column_name or TRUE
										(%3$s)::text AS src_label, -- Label generated
										'%13$s'::text AS enhed, -- Unit
										%4$s::numeric(10, 2) AS src_col_pris -- Price either from source table or from basis.maengder
									%1$s -- FROM source table or just a single row
									%5$s -- WHERE CLAUSE to exclude certain values in SELECT
								)

								SELECT
									a.versions_id,
									c.src_label AS mgd_besk,
									c.enhed,
									SUM(%6$s)::numeric(10, %11$s) AS mgd, -- SUM of specified SQL or the general SQL for the datatype (NumGeometries, Length and Area)
									NULLIF((c.src_col_pris %12$s)::numeric(10, 2), 0.00) AS enhedspris, -- Price, might be adjusted
									NULLIF(SUM(%6$s * c.src_col_pris %12$s)::numeric(10, 2), 0.00) AS pris -- Actual price, might be adjusted
								FROM %7$I.%8$I a -- Specified table
								LEFT JOIN basis.v_elementer b ON a.element_kode = b.element_kode
								INNER JOIN cte1 c ON %9$s -- Either join based on reference table or TRUE
								WHERE '%15$s'::bool AND b.tbl AND (a.element_kode::text = '%10$s' OR '%10$s' = '-1') AND a.aktiv %14$s -- b.tbl IS TRUE AND a.element_kode is either any of the elements chosen or no elements has been chosen
								GROUP BY
									a.versions_id,
									mgd_besk,
									c.enhed,
									c.src_col_pris)
						$$,
							COALESCE('FROM ' || quote_ident(a.source_schema) || '.' || quote_ident(a.source_table), ''),
							COALESCE(a.source_column, 'TRUE'),
							'''' || a.beskrivelse || ''' ' || COALESCE('|| '' - '' || ' || a.source_label, ''),
							COALESCE('COALESCE(' || a.source_column_pris || ', 0.00)', a.enhedspris::text),
							COALESCE('WHERE ' || a.source_where_clause, ''),
							'(' || COALESCE(a.maengde_sql, b.mgd_sql) || ')',
							$1,
							$2,
							COALESCE('a.' || COALESCE(a.target_column, source_column) || ' = c.src_col', 'TRUE'),
							a.element_kode,
							b._decimal,
							_price_2,
							CASE WHEN a.alt_enhed IN(SELECT dimension::text FROM basis.objekt_typer) THEN (SELECT z.enhed FROM basis.objekt_typer z WHERE z.dimension::text = a.alt_enhed) ELSE COALESCE(a.alt_enhed, b.enhed) END,
							_filter,
							a.aktiv
					), E'\n\nUNION ALL\n\n'
				) || E'\nORDER BY 1,2', ''
			)
		FROM cte1 a
		LEFT JOIN basis.objekt_typer b ON b.dimension = geom_type
		WHERE ARRAY[geom_type] <@ objekt_type
		)
	INTO _query;

	RETURN QUERY
	EXECUTE FORMAT(
		'%s', _query
	);

END $BODY$;

COMMENT ON FUNCTION greg.tbl_ext_i(schema_name text, table_name text, type int) IS 'Udvidet sammendrag af mængder (basis.maengder inkluderet) baseret på versions_id ved angivelse af tabel.
Derudover angives en type ift. priser:
0: Grundpriser
1: Prisregulering, dags dato
2: Prisregulering, historik';


-- DROP FUNCTION IF EXISTS greg.tbl_html(schema_name text, table_name text, _type int) CASCADE;

CREATE OR REPLACE FUNCTION greg.tbl_html(schema_name text, table_name text, _type int DEFAULT 1)
	RETURNS TABLE(
		versions_id uuid,
		_html text
	)
	LANGUAGE plpgsql AS
$BODY$

BEGIN

	IF (SELECT filter._value('maengder')::boolean) THEN

		RETURN QUERY
		WITH

			cte1 AS(
				SELECT
					a.versions_id,
					a.maengde_besk,
					E'\n<td>' ||
					a.maengde_besk ||
					E'</td>\n<td align="right">' ||
					TO_CHAR(a.maengde, 'FM999G999G999G999G999G999G999G990D0') || ' ' || a.enhed ||
					E'</td>\n<td align="right">' ||
					COALESCE(TO_CHAR(a.enhedspris, 'FM999G999G999G999G999G999G999G990D00') || ' kr.', '-') ||
					E'</td>\n<td align="right">' ||
					COALESCE(TO_CHAR(a.pris, 'FM999G999G999G999G999G999G999G990D00') || ' kr.', '-') ||
					E'</td>\n' AS _body,
					NULLIF(SUM(a.pris) OVER(PARTITION BY a.versions_id), 0.00) AS _total
				FROM greg.tbl_ext_i($1, $2, $3) a
			),

			cte2 AS(
				SELECT
					a.versions_id,
					'<tr>' ||
					string_agg(a._body, E'</tr>\n<tr>' ORDER BY a.maengde_besk) ||
					'</tr><tr><td colspan="4" align="right">' ||
					REPEAT('_', LENGTH(COALESCE(TO_CHAR(a._total, 'FM999G999G999G999G999G999G999G990D00') || ' kr.', '_____'))) ||
					'</td>' ||
					'</tr><tr><td>Total</td><td colspan="3" align="right">' ||
					COALESCE(TO_CHAR(a._total, 'FM999G999G999G999G999G999G999G990D00') || ' kr.', '-') ||
					'</td></tr>' AS _body
				FROM cte1 a
				GROUP BY a.versions_id, a._total
			)

		SELECT
			a.versions_id,
			FORMAT(
				$$
					<!DOCTYPE html>
					<html>
					<head>
					<style>
					th, td {
						padding: 5px;
					}
					th {
						text-align: center;
					}
					</style>
					</head>
					<body>

					<table style="width:100%%">
						<tr>
							<th>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Beskrivelse&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</th>
							<th>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Mængde&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</th>
							<th>&nbsp;&nbsp;&nbsp;&nbsp;Enhedspris&nbsp;&nbsp;&nbsp;&nbsp;</th>
							<th>&nbsp;&nbsp;&nbsp;&nbsp;Pris&nbsp;&nbsp;&nbsp;&nbsp;</th>
						</tr>
						%s
					</table>

					</body>
					</html>
				$$, a._body
			)
		FROM cte2 a;

	ELSE

		RETURN QUERY
		SELECT
			NULL::uuid AS versions_id,
			NULL::text AS _html;

	END IF;

END $BODY$;

COMMENT ON FUNCTION greg.tbl_html(schema_name text, table_name text, type int) IS 'Genererer sammendrag af greg.tbl_ext_i i HTML-format.
Derudover angives en type ift. priser:
0: Grundpriser
1: Prisregulering, dags dato
2: Prisregulering, historik';


--
-- TRIGGER FUNCTIONS
--


DO $DO_BODY$

DECLARE

	_col text;

BEGIN

--
-- Find columns
--

	SELECT
		'COALESCE(' || string_agg('NEW.element_kode_' || SUBSTRING(relname::text, '\d+$'), ', ' ORDER BY relname DESC) || ')'
	FROM pg_catalog.pg_class a
	LEFT JOIN pg_catalog.pg_namespace b ON a.relnamespace = b.oid
	WHERE b.nspname = 'basis' AND a.relname ~* 'v_elementer_\d'
	INTO _col;

	EXECUTE FORMAT(
		$$
			-- DROP FUNCTION IF EXISTS greg.data_element_kode() CASCADE;

			CREATE OR REPLACE FUNCTION greg.data_element_kode()
				RETURNS trigger
				LANGUAGE plpgsql AS
			$BODY$

			BEGIN

					NEW.element_kode = (SELECT a.element_kode FROM basis.v_elementer a WHERE a.element_kode_def = %s);

					IF NOT EXISTS(
						SELECT
							'1'
						FROM basis.elementer a
						WHERE a.element_kode = NEW.element_kode
						AND ARRAY[(SELECT (custom.geometry_of(TG_TABLE_SCHEMA, TG_TABLE_NAME)).geom_type)] <@ a.objekt_type
						AND a.aktiv
					) THEN

						RAISE EXCEPTION 'Det givne element kan ikke benyttes på til det givne lag!';

					END IF;

					RETURN NEW;

			END $BODY$;
		$$, _col
	);

END $DO_BODY$;


-- DROP FUNCTION IF EXISTS greg.red_omraader_trg() CASCADE;

CREATE OR REPLACE FUNCTION greg.red_omraader_trg()
	RETURNS trigger
	LANGUAGE plpgsql
	SECURITY DEFINER AS
$BODY$

BEGIN

--
-- DELETE
--

	IF (TG_OP = 'DELETE') THEN

		IF OLD.pg_distrikt_nr IS NULL THEN

			RETURN OLD;

		END IF;

		UPDATE basis.omraader a
			SET
				geometri = (
					SELECT
						public.ST_Multi(
							public.ST_Union(
								public.ST_Union(
									public.ST_Union(
										public.ST_Difference(
											b.geometri, public.ST_Buffer(OLD.geometri, 0.25 + 0.01)
										), (SELECT COALESCE(ST_Buffer(ST_Union(g.geometri), 0.25), ST_GeomFromText('POLYGON EMPTY', 25832)) FROM greg.flader g	WHERE g.systid_til IS NULL AND g.versions_id != OLD.versions_id AND g.pg_distrikt_nr = OLD.pg_distrikt_nr AND public.ST_Intersects(g.geometri, public.ST_Buffer(OLD.geometri, 0.25 + 0.01)))
									), (SELECT COALESCE(ST_Buffer(ST_Union(g.geometri), 0.25), ST_GeomFromText('LINESTRING EMPTY', 25832)) FROM greg.linier g	WHERE g.systid_til IS NULL AND g.versions_id != OLD.versions_id AND g.pg_distrikt_nr = OLD.pg_distrikt_nr AND public.ST_Intersects(g.geometri, public.ST_Buffer(OLD.geometri, 0.25 + 0.01)))
								), (SELECT COALESCE(ST_Buffer(ST_Union(g.geometri), 0.25), ST_GeomFromText('POINT EMPTY', 25832)) FROM greg.punkter g			WHERE g.systid_til IS NULL AND g.versions_id != OLD.versions_id AND g.pg_distrikt_nr = OLD.pg_distrikt_nr AND public.ST_Intersects(g.geometri, public.ST_Buffer(OLD.geometri, 0.25 + 0.01)))
							)
						)
					FROM basis.omraader b
					WHERE b.pg_distrikt_nr = OLD.pg_distrikt_nr
				)
		WHERE a.pg_distrikt_nr = OLD.pg_distrikt_nr;

		RETURN OLD;

	END IF;

--
-- UPDATE
--

	IF (TG_OP = 'UPDATE') THEN

--
-- No changes in geometry
--

		IF
			(public.ST_Equals(NEW.geometri, OLD.geometri)
			OR (NEW.pg_distrikt_nr IS NULL AND OLD.pg_distrikt_nr IS NULL))
			AND NEW.pg_distrikt_nr IS NOT DISTINCT FROM OLD.pg_distrikt_nr
		THEN

			RETURN NEW;

		END IF;

--
-- Same pg_distrikt_nr
--

		IF NEW.pg_distrikt_nr IS NOT DISTINCT FROM OLD.pg_distrikt_nr THEN

			UPDATE basis.omraader a
				SET
					geometri = (
						SELECT
							public.ST_Multi(
								public.ST_Union(
									public.ST_Union(
										public.ST_Union(
											public.ST_Union(
												public.ST_Difference(
													b.geometri, public.ST_Buffer(OLD.geometri, 0.25 + 0.01)
												), (SELECT COALESCE(ST_Buffer(ST_Union(g.geometri), 0.25), ST_GeomFromText('POLYGON EMPTY', 25832)) FROM greg.flader g	WHERE g.systid_til IS NULL AND g.versions_id != OLD.versions_id AND g.pg_distrikt_nr = OLD.pg_distrikt_nr AND public.ST_Intersects(g.geometri, public.ST_Buffer(OLD.geometri, 0.25 + 0.01)))
											), (SELECT COALESCE(ST_Buffer(ST_Union(g.geometri), 0.25), ST_GeomFromText('LINESTRING EMPTY', 25832)) FROM greg.linier g	WHERE g.systid_til IS NULL AND g.versions_id != OLD.versions_id AND g.pg_distrikt_nr = OLD.pg_distrikt_nr AND public.ST_Intersects(g.geometri, public.ST_Buffer(OLD.geometri, 0.25 + 0.01)))
										), (SELECT COALESCE(ST_Buffer(ST_Union(g.geometri), 0.25), ST_GeomFromText('POINT EMPTY', 25832)) FROM greg.punkter g			WHERE g.systid_til IS NULL AND g.versions_id != OLD.versions_id AND g.pg_distrikt_nr = OLD.pg_distrikt_nr AND public.ST_Intersects(g.geometri, public.ST_Buffer(OLD.geometri, 0.25 + 0.01)))
									), public.ST_Buffer(NEW.geometri, 0.25)
								)
							)
						FROM basis.omraader b
						WHERE b.pg_distrikt_nr = NEW.pg_distrikt_nr
					)
			WHERE a.pg_distrikt_nr = NEW.pg_distrikt_nr;

			RETURN NEW;

		ELSE

			IF OLD.pg_distrikt_nr IS NOT NULL THEN

				UPDATE basis.omraader a
					SET
						geometri = (
							SELECT
								public.ST_Multi(
									public.ST_Union(
										public.ST_Union(
											public.ST_Union(
												public.ST_Difference(
													b.geometri, public.ST_Buffer(OLD.geometri, 0.25 + 0.01)
												), (SELECT COALESCE(ST_Buffer(ST_Union(g.geometri), 0.25), ST_GeomFromText('POLYGON EMPTY', 25832)) FROM greg.flader g	WHERE g.systid_til IS NULL AND g.versions_id != OLD.versions_id AND g.pg_distrikt_nr = OLD.pg_distrikt_nr AND public.ST_Intersects(g.geometri, public.ST_Buffer(OLD.geometri, 0.25 + 0.01)))
											), (SELECT COALESCE(ST_Buffer(ST_Union(g.geometri), 0.25), ST_GeomFromText('LINESTRING EMPTY', 25832)) FROM greg.linier g	WHERE g.systid_til IS NULL AND g.versions_id != OLD.versions_id AND g.pg_distrikt_nr = OLD.pg_distrikt_nr AND public.ST_Intersects(g.geometri, public.ST_Buffer(OLD.geometri, 0.25 + 0.01)))
										), (SELECT COALESCE(ST_Buffer(ST_Union(g.geometri), 0.25), ST_GeomFromText('POINT EMPTY', 25832)) FROM greg.punkter g			WHERE g.systid_til IS NULL AND g.versions_id != OLD.versions_id AND g.pg_distrikt_nr = OLD.pg_distrikt_nr AND public.ST_Intersects(g.geometri, public.ST_Buffer(OLD.geometri, 0.25 + 0.01)))
									)
								)
							FROM basis.omraader b
							WHERE b.pg_distrikt_nr = OLD.pg_distrikt_nr
						)
				WHERE a.pg_distrikt_nr = OLD.pg_distrikt_nr;

			END IF;

			IF NEW.pg_distrikt_nr IS NOT NULL THEN

				UPDATE basis.omraader a
					SET
						geometri = (
							SELECT
								public.ST_Multi(
									public.ST_Union(
										b.geometri, public.ST_Buffer(NEW.geometri, 0.25)
									)
								)
							FROM basis.omraader b
							WHERE b.pg_distrikt_nr = NEW.pg_distrikt_nr
						)
				WHERE a.pg_distrikt_nr = NEW.pg_distrikt_nr;

			END IF;

			RETURN NEW;

		END IF;

--
-- INSERT
--

	ELSIF (TG_OP = 'INSERT') THEN

--
-- Geometry is within area
--

		IF EXISTS(
			SELECT
				'1'
			FROM basis.omraader a
			WHERE a.pg_distrikt_nr = NEW.pg_distrikt_nr
			AND public.ST_Within(NEW.geometri, a.geometri)
		) THEN

			RETURN NEW;

		END IF;

		UPDATE basis.omraader a
			SET
				geometri = (
					SELECT
						public.ST_Multi(
							public.ST_Union(
								b.geometri, public.ST_Buffer(NEW.geometri, 0.25)
							)
						)
					FROM basis.omraader b
					WHERE b.pg_distrikt_nr = NEW.pg_distrikt_nr
				)
		WHERE a.pg_distrikt_nr = NEW.pg_distrikt_nr;

		RETURN NEW;

	END IF;

END $BODY$;


--
-- TABLES
--


-- DROP TABLE IF EXISTS greg.flader CASCADE;

CREATE TABLE greg.flader (
	versions_id uuid NOT NULL,
	objekt_id uuid NOT NULL,
	oprettet timestamp with time zone NOT NULL,
	systid_fra timestamp with time zone NOT NULL,
	systid_til timestamp with time zone,
	bruger_id_start character varying(128) NOT NULL,
	bruger_id_slut character varying(128),
	geometri public.geometry('MultiPolygon', 25832) NOT NULL,
	cvr_kode integer NOT NULL DEFAULT filter._value('cvr')::int,
	oprindkode integer NOT NULL DEFAULT filter._value('oprind')::int,
	statuskode integer NOT NULL DEFAULT filter._value('status')::int,
	off_kode integer NOT NULL DEFAULT filter._value('off_')::int,
	element_kode text[] NOT NULL,
	pg_distrikt_nr integer,
	vejkode integer,
	vejnr character varying(20),
	anlaegsaar custom.dato,
	etabl_pleje_udloeb custom.dato,
	udskiftningsaar custom.dato,
	udtyndaar custom.dato,
	kommunal_kontakt_kode integer,
	udfoerer_entrep_kode integer,
	driftniv_kode integer,
	ukrudtsbek_kode integer,
	konto_nr character varying(150),
	tilstand_kode integer DEFAULT filter._value('tilstand')::int,
	klip_sider_kode integer,
	hoejde custom.maal,
	slaegt character varying(150),
	art character varying(150),
	sort character varying(150),
	note character varying(254),
	link character varying(1024),
	-- User-defined
	litra character varying(128),
	-- Constraints
	CONSTRAINT flader_pk						PRIMARY KEY (versions_id) WITH (fillfactor='10') DEFERRABLE INITIALLY DEFERRED,
	CONSTRAINT flader_fk_ansvarlig_myndighed	FOREIGN KEY (cvr_kode) REFERENCES basis.ansvarlig_myndighed(cvr_kode) MATCH FULL,
	CONSTRAINT flader_fk_driftniv				FOREIGN KEY (driftniv_kode) REFERENCES basis.driftniv(driftniv_kode) MATCH FULL,
	CONSTRAINT flader_fk_elementer				FOREIGN KEY (element_kode) REFERENCES basis.elementer(element_kode) MATCH FULL,
	CONSTRAINT flader_fk_klip_sider				FOREIGN KEY (klip_sider_kode) REFERENCES basis.klip_sider(klip_sider_kode) MATCH FULL,
	CONSTRAINT flader_fk_kommunal_kontakt		FOREIGN KEY (kommunal_kontakt_kode) REFERENCES basis.kommunal_kontakt(kommunal_kontakt_kode) MATCH FULL,
	CONSTRAINT flader_fk_offentlig				FOREIGN KEY (off_kode) REFERENCES basis.offentlig(off_kode) MATCH FULL,
	CONSTRAINT flader_fk_oprindelse				FOREIGN KEY (oprindkode) REFERENCES basis.oprindelse(oprindkode) MATCH FULL,
	CONSTRAINT flader_fk_status					FOREIGN KEY (statuskode) REFERENCES basis.status(statuskode) MATCH FULL,
	CONSTRAINT flader_fk_tilstand				FOREIGN KEY (tilstand_kode) REFERENCES basis.tilstand(tilstand_kode) MATCH FULL,
	CONSTRAINT flader_fk_udfoerer_entrep		FOREIGN KEY (udfoerer_entrep_kode) REFERENCES basis.udfoerer_entrep(udfoerer_entrep_kode) MATCH FULL,
	CONSTRAINT flader_fk_ukrudtsbek				FOREIGN KEY (ukrudtsbek_kode) REFERENCES basis.ukrudtsbek(ukrudtsbek_kode) MATCH FULL,
	CONSTRAINT flader_fk_vejnavn				FOREIGN KEY (vejkode) REFERENCES basis.vejnavn(vejkode) MATCH FULL,
	CONSTRAINT flader_ck_geometri				CHECK (public.ST_IsValid(geometri) IS TRUE AND public.ST_IsEmpty(geometri) IS FALSE)
);

COMMENT ON TABLE greg.flader IS 'Rådatatabel for elementer defineret som flader. Indeholder både aktuel og historisk data.';

CREATE TRIGGER a_geom_check_i
	BEFORE INSERT ON greg.flader
	FOR EACH ROW
	WHEN (NEW.systid_til IS NULL)
	EXECUTE PROCEDURE custom.geom_check_passive('systid_til IS NULL', '(SELECT geometri_tjek_2 FROM filter.v_settings) AND NOT (SELECT geometri_aggresive FROM filter.v_settings)');

CREATE TRIGGER a_geom_check_aggr_i
	BEFORE INSERT ON greg.flader
	FOR EACH ROW
	WHEN (NEW.systid_til IS NULL)
	EXECUTE PROCEDURE custom.geom_check_aggressive('systid_til IS NULL', '(SELECT geometri_tjek_2 FROM filter.v_settings) AND (SELECT geometri_aggresive FROM filter.v_settings)');

CREATE TRIGGER b_red_omraader_i
	BEFORE INSERT ON greg.flader
	FOR EACH ROW
	WHEN (NEW.systid_til IS NULL AND NEW.pg_distrikt_nr IS NOT NULL AND COALESCE(filter._value('omr_red')::boolean, TRUE))
	EXECUTE PROCEDURE greg.red_omraader_trg();

CREATE TRIGGER b_red_omraader_u
	BEFORE UPDATE ON greg.flader
	FOR EACH ROW
	WHEN ((NEW.pg_distrikt_nr IS NOT NULL OR OLD.pg_distrikt_nr IS NOT NULL) AND COALESCE(filter._value('omr_red')::boolean, TRUE))
	EXECUTE PROCEDURE greg.red_omraader_trg();

CREATE TRIGGER b_red_omraader_d
	BEFORE DELETE ON greg.flader
	FOR EACH ROW
	WHEN (COALESCE(filter._value('omr_red')::boolean, TRUE) AND OLD.pg_distrikt_nr IS NOT NULL)
	EXECUTE PROCEDURE greg.red_omraader_trg();

CREATE TRIGGER z_flader_history_iud
	BEFORE INSERT OR UPDATE OR DELETE ON greg.flader
	FOR EACH ROW
	EXECUTE PROCEDURE custom.history();

CREATE INDEX flader_gist ON greg.flader USING gist(geometri);


-- DROP TABLE IF EXISTS greg.linier CASCADE;

CREATE TABLE greg.linier (
	versions_id uuid NOT NULL,
	objekt_id uuid NOT NULL,
	oprettet timestamp with time zone NOT NULL,
	systid_fra timestamp with time zone NOT NULL,
	systid_til timestamp with time zone,
	bruger_id_start character varying(128) NOT NULL,
	bruger_id_slut character varying(128),
	geometri public.geometry('MultiLineString', 25832) NOT NULL,
	cvr_kode integer NOT NULL DEFAULT filter._value('cvr')::int,
	oprindkode integer NOT NULL DEFAULT filter._value('oprind')::int,
	statuskode integer NOT NULL DEFAULT filter._value('status')::int,
	off_kode integer NOT NULL DEFAULT filter._value('off_')::int,
	element_kode text[] NOT NULL,
	pg_distrikt_nr integer,
	vejkode integer,
	vejnr character varying(20),
	anlaegsaar custom.dato,
	etabl_pleje_udloeb custom.dato,
	udskiftningsaar custom.dato,
	kommunal_kontakt_kode integer,
	udfoerer_entrep_kode integer,
	driftniv_kode integer,
	ukrudtsbek_kode integer,
	konto_nr character varying(150),
	tilstand_kode integer DEFAULT filter._value('tilstand')::int,
	klip_sider_kode integer,
	bredde custom.maal,
	hoejde custom.maal,
	slaegt character varying(150),
	art character varying(150),
	sort character varying(150),
	note character varying(254),
	link character varying(1024),
	-- User-defined
	litra character varying(128),
	-- Constraints
	CONSTRAINT linier_pk						PRIMARY KEY (versions_id) WITH (fillfactor='10') DEFERRABLE INITIALLY DEFERRED,
	CONSTRAINT linier_fk_ansvarlig_myndighed	FOREIGN KEY (cvr_kode) REFERENCES basis.ansvarlig_myndighed(cvr_kode) MATCH FULL,
	CONSTRAINT linier_fk_driftniv				FOREIGN KEY (driftniv_kode) REFERENCES basis.driftniv(driftniv_kode) MATCH FULL,
	CONSTRAINT linier_fk_elementer				FOREIGN KEY (element_kode) REFERENCES basis.elementer(element_kode) MATCH FULL,
	CONSTRAINT linier_fk_klip_sider				FOREIGN KEY (klip_sider_kode) REFERENCES basis.klip_sider(klip_sider_kode) MATCH FULL,
	CONSTRAINT linier_fk_kommunal_kontakt		FOREIGN KEY (kommunal_kontakt_kode) REFERENCES basis.kommunal_kontakt(kommunal_kontakt_kode) MATCH FULL,
	CONSTRAINT linier_fk_offentlig				FOREIGN KEY (off_kode) REFERENCES basis.offentlig(off_kode) MATCH FULL,
	CONSTRAINT linier_fk_oprindelse				FOREIGN KEY (oprindkode) REFERENCES basis.oprindelse(oprindkode) MATCH FULL,
	CONSTRAINT linier_fk_status					FOREIGN KEY (statuskode) REFERENCES basis.status(statuskode) MATCH FULL,
	CONSTRAINT linier_fk_tilstand				FOREIGN KEY (tilstand_kode) REFERENCES basis.tilstand(tilstand_kode) MATCH FULL,
	CONSTRAINT linier_fk_udfoerer_entrep		FOREIGN KEY (udfoerer_entrep_kode) REFERENCES basis.udfoerer_entrep(udfoerer_entrep_kode) MATCH FULL,
	CONSTRAINT linier_fk_ukrudtsbek				FOREIGN KEY (ukrudtsbek_kode) REFERENCES basis.ukrudtsbek(ukrudtsbek_kode) MATCH FULL,
	CONSTRAINT linier_fk_vejnavn				FOREIGN KEY (vejkode) REFERENCES basis.vejnavn(vejkode) MATCH FULL,
	CONSTRAINT linier_ck_geometri				CHECK (public.ST_IsValid(geometri) IS TRUE AND public.ST_IsEmpty(geometri) IS FALSE)
);

COMMENT ON TABLE greg.linier IS 'Rådatatabel for elementer defineret som linier. Indeholder både aktuel og historisk data.';

CREATE TRIGGER b_red_omraader_i
	BEFORE INSERT ON greg.linier
	FOR EACH ROW
	WHEN (NEW.systid_til IS NULL AND NEW.pg_distrikt_nr IS NOT NULL AND COALESCE(filter._value('omr_red')::boolean, TRUE))
	EXECUTE PROCEDURE greg.red_omraader_trg();

CREATE TRIGGER b_red_omraader_u
	BEFORE UPDATE ON greg.linier
	FOR EACH ROW
	WHEN ((NEW.pg_distrikt_nr IS NOT NULL OR OLD.pg_distrikt_nr IS NOT NULL) AND COALESCE(filter._value('omr_red')::boolean, TRUE))
	EXECUTE PROCEDURE greg.red_omraader_trg();

CREATE TRIGGER b_red_omraader_d
	BEFORE DELETE ON greg.linier
	FOR EACH ROW
	WHEN (COALESCE(filter._value('omr_red')::boolean, TRUE) AND OLD.pg_distrikt_nr IS NOT NULL)
	EXECUTE PROCEDURE greg.red_omraader_trg();

CREATE TRIGGER z_linier_history_iud
	BEFORE INSERT OR UPDATE OR DELETE ON greg.linier
	FOR EACH ROW
	EXECUTE PROCEDURE custom.history();

CREATE INDEX linier_gist ON greg.linier USING gist(geometri);


-- DROP TABLE IF EXISTS greg.punkter CASCADE;

CREATE TABLE greg.punkter (
	versions_id uuid NOT NULL,
	objekt_id uuid NOT NULL,
	oprettet timestamp with time zone NOT NULL,
	systid_fra timestamp with time zone NOT NULL,
	systid_til timestamp with time zone,
	bruger_id_start character varying(128) NOT NULL,
	bruger_id_slut character varying(128),
	geometri public.geometry('MultiPoint', 25832) NOT NULL,
	cvr_kode integer NOT NULL DEFAULT filter._value('cvr')::int,
	oprindkode integer NOT NULL DEFAULT filter._value('oprind')::int,
	statuskode integer NOT NULL DEFAULT filter._value('status')::int,
	off_kode integer NOT NULL DEFAULT filter._value('off_')::int,
	element_kode text[] NOT NULL,
	pg_distrikt_nr integer,
	vejkode integer,
	vejnr character varying(20),
	anlaegsaar custom.dato,
	etabl_pleje_udloeb custom.dato,
	udskiftningsaar custom.dato,
	kommunal_kontakt_kode integer,
	udfoerer_entrep_kode integer,
	driftniv_kode integer,
	ukrudtsbek_kode integer,
	konto_nr character varying(150),
	tilstand_kode integer DEFAULT filter._value('tilstand')::int,
	laengde custom.maal,
	bredde custom.maal,
	diameter custom.maal,
	hoejde custom.maal,
	slaegt character varying(150),
	art character varying(150),
	sort character varying(150),
	note character varying(254),
	link character varying(1024),
	-- User-defined
	litra character varying(128),
	-- Constraints
	CONSTRAINT punkter_pk						PRIMARY KEY (versions_id) WITH (fillfactor='10') DEFERRABLE INITIALLY DEFERRED,
	CONSTRAINT punkter_fk_ansvarlig_myndighed	FOREIGN KEY (cvr_kode) REFERENCES basis.ansvarlig_myndighed(cvr_kode) MATCH FULL,
	CONSTRAINT punkter_fk_driftniv				FOREIGN KEY (driftniv_kode) REFERENCES basis.driftniv(driftniv_kode) MATCH FULL,
	CONSTRAINT punkter_fk_elementer				FOREIGN KEY (element_kode) REFERENCES basis.elementer(element_kode) MATCH FULL,
	CONSTRAINT punkter_fk_kommunal_kontakt		FOREIGN KEY (kommunal_kontakt_kode) REFERENCES basis.kommunal_kontakt(kommunal_kontakt_kode) MATCH FULL,
	CONSTRAINT punkter_fk_offentlig				FOREIGN KEY (off_kode) REFERENCES basis.offentlig(off_kode) MATCH FULL,
	CONSTRAINT punkter_fk_oprindelse			FOREIGN KEY (oprindkode) REFERENCES basis.oprindelse(oprindkode) MATCH FULL,
	CONSTRAINT punkter_fk_status				FOREIGN KEY (statuskode) REFERENCES basis.status(statuskode) MATCH FULL,
	CONSTRAINT punkter_fk_tilstand				FOREIGN KEY (tilstand_kode) REFERENCES basis.tilstand(tilstand_kode) MATCH FULL,
	CONSTRAINT punkter_fk_udfoerer_entrep		FOREIGN KEY (udfoerer_entrep_kode) REFERENCES basis.udfoerer_entrep(udfoerer_entrep_kode) MATCH FULL,
	CONSTRAINT punkter_fk_ukrudtsbek			FOREIGN KEY (ukrudtsbek_kode) REFERENCES basis.ukrudtsbek(ukrudtsbek_kode) MATCH FULL,
	CONSTRAINT punkter_fk_vejnavn				FOREIGN KEY (vejkode) REFERENCES basis.vejnavn(vejkode) MATCH FULL,
	CONSTRAINT punkter_ck_geometri				CHECK (public.ST_IsValid(geometri) IS TRUE AND public.ST_IsEmpty(geometri) IS FALSE),
	CONSTRAINT punkter_ck_measure				CHECK (((laengde IS NULL OR laengde = 0.00) AND (bredde IS NULL OR bredde = 0.00)) OR (diameter IS NULL OR diameter = 0.00))
);

COMMENT ON TABLE greg.punkter IS 'Rådatatabel for elementer defineret som punkter. Indeholder både aktuel og historisk data.';

CREATE TRIGGER b_red_omraader_i
	BEFORE INSERT ON greg.punkter
	FOR EACH ROW
	WHEN (NEW.systid_til IS NULL AND NEW.pg_distrikt_nr IS NOT NULL AND COALESCE(filter._value('omr_red')::boolean, TRUE))
	EXECUTE PROCEDURE greg.red_omraader_trg();

CREATE TRIGGER b_red_omraader_u
	BEFORE UPDATE ON greg.punkter
	FOR EACH ROW
	WHEN ((NEW.pg_distrikt_nr IS NOT NULL OR OLD.pg_distrikt_nr IS NOT NULL) AND COALESCE(filter._value('omr_red')::boolean, TRUE))
	EXECUTE PROCEDURE greg.red_omraader_trg();

CREATE TRIGGER b_red_omraader_d
	BEFORE DELETE ON greg.punkter
	FOR EACH ROW
	WHEN (COALESCE(filter._value('omr_red')::boolean, TRUE) AND OLD.pg_distrikt_nr IS NOT NULL)
	EXECUTE PROCEDURE greg.red_omraader_trg();

CREATE TRIGGER z_punkter_history_iud
	BEFORE INSERT OR UPDATE OR DELETE ON greg.punkter
	FOR EACH ROW
	EXECUTE PROCEDURE custom.history();

CREATE INDEX punkter_gist ON greg.punkter USING gist(geometri);


--
-- VIEWS
--


-- DROP VIEW IF EXISTS greg.v_data_flader CASCADE;

CREATE VIEW greg.v_data_flader AS

SELECT
	a.versions_id,
	a.objekt_id,
	a.oprettet,
	a.systid_fra,
	a.systid_til,
	a.bruger_id_start,
	COALESCE(fs1.navn, a.bruger_id_start) AS bruger_start,
	a.bruger_id_slut,
	COALESCE(fs2.navn, a.bruger_id_slut) AS bruger_slut,
	a.geometri,
	a.cvr_kode,
	am.label AS kommune,
	a.oprindkode,
	o.oprindelse,
	a.statuskode,
	s.status,
	a.off_kode,
	of.offentlig,
	a.element_kode,
	ele.element_kode_all[1] AS element_kode_1,
	ele.element_kode_all[2] AS element_kode_2,
	ele.element_kode_all[3] AS element_kode_3,
	ele.label AS element,
	om.afdeling_nr, -- Filter
	om.afdeling,
	a.pg_distrikt_nr,
	COALESCE(om.omraade, 'Udenfor område') AS omraade,
	om.pg_distrikt_type_kode, -- Filter
	om.pg_distrikt_type,
	om.postnr, -- Filter
	om.postdistrikt,
	om.udfoerer_kode, -- Filter
	a.vejkode,
	v.label AS vej,
	a.vejnr,
	a.anlaegsaar,
	a.etabl_pleje_udloeb,
	a.udskiftningsaar,
	a.udtyndaar,
	a.kommunal_kontakt_kode,
	kk.label AS kommunal_kontakt,
	a.udfoerer_entrep_kode,
	u.label AS udfoerer_entrep,
	a.driftniv_kode,
	dn.label AS driftniv,
	a.ukrudtsbek_kode,
	ub.label AS ukrudtsbek,
	a.konto_nr,
	a.tilstand_kode,
	t.label AS tilstand,
	a.klip_sider_kode,
	ks.label AS klip_sider,
	a.hoejde,
	a.slaegt,
	a.art,
	a.sort,
	a.note,
	a.link,
	a.litra,
	public.ST_Area(a.geometri)::numeric(10, 1) AS areal,
	public.ST_Perimeter(a.geometri)::numeric(10, 1) AS omkreds,
	CASE 
		WHEN a.pg_distrikt_nr IS NOT NULL
		THEN om.aktiv
		ELSE TRUE
	END AS aktiv,
	COALESCE(om.omraade, 'Udenfor område') || ', ' || ele.label AS label,
	FALSE::boolean AS omr_filter
FROM greg.flader a
LEFT JOIN filter.settings fs1 ON a.bruger_id_start = fs1.rolname
LEFT JOIN filter.settings fs2 ON a.bruger_id_slut = fs2.rolname
LEFT JOIN basis.v_ansvarlig_myndighed am ON a.cvr_kode = am.cvr_kode
LEFT JOIN basis.v_oprindelse o ON a.oprindkode = o.oprindkode
LEFT JOIN basis.v_status s ON a.statuskode = s.statuskode
LEFT JOIN basis.v_offentlig of ON a.off_kode = of.off_kode
LEFT JOIN basis.v_udfoerer_entrep u ON a.udfoerer_entrep_kode = u.udfoerer_entrep_kode
LEFT JOIN basis.v_kommunal_kontakt kk ON a.kommunal_kontakt_kode = kk.kommunal_kontakt_kode
LEFT JOIN basis.v_driftniv dn ON a.driftniv_kode = dn.driftniv_kode
LEFT JOIN basis.v_ukrudtsbek ub ON a.ukrudtsbek_kode = ub.ukrudtsbek_kode
LEFT JOIN basis.v_tilstand t ON a.tilstand_kode = t.tilstand_kode
LEFT JOIN basis.v_vejnavn v ON a.vejkode = v.vejkode
LEFT JOIN basis.v_klip_sider ks ON a.klip_sider_kode = ks.klip_sider_kode
LEFT JOIN basis.v_elementer ele ON a.element_kode = ele.element_kode
LEFT JOIN basis.v_omraader om ON a.pg_distrikt_nr = om.pg_distrikt_nr;

-- DROP VIEW IF EXISTS greg.v_cur_flader CASCADE;

CREATE VIEW greg.v_cur_flader AS

SELECT
	a.*,
	COALESCE(b._html, 'Mængder er slået fra.') AS _html
FROM filter.filter('greg', 'v_data_flader') tbl(versions_id uuid)
INNER JOIN greg.v_data_flader a ON a.versions_id = tbl.versions_id
LEFT JOIN greg.tbl_html('greg', 'v_data_flader', 1) b ON a.versions_id = b.versions_id
WHERE a.systid_til IS NULL;

CREATE TRIGGER a_v_cur_flader_element_kode_iud INSTEAD OF INSERT OR UPDATE ON greg.v_cur_flader FOR EACH ROW EXECUTE PROCEDURE greg.data_element_kode();

SELECT custom.create_auto_trigger('greg', 'v_cur_flader', 'greg', 'flader');

-- DROP VIEW IF EXISTS greg.v_his_flader CASCADE;

CREATE VIEW greg.v_his_flader AS

SELECT
	a.*,
	COALESCE(b._html, 'Mængder er slået fra.') AS _html
FROM filter.filter('greg', 'v_data_flader') tbl(versions_id uuid)
INNER JOIN greg.v_data_flader a ON a.versions_id = tbl.versions_id
LEFT JOIN greg.tbl_html('greg', 'v_data_flader', 2) b ON a.versions_id = b.versions_id
WHERE	a.systid_fra <= (SELECT _value::timestamptz FROM filter._value('historik')) AND 
		(a.systid_til > (SELECT _value::timestamptz FROM filter._value('historik')) OR a.systid_til IS NULL);


-- DROP VIEW IF EXISTS greg.v_data_linier CASCADE;

CREATE VIEW greg.v_data_linier AS

SELECT
	a.versions_id,
	a.objekt_id,
	a.oprettet,
	a.systid_fra,
	a.systid_til,
	a.bruger_id_start,
	COALESCE(fs1.navn, a.bruger_id_start) AS bruger_start,
	a.bruger_id_slut,
	COALESCE(fs2.navn, a.bruger_id_slut) AS bruger_slut,
	a.geometri,
	a.cvr_kode,
	am.label AS kommune,
	a.oprindkode,
	o.oprindelse,
	a.statuskode,
	s.status,
	a.off_kode,
	of.offentlig,
	a.element_kode,
	ele.element_kode_all[1] AS element_kode_1,
	ele.element_kode_all[2] AS element_kode_2,
	ele.element_kode_all[3] AS element_kode_3,
	ele.label AS element,
	om.afdeling_nr, -- Filter
	om.afdeling,
	a.pg_distrikt_nr,
	COALESCE(om.omraade, 'Udenfor område') AS omraade,
	om.pg_distrikt_type_kode, -- Filter
	om.pg_distrikt_type,
	om.postnr, -- Filter
	om.postdistrikt,
	om.udfoerer_kode, -- Filter
	a.vejkode,
	v.label AS vej,
	a.vejnr,
	a.anlaegsaar,
	a.etabl_pleje_udloeb,
	a.udskiftningsaar,
	a.kommunal_kontakt_kode,
	kk.label AS kommunal_kontakt,
	a.udfoerer_entrep_kode,
	u.label AS udfoerer_entrep,
	a.driftniv_kode,
	dn.label AS driftniv,
	a.ukrudtsbek_kode,
	ub.label AS ukrudtsbek,
	a.konto_nr,
	a.tilstand_kode,
	t.label AS tilstand,
	a.klip_sider_kode,
	ks.label AS klip_sider,
	a.bredde,
	a.hoejde,
	a.slaegt,
	a.art,
	a.sort,
	a.note,
	a.link,
	a.litra,
	public.ST_Length(a.geometri)::numeric(10, 1) AS laengde,
	CASE 
		WHEN a.pg_distrikt_nr IS NOT NULL
		THEN om.aktiv
		ELSE TRUE
	END AS aktiv,
	COALESCE(om.omraade, 'Udenfor område') || ', ' || ele.label AS label,
	FALSE::boolean AS omr_filter
FROM greg.linier a
LEFT JOIN filter.settings fs1 ON a.bruger_id_start = fs1.rolname
LEFT JOIN filter.settings fs2 ON a.bruger_id_slut = fs2.rolname
LEFT JOIN basis.v_ansvarlig_myndighed am ON a.cvr_kode = am.cvr_kode
LEFT JOIN basis.v_oprindelse o ON a.oprindkode = o.oprindkode
LEFT JOIN basis.v_status s ON a.statuskode = s.statuskode
LEFT JOIN basis.v_offentlig of ON a.off_kode = of.off_kode
LEFT JOIN basis.v_udfoerer_entrep u ON a.udfoerer_entrep_kode = u.udfoerer_entrep_kode
LEFT JOIN basis.v_kommunal_kontakt kk ON a.kommunal_kontakt_kode = kk.kommunal_kontakt_kode
LEFT JOIN basis.v_driftniv dn ON a.driftniv_kode = dn.driftniv_kode
LEFT JOIN basis.v_ukrudtsbek ub ON a.ukrudtsbek_kode = ub.ukrudtsbek_kode
LEFT JOIN basis.v_tilstand t ON a.tilstand_kode = t.tilstand_kode
LEFT JOIN basis.v_vejnavn v ON a.vejkode = v.vejkode
LEFT JOIN basis.v_klip_sider ks ON a.klip_sider_kode = ks.klip_sider_kode
LEFT JOIN basis.v_elementer ele ON a.element_kode = ele.element_kode
LEFT JOIN basis.v_omraader om ON a.pg_distrikt_nr = om.pg_distrikt_nr;

-- DROP VIEW IF EXISTS greg.v_cur_linier CASCADE;

CREATE VIEW greg.v_cur_linier AS

SELECT
	a.*,
	COALESCE(b._html, 'Mængder er slået fra.') AS _html
FROM filter.filter('greg', 'v_data_linier') tbl(versions_id uuid)
INNER JOIN greg.v_data_linier a ON a.versions_id = tbl.versions_id
LEFT JOIN greg.tbl_html('greg', 'v_data_linier', 1) b ON a.versions_id = b.versions_id
WHERE a.systid_til IS NULL;

CREATE TRIGGER a_v_cur_linier_element_kode INSTEAD OF INSERT OR UPDATE ON greg.v_cur_linier FOR EACH ROW EXECUTE PROCEDURE greg.data_element_kode();

SELECT custom.create_auto_trigger('greg', 'v_cur_linier', 'greg', 'linier');

-- DROP VIEW IF EXISTS greg.v_his_linier CASCADE;

CREATE VIEW greg.v_his_linier AS

SELECT
	a.*,
	COALESCE(b._html, 'Mængder er slået fra.') AS _html
FROM filter.filter('greg', 'v_data_linier') tbl(versions_id uuid)
INNER JOIN greg.v_data_linier a ON a.versions_id = tbl.versions_id
LEFT JOIN greg.tbl_html('greg', 'v_data_linier', 2) b ON a.versions_id = b.versions_id
WHERE	a.systid_fra <= (SELECT _value::timestamptz FROM filter._value('historik')) AND 
		(a.systid_til > (SELECT _value::timestamptz FROM filter._value('historik')) OR a.systid_til IS NULL);


-- DROP VIEW IF EXISTS greg.v_data_punkter CASCADE;

CREATE VIEW greg.v_data_punkter AS

SELECT
	a.versions_id,
	a.objekt_id,
	a.oprettet,
	a.systid_fra,
	a.systid_til,
	a.bruger_id_start,
	COALESCE(fs1.navn, a.bruger_id_start) AS bruger_start,
	a.bruger_id_slut,
	COALESCE(fs2.navn, a.bruger_id_slut) AS bruger_slut,
	a.geometri,
	a.cvr_kode,
	am.label AS kommune,
	a.oprindkode,
	o.oprindelse,
	a.statuskode,
	s.status,
	a.off_kode,
	of.offentlig,
	a.element_kode,
	ele.element_kode_all[1] AS element_kode_1,
	ele.element_kode_all[2] AS element_kode_2,
	ele.element_kode_all[3] AS element_kode_3,
	ele.label AS element,
	om.afdeling_nr, -- Filter
	om.afdeling,
	a.pg_distrikt_nr,
	COALESCE(om.omraade, 'Udenfor område') AS omraade,
	om.pg_distrikt_type_kode, -- Filter
	om.pg_distrikt_type,
	om.postnr, -- Filter
	om.postdistrikt,
	om.udfoerer_kode, -- Filter
	a.vejkode,
	v.label AS vej,
	a.vejnr,
	a.anlaegsaar,
	a.etabl_pleje_udloeb,
	a.udskiftningsaar,
	a.kommunal_kontakt_kode,
	kk.label AS kommunal_kontakt,
	a.udfoerer_entrep_kode,
	u.label AS udfoerer_entrep,
	a.driftniv_kode,
	dn.label AS driftniv,
	a.ukrudtsbek_kode,
	ub.label AS ukrudtsbek,
	a.konto_nr,
	a.tilstand_kode,
	t.label AS tilstand,
	a.laengde,
	a.bredde,
	a.diameter,
	a.hoejde,
	a.slaegt,
	a.art,
	a.sort,
	a.note,
	a.link,
	a.litra,
	public.ST_NumGeometries(a.geometri) AS antal,
	CASE 
		WHEN a.pg_distrikt_nr IS NOT NULL
		THEN om.aktiv
		ELSE TRUE
	END AS aktiv,
	COALESCE(om.omraade, 'Udenfor område') || ', ' || ele.label AS label,
	FALSE::boolean AS omr_filter
FROM greg.punkter a
LEFT JOIN filter.settings fs1 ON a.bruger_id_start = fs1.rolname
LEFT JOIN filter.settings fs2 ON a.bruger_id_slut = fs2.rolname
LEFT JOIN basis.v_ansvarlig_myndighed am ON a.cvr_kode = am.cvr_kode
LEFT JOIN basis.v_oprindelse o ON a.oprindkode = o.oprindkode
LEFT JOIN basis.v_status s ON a.statuskode = s.statuskode
LEFT JOIN basis.v_offentlig of ON a.off_kode = of.off_kode
LEFT JOIN basis.v_udfoerer_entrep u ON a.udfoerer_entrep_kode = u.udfoerer_entrep_kode
LEFT JOIN basis.v_kommunal_kontakt kk ON a.kommunal_kontakt_kode = kk.kommunal_kontakt_kode
LEFT JOIN basis.v_driftniv dn ON a.driftniv_kode = dn.driftniv_kode
LEFT JOIN basis.v_ukrudtsbek ub ON a.ukrudtsbek_kode = ub.ukrudtsbek_kode
LEFT JOIN basis.v_tilstand t ON a.tilstand_kode = t.tilstand_kode
LEFT JOIN basis.v_vejnavn v ON a.vejkode = v.vejkode
LEFT JOIN basis.v_elementer ele ON a.element_kode = ele.element_kode
LEFT JOIN basis.v_omraader om ON a.pg_distrikt_nr = om.pg_distrikt_nr;

-- DROP VIEW IF EXISTS greg.v_cur_punkter CASCADE;

CREATE VIEW greg.v_cur_punkter AS

SELECT
	a.*,
	COALESCE(b._html, 'Mængder er slået fra.') AS _html
FROM filter.filter('greg', 'v_data_punkter') tbl(versions_id uuid)
INNER JOIN greg.v_data_punkter a ON a.versions_id = tbl.versions_id
LEFT JOIN greg.tbl_html('greg', 'v_data_punkter', 1) b ON a.versions_id = b.versions_id
WHERE a.systid_til IS NULL;

CREATE TRIGGER a_v_cur_punkter_element_kode INSTEAD OF INSERT OR UPDATE ON greg.v_cur_punkter FOR EACH ROW EXECUTE PROCEDURE greg.data_element_kode();

SELECT custom.create_auto_trigger('greg', 'v_cur_punkter', 'greg', 'punkter');

-- DROP VIEW IF EXISTS greg.v_his_punkter CASCADE;

CREATE VIEW greg.v_his_punkter AS

SELECT
	a.*,
	COALESCE(b._html, 'Mængder er slået fra.') AS _html
FROM filter.filter('greg', 'v_data_punkter') tbl(versions_id uuid)
INNER JOIN greg.v_data_punkter a ON a.versions_id = tbl.versions_id
LEFT JOIN greg.tbl_html('greg', 'v_data_punkter', 2) b ON a.versions_id = b.versions_id
WHERE	a.systid_fra <= (SELECT _value::timestamptz FROM filter._value('historik')) AND 
		(a.systid_til > (SELECT _value::timestamptz FROM filter._value('historik')) OR a.systid_til IS NULL);


-- DROP VIEW IF EXISTS greg.v_change_flader CASCADE;

CREATE VIEW greg.v_change_flader AS

SELECT
	*
FROM custom.log_geom(
	'greg',
	'flader',
	(SELECT COALESCE(_value::int, 14) FROM filter._value('aendringer')),
	$$COALESCE(om.pg_distrikt_tekst, 'Udenfor område') || ', ' || ele.label$$,
	$$LEFT JOIN basis.omraader om ON a.pg_distrikt_nr = om.pg_distrikt_nr
	LEFT JOIN basis.v_elementer ele ON a.element_kode = ele.element_kode$$
);


-- DROP VIEW IF EXISTS greg.v_change_linier CASCADE;

CREATE VIEW greg.v_change_linier AS

SELECT
	*
FROM custom.log_geom(
	'greg',
	'linier',
	(SELECT COALESCE(_value::int, 14) FROM filter._value('aendringer')),
	$$COALESCE(om.pg_distrikt_tekst, 'Udenfor område') || ', ' || ele.label$$,
	$$LEFT JOIN basis.omraader om ON a.pg_distrikt_nr = om.pg_distrikt_nr
	LEFT JOIN basis.v_elementer ele ON a.element_kode = ele.element_kode$$
);


-- DROP VIEW IF EXISTS greg.v_change_punkter CASCADE;

CREATE VIEW greg.v_change_punkter AS

SELECT
	*
FROM custom.log_geom(
	'greg',
	'punkter',
	(SELECT COALESCE(_value::int, 14) FROM filter._value('aendringer')),
	$$COALESCE(om.pg_distrikt_tekst, 'Udenfor område') || ', ' || ele.label$$,
	$$LEFT JOIN basis.omraader om ON a.pg_distrikt_nr = om.pg_distrikt_nr
	LEFT JOIN basis.v_elementer ele ON a.element_kode = ele.element_kode$$
);


-- DROP VIEW IF EXISTS greg.v_cur_log CASCADE;

CREATE VIEW greg.v_cur_log AS

SELECT
	'Flader' AS type,
	*
FROM custom.log(
	'greg',
	'flader',
	EXTRACT(YEAR FROM current_date)::int,
	$$COALESCE(om.pg_distrikt_tekst, 'Udenfor område') || ', ' || ele.label$$,
	$$LEFT JOIN basis.omraader om ON a.pg_distrikt_nr = om.pg_distrikt_nr
	LEFT JOIN basis.v_elementer ele ON a.element_kode = ele.element_kode$$
)

UNION ALL

SELECT
	'Linier' AS type,
	*
FROM custom.log(
	'greg',
	'linier',
	EXTRACT(YEAR FROM current_date)::int,
	$$COALESCE(om.pg_distrikt_tekst, 'Udenfor område') || ', ' || ele.label$$,
	$$LEFT JOIN basis.omraader om ON a.pg_distrikt_nr = om.pg_distrikt_nr
	LEFT JOIN basis.v_elementer ele ON a.element_kode = ele.element_kode$$
)

UNION ALL

SELECT
	'Punkter' AS type,
	*
FROM custom.log(
	'greg',
	'punkter',
	EXTRACT(YEAR FROM current_date)::int,
	$$COALESCE(om.pg_distrikt_tekst, 'Udenfor område') || ', ' || ele.label$$,
	$$LEFT JOIN basis.omraader om ON a.pg_distrikt_nr = om.pg_distrikt_nr
	LEFT JOIN basis.v_elementer ele ON a.element_kode = ele.element_kode$$
)

ORDER BY dato DESC;


-- DROP VIEW IF EXISTS greg.v_his_log CASCADE;

CREATE VIEW greg.v_his_log AS

SELECT
	'Flader' AS type,
	*
FROM custom.log(
	'greg',
	'flader',
	(SELECT _value::int FROM filter._value('log_')),
	$$COALESCE(om.pg_distrikt_tekst, 'Udenfor område') || ', ' || ele.label$$,
	$$LEFT JOIN basis.omraader om ON a.pg_distrikt_nr = om.pg_distrikt_nr
	LEFT JOIN basis.v_elementer ele ON a.element_kode = ele.element_kode$$
)

UNION ALL

SELECT
	'Linier' AS type,
	*
FROM custom.log(
	'greg',
	'linier',
	(SELECT _value::int FROM filter._value('log_')),
	$$COALESCE(om.pg_distrikt_tekst, 'Udenfor område') || ', ' || ele.label$$,
	$$LEFT JOIN basis.omraader om ON a.pg_distrikt_nr = om.pg_distrikt_nr
	LEFT JOIN basis.v_elementer ele ON a.element_kode = ele.element_kode$$
)

UNION ALL

SELECT
	'Punkter' AS type,
	*
FROM custom.log(
	'greg',
	'punkter',
	(SELECT _value::int FROM filter._value('log_')),
	$$COALESCE(om.pg_distrikt_tekst, 'Udenfor område') || ', ' || ele.label$$,
	$$LEFT JOIN basis.omraader om ON a.pg_distrikt_nr = om.pg_distrikt_nr
	LEFT JOIN basis.v_elementer ele ON a.element_kode = ele.element_kode$$
)

ORDER BY dato DESC;


-- DROP VIEW IF EXISTS greg.v_cur_tbl CASCADE;

CREATE VIEW greg.v_cur_tbl AS

WITH

	cte1 AS(
		SELECT
			omraade,
			element,
			maengde_besk,
			maengde,
			enhedspris,
			pris
		FROM greg.tbl('greg', 'v_cur_flader', 1)

		UNION ALL

		SELECT
			omraade,
			element,
			maengde_besk,
			maengde,
			enhedspris,
			pris
		FROM greg.tbl('greg', 'v_cur_linier', 1)

		UNION ALL

		SELECT
			omraade,
			element,
			maengde_besk,
			maengde,
			enhedspris,
			pris
		FROM greg.tbl('greg', 'v_cur_punkter', 1)
	)

SELECT
	'Udtræk pr. ' || to_char(current_timestamp, 'DD-MM-YYYY HH24:MI:SS') || E'\n' || (SELECT filter.label()) AS filter,
	*
FROM cte1
ORDER BY 2,3;


-- DROP VIEW IF EXISTS greg.v_cur_tbl_ext CASCADE;

CREATE VIEW greg.v_cur_tbl_ext AS

WITH

	cte1 AS(
		SELECT
			omraade,
			element,
			maengde_besk,
			maengde,
			enhedspris,
			pris
		FROM greg.tbl_ext('greg', 'v_cur_flader', 1)

		UNION ALL

		SELECT
			omraade,
			element,
			maengde_besk,
			maengde,
			enhedspris,
			pris
		FROM greg.tbl_ext('greg', 'v_cur_linier', 1)

		UNION ALL

		SELECT
			omraade,
			element,
			maengde_besk,
			maengde,
			enhedspris,
			pris
		FROM greg.tbl_ext('greg', 'v_cur_punkter', 1)
	)

SELECT
	'Udtræk pr. ' || to_char(current_timestamp, 'DD-MM-YYYY HH24:MI:SS') || E'\n' || (SELECT filter.label()) AS filter,
	*
FROM cte1
ORDER BY 2,3;


-- DROP VIEW IF EXISTS greg.v_his_tbl CASCADE;

CREATE VIEW greg.v_his_tbl AS

WITH

	cte1 AS(
		SELECT
			omraade,
			element,
			maengde_besk,
			maengde,
			enhedspris,
			pris
		FROM greg.tbl('greg', 'v_his_flader', 2)

		UNION ALL

		SELECT
			omraade,
			element,
			maengde_besk,
			maengde,
			enhedspris,
			pris
		FROM greg.tbl('greg', 'v_his_linier', 2)

		UNION ALL

		SELECT
			omraade,
			element,
			maengde_besk,
			maengde,
			enhedspris,
			pris
		FROM greg.tbl('greg', 'v_his_punkter', 2)
	)

SELECT
	'Udtræk pr. ' || to_char(filter._value('historik')::timestamp, 'DD-MM-YYYY HH24:MI:SS') || E'\n' || (SELECT filter.label()) AS filter,
	*
FROM cte1
ORDER BY 2,3;


-- DROP VIEW IF EXISTS greg.v_his_tbl_ext CASCADE;

CREATE VIEW greg.v_his_tbl_ext AS

WITH

	cte1 AS(
		SELECT
			omraade,
			element,
			maengde_besk,
			maengde,
			enhedspris,
			pris
		FROM greg.tbl_ext('greg', 'v_his_flader', 2)

		UNION ALL

		SELECT
			omraade,
			element,
			maengde_besk,
			maengde,
			enhedspris,
			pris
		FROM greg.tbl_ext('greg', 'v_his_linier', 2)

		UNION ALL

		SELECT
			omraade,
			element,
			maengde_besk,
			maengde,
			enhedspris,
			pris
		FROM greg.tbl_ext('greg', 'v_his_punkter', 2)
	)

SELECT
	'Udtræk pr. ' || to_char(filter._value('historik')::timestamp, 'DD-MM-YYYY HH24:MI:SS') || E'\n' || (SELECT filter.label()) AS filter,
	*
FROM cte1
ORDER BY 2,3;


-- DROP VIEW IF EXISTS greg.v_atlas CASCADE;

CREATE VIEW greg.v_atlas AS

WITH

	cte1 AS(
		SELECT
			pg_distrikt_nr AS id,
			a.geometri,
			a.pg_distrikt_nr,
			NULL::int AS part,
			a.omraade,
			a.pg_distrikt_type,
			a.vejnavn,
			a.vejnr,
			a.postdistrikt
		FROM basis.v_omraader a
		WHERE a.synlig AND a.aktiv

		UNION ALL

		SELECT
			id + (SELECT MAX(pg_distrikt_nr) FROM basis.omraader) AS id,
			a.geometri,
			a.pg_distrikt_nr,
			a.part,
			a.omraade,
			a.pg_distrikt_type,
			a.vejnavn,
			a.vejnr,
			a.postdistrikt
		FROM basis.v_delomraader a
		LEFT JOIN basis.v_omraader b ON a.pg_distrikt_nr = b.pg_distrikt_nr
		WHERE b.aktiv
	)

SELECT
	a.id,
	a.geometri,
	a.pg_distrikt_nr,
	a.omraade || COALESCE(' (' || a.part || '/' || (SELECT MAX(b.part) FROM cte1 b WHERE b.pg_distrikt_nr = a.pg_distrikt_nr) || ')', '') AS omraade,
	a.pg_distrikt_type,
	COALESCE(a.vejnavn || ' ' || a.vejnr || ', ', a.vejnavn || ', ', '') || a.postdistrikt AS adresse,
	custom.frame_scale(a.geometri, 50, 213, 201, 5) AS scale_a4,
	custom.frame_scale(a.geometri, 50, 335, 283, 5) AS scale_a3
FROM cte1 a
ORDER BY pg_distrikt_nr, part;


/*-- DROP VIEW IF EXISTS greg.v_atlas_mgd CASCADE;

CREATE VIEW greg.v_atlas_mgd AS

WITH

	cte1 AS(
		SELECT
			1 AS _order,
			pg_distrikt_nr,
			element,
			TO_CHAR(maengde,'FM999G999G990')  || ' stk' AS maengde
		FROM greg.tbl('greg', 'v_cur_punkter', 1)

		UNION ALL

		SELECT
			2 AS _order,
			pg_distrikt_nr,
			element,
			TO_CHAR(maengde,'FM999G999G990')  || ' lbm' AS maengde
		FROM greg.tbl('greg', 'v_cur_linier', 1)

		UNION ALL

		SELECT
			3 AS _order,
			pg_distrikt_nr,
			element,
			CASE
				WHEN maengde::int = 0
				THEN TO_CHAR(maengde,'FM999G999G990')
				ELSE TO_CHAR(maengde::int,'FM999G999G990')
			END || ' m²' AS maengde
		FROM greg.tbl('greg', 'v_cur_flader', 1)
	),

	cte2 AS(
		SELECT
			pg_distrikt_nr,
			string_agg('<tr><td>' || element || COALESCE(', ' || maengde, '') || '</td></tr>', E'\n' ORDER BY _order, element) AS _body
		FROM cte1
		GROUP BY pg_distrikt_nr
	),
	
	cte3 AS(
		SELECT
			pg_distrikt_nr,
			FORMAT(
			$$
<!DOCTYPE html>
<html>
<body>
<table style="width:100%%">
%s
</table>
</body>
</html>

			$$, _body) AS _html
		FROM cte2
	)

SELECT
	a.geometri,
	a.pg_distrikt_nr,
	a.omraade,
	a.pg_distrikt_type,
	COALESCE(a.vejnavn || ' ' || a.vejnr || ', ', a.vejnavn || ', ', '') || a.postdistrikt AS adresse,
	custom.frame_scale(a.geometri, 50, 200, 254.1, 5) AS scale_a4,
	custom.frame_scale(a.geometri, 50, 200, 254.1, 5) AS scale_a3,
	b._html
FROM basis.v_omraader a
LEFT JOIN cte3 b ON a.pg_distrikt_nr = b.pg_distrikt_nr;*/


--
-- SCHEMA skitse
--

CREATE SCHEMA skitse;
COMMENT ON SCHEMA skitse IS 'Skitselag.';


--
-- TABLES
--


-- DROP TABLE IF EXISTS skitse.flader CASCADE;

CREATE TABLE skitse.flader(
	id serial NOT NULL,
	geometri public.geometry('MultiPolygon', 25832) NOT NULL,
	note character varying(256),
	CONSTRAINT flader_pk PRIMARY KEY (id) WITH (fillfactor='10'),
	CONSTRAINT flader_ck_geometri CHECK (public.ST_IsValid(geometri) IS TRUE AND public.ST_IsEmpty(geometri) IS FALSE)
);


-- DROP TABLE IF EXISTS skitse.linier CASCADE;

CREATE TABLE skitse.linier(
	id serial NOT NULL,
	geometri public.geometry('MultiLineString', 25832) NOT NULL,
	note character varying(256),
	CONSTRAINT linier_pk PRIMARY KEY (id) WITH (fillfactor='10'),
	CONSTRAINT linier_ck_geometri CHECK (public.ST_IsEmpty(geometri) IS FALSE)
);


-- DROP TABLE IF EXISTS skitse.punkter CASCADE;

CREATE TABLE skitse.punkter(
	id serial NOT NULL,
	geometri public.geometry('MultiPoint', 25832) NOT NULL,
	note character varying(256),
	CONSTRAINT punkter_pk PRIMARY KEY (id) WITH (fillfactor='10'),
	CONSTRAINT punkter_ck_geometri CHECK (public.ST_IsEmpty(geometri) IS FALSE)
);


--
-- ADMIN
--

DO

$BODY$

	DECLARE

		role text;

	BEGIN

		role := current_database() || '_admin';


		IF NOT EXISTS (SELECT '1' FROM pg_catalog.pg_roles WHERE rolname = role) THEN

			EXECUTE format('CREATE ROLE %s PASSWORD ''123'' NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION', role);

		END IF;

		EXECUTE FORMAT(
			$$
				GRANT CONNECT
					ON DATABASE %1$s
					TO %2$s
				;

				GRANT USAGE
					ON SCHEMA
						basis,
						custom,
						filter,
						greg,
						grunddata,
						roles,
						styles,
						public,
						skitse
					TO %2$s
				;

				GRANT SELECT
					ON ALL TABLES IN SCHEMA
						basis,
						custom,
						filter,
						greg,
						grunddata,
						roles,
						styles,
						public,
						skitse
					TO %2$s
				;

				ALTER DEFAULT PRIVILEGES
					IN SCHEMA
						basis,
						custom,
						filter,
						greg,
						grunddata,
						roles,
						styles,
						public,
						skitse
					GRANT SELECT ON TABLES
					TO %2$s
				;

				GRANT INSERT, UPDATE
					ON TABLE filter.v_settings -- Only view for own data
					TO %2$s
				;

				GRANT EXECUTE
					ON ALL FUNCTIONS IN SCHEMA
						basis,
						custom,
						filter,
						greg,
						grunddata,
						roles,
						styles,
						public,
						skitse
					TO %2$s
				;

				ALTER DEFAULT PRIVILEGES
					IN SCHEMA
						basis,
						custom,
						filter,
						greg,
						grunddata,
						roles,
						styles,
						public,
						skitse
					GRANT EXECUTE ON FUNCTIONS
					TO %2$s
				;

				GRANT INSERT,UPDATE,DELETE
					ON ALL TABLES IN SCHEMA
						basis,
						custom,
						filter,
						greg,
						grunddata,
						roles,
						styles,
						public,
						skitse
					TO %2$s
				;

				ALTER DEFAULT PRIVILEGES
					IN SCHEMA
						basis,
						custom,
						filter,
						greg,
						grunddata,
						roles,
						styles,
						public,
						skitse
					GRANT INSERT,UPDATE,DELETE ON TABLES
					TO %2$s
				;

				GRANT ALL
					ON ALL SEQUENCES IN SCHEMA
						basis,
						custom,
						filter,
						greg,
						grunddata,
						roles,
						styles,
						public,
						skitse
					TO %2$s
				;

				ALTER DEFAULT PRIVILEGES
					IN SCHEMA
						basis,
						custom,
						filter,
						greg,
						grunddata,
						roles,
						styles,
						public,
						skitse
					GRANT ALL ON SEQUENCES
					TO %2$s
				;
			$$,
			current_database(), role
		);

	END

$BODY$;


--
-- READER
--

DO

$BODY$

	DECLARE

		role text;

	BEGIN

		role := current_database() || '_reader';


		IF NOT EXISTS (SELECT '1' FROM pg_catalog.pg_roles WHERE rolname = role) THEN

			EXECUTE format('CREATE ROLE %s PASSWORD ''123'' NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION', role);

		END IF;

		EXECUTE FORMAT(
			$$
				GRANT CONNECT
					ON DATABASE %1$s
					TO %2$s
				;

				GRANT USAGE
					ON SCHEMA
						basis,
						custom,
						filter,
						greg,
						grunddata,
						roles,
						styles,
						public,
						skitse
					TO %2$s
				;

				GRANT SELECT
					ON ALL TABLES IN SCHEMA
						basis,
						custom,
						filter,
						greg,
						grunddata,
						roles,
						styles,
						public,
						skitse
					TO %2$s
				;

				ALTER DEFAULT PRIVILEGES
					IN SCHEMA
						basis,
						custom,
						filter,
						greg,
						grunddata,
						roles,
						styles,
						public,
						skitse
					GRANT SELECT ON TABLES
					TO %2$s
				;

				GRANT INSERT, UPDATE
					ON TABLE filter.v_settings -- Only view for own data
					TO %2$s
				;

				GRANT EXECUTE
					ON ALL FUNCTIONS IN SCHEMA
						basis,
						custom,
						filter,
						greg,
						grunddata,
						roles,
						styles,
						public,
						skitse
					TO %2$s
				;

				ALTER DEFAULT PRIVILEGES
					IN SCHEMA
						basis,
						custom,
						filter,
						greg,
						grunddata,
						roles,
						styles,
						public,
						skitse
					GRANT EXECUTE ON FUNCTIONS
					TO %2$s
				;

				GRANT INSERT,UPDATE,DELETE
					ON ALL TABLES IN SCHEMA
						skitse
					TO %2$s
				;

				ALTER DEFAULT PRIVILEGES
					IN SCHEMA
						skitse
					GRANT INSERT,UPDATE,DELETE ON TABLES
					TO %2$s
				;

				GRANT ALL
					ON ALL SEQUENCES IN SCHEMA
						skitse
					TO %2$s
				;

				ALTER DEFAULT PRIVILEGES
					IN SCHEMA
						skitse
					GRANT ALL ON SEQUENCES
					TO %2$s
			$$,
			current_database(), role
		);

	END

$BODY$;


--
-- WRITER
--

DO

$BODY$

	DECLARE

		role text;

	BEGIN

		role := current_database() || '_writer';


		IF NOT EXISTS (SELECT '1' FROM pg_catalog.pg_roles WHERE rolname = role) THEN

			EXECUTE format('CREATE ROLE %s PASSWORD ''123'' NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION', role);

		END IF;

		EXECUTE FORMAT(
			$$
				GRANT CONNECT
					ON DATABASE %1$s
					TO %2$s
				;

				GRANT USAGE
					ON SCHEMA
						basis,
						custom,
						filter,
						greg,
						grunddata,
						roles,
						styles,
						public,
						skitse
					TO %2$s
				;

				GRANT SELECT
					ON ALL TABLES IN SCHEMA
						basis,
						custom,
						filter,
						greg,
						grunddata,
						roles,
						styles,
						public,
						skitse
					TO %2$s
				;

				ALTER DEFAULT PRIVILEGES
					IN SCHEMA
						basis,
						custom,
						filter,
						greg,
						grunddata,
						roles,
						styles,
						public,
						skitse
					GRANT SELECT ON TABLES
					TO %2$s
				;

				GRANT INSERT, UPDATE
					ON TABLE filter.v_settings -- Only view for own data
					TO %2$s
				;

				GRANT EXECUTE
					ON ALL FUNCTIONS IN SCHEMA
						basis,
						custom,
						filter,
						greg,
						grunddata,
						roles,
						styles,
						public,
						skitse
					TO %2$s
				;

				ALTER DEFAULT PRIVILEGES
					IN SCHEMA
						basis,
						custom,
						filter,
						greg,
						grunddata,
						roles,
						styles,
						public,
						skitse
					GRANT EXECUTE ON FUNCTIONS
					TO %2$s
				;

				GRANT INSERT,UPDATE,DELETE
					ON ALL TABLES IN SCHEMA
						greg,
						grunddata,
						skitse
					TO %2$s
				;

				ALTER DEFAULT PRIVILEGES
					IN SCHEMA
						greg,
						grunddata,
						skitse
					GRANT INSERT,UPDATE,DELETE ON TABLES
					TO %2$s
				;

				GRANT ALL
					ON ALL SEQUENCES IN SCHEMA
						greg,
						grunddata,
						skitse
					TO %2$s
				;

				ALTER DEFAULT PRIVILEGES
					IN SCHEMA
						greg,
						grunddata,
						skitse
					GRANT ALL ON SEQUENCES
					TO %2$s
				;
			$$,
			current_database(), role
		);

	END

$BODY$;


