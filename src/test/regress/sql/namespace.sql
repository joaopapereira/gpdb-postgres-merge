--
-- Regression tests for schemas (namespaces)
--

CREATE SCHEMA test_schema_1
       CREATE INDEX abc_a_idx ON abc (a)

       CREATE VIEW abc_view AS
              SELECT a+1 AS a, b+1 AS b FROM abc

       CREATE TABLE abc (
              a serial,
              b int UNIQUE
       ) DISTRIBUTED BY (b);

-- verify that the objects were created
SELECT COUNT(*) FROM pg_class WHERE relnamespace =
    (SELECT oid FROM pg_namespace WHERE nspname = 'test_schema_1');

INSERT INTO test_schema_1.abc DEFAULT VALUES;
INSERT INTO test_schema_1.abc DEFAULT VALUES;
INSERT INTO test_schema_1.abc DEFAULT VALUES;

SELECT * FROM test_schema_1.abc;
SELECT * FROM test_schema_1.abc_view;

ALTER SCHEMA test_schema_1 RENAME TO test_schema_renamed;
SELECT COUNT(*) FROM pg_class WHERE relnamespace =
    (SELECT oid FROM pg_namespace WHERE nspname = 'test_schema_1');

-- test IF NOT EXISTS cases
CREATE SCHEMA test_schema_renamed; -- fail, already exists
CREATE SCHEMA IF NOT EXISTS test_schema_renamed; -- ok with notice
CREATE SCHEMA IF NOT EXISTS test_schema_renamed -- fail, disallowed
       CREATE TABLE abc (
              a serial,
              b int UNIQUE
       );

<<<<<<< HEAD
-- Test GRANT/REVOKE 
CREATE SCHEMA test_schema_2;
CREATE TABLE test_schema_2.abc as select * from test_schema_1.abc DISTRIBUTED BY (a);
create role tmp_test_schema_role RESOURCE QUEUE pg_default;
GRANT ALL ON SCHEMA test_schema_1 to tmp_test_schema_role;

SET SESSION AUTHORIZATION tmp_test_schema_role;
CREATE TABLE test_schema_1.grant_test(a int) DISTRIBUTED BY (a);
DROP TABLE test_schema_1.grant_test;
CREATE TABLE test_schema_2.grant_test(a int) DISTRIBUTED BY (a); -- no permissions on schema
SELECT * FROM test_schema_1.abc; -- no permissions on table
SELECT * FROM test_schema_2.abc; -- no permissions on schema
ALTER SCHEMA test_schema_1 RENAME to myschema;  -- not the schema owner

RESET SESSION AUTHORIZATION;
DROP TABLE test_schema_2.abc;
DROP SCHEMA test_schema_2;

-- ALTER SCHEMA .. OWNER TO
ALTER SCHEMA pg_toast OWNER to tmp_test_schema_role; -- system schema
alter schema test_schema_1 owner to tmp_test_schema_role;
select rolname from pg_authid a, pg_namespace n where a.oid = n.nspowner
  and nspname = 'test_schema_1';

-- test CREATE SCHEMA/ALTER SCHEMA for reserved names
CREATE SCHEMA pg_schema; -- reserved name
CREATE SCHEMA gp_schema; -- reserved name
ALTER SCHEMA test_schema_1 RENAME to pg_schema; -- reseved name
ALTER SCHEMA test_schema_1 RENAME to gp_schema; -- reserved name
ALTER SCHEMA pg_toast RENAME to bread;  -- system schema

-- RENAME to a valid new name
ALTER SCHEMA test_schema_1 RENAME to test_schema_2;

-- Check that ALTER statements dispatched correctly
select * 
FROM gp_dist_random('pg_namespace') n1 
  full outer join pg_namespace n2 on (n1.oid = n2.oid)
WHERE n1.nspname != n2.nspname or n1.nspowner != n2.nspowner or
      n1.nspname is null or n2.nspname is null;

-- DROP SCHEMA
DROP SCHEMA pg_toast;       -- system schema
DROP SCHEMA test_schema_1;  -- does not exist
DROP SCHEMA test_schema_2;  -- contains objects
DROP SCHEMA test_schema_2 CASCADE;
DROP ROLE tmp_test_schema_role;

-- verify that the objects were dropped
SELECT nspname, relname
FROM pg_class c JOIN pg_namespace n ON (c.relnamespace = n.oid)
WHERE nspname ~ 'test_schema_[12]';

SELECT nspname 
FROM pg_namespace n
WHERE nspname ~ 'test_schema_[12]';

SELECT rolname
FROM pg_authid a
WHERE rolname ~ 'tmp_test_schema_role'
=======
DROP SCHEMA test_schema_renamed CASCADE;

-- verify that the objects were dropped
SELECT COUNT(*) FROM pg_class WHERE relnamespace =
    (SELECT oid FROM pg_namespace WHERE nspname = 'test_schema_renamed');
>>>>>>> ab76208e3df6841b3770edeece57d0f048392237
