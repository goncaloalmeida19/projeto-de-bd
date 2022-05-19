CREATE DATABASE dbproj;
CREATE USER projuser PASSWORD 'projuser';

\c dbproj

\i dbproj_create_tables.sql

\i dbproj_functions.sql

\i dbproj_insert_data.sql

REVOKE CONNECT ON DATABASE dbproj FROM PUBLIC;

GRANT CONNECT ON DATABASE dbproj TO projuser;

REVOKE ALL ON ALL TABLES IN SCHEMA public FROM PUBLIC;

GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO projuser;