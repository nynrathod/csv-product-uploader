-- Runs once on first boot, as admin. Creates the two logical databases and
-- locks each service role so it can ONLY connect to its own database and
-- can ONLY create objects there. This is the enforced database-per-service
-- boundary.

CREATE DATABASE import_db;
CREATE DATABASE catalog_db;

CREATE ROLE importer_svc LOGIN PASSWORD 'importer_dev';
CREATE ROLE catalog_svc  LOGIN PASSWORD 'catalog_dev';

REVOKE CONNECT ON DATABASE import_db FROM PUBLIC;
GRANT  CONNECT ON DATABASE import_db TO importer_svc;

REVOKE CONNECT ON DATABASE catalog_db FROM PUBLIC;
GRANT  CONNECT ON DATABASE catalog_db TO catalog_svc;

\connect import_db
GRANT ALL ON SCHEMA public TO importer_svc;

\connect catalog_db
GRANT ALL ON SCHEMA public TO catalog_svc;