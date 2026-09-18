-- Runs ONCE on first boot, as admin. Creates the two logical databases and
-- locks each service role so it can ONLY connect to its own database.
-- This is the enforced database-per-service boundary.

CREATE DATABASE import_db;
CREATE DATABASE catalog_db;

CREATE ROLE importer_svc LOGIN PASSWORD 'importer_dev';
CREATE ROLE catalog_svc  LOGIN PASSWORD 'catalog_dev';

-- importer may ONLY reach import_db
REVOKE CONNECT ON DATABASE import_db FROM PUBLIC;
GRANT  CONNECT ON DATABASE import_db TO importer_svc;

-- catalog-worker may ONLY reach catalog_db
REVOKE CONNECT ON DATABASE catalog_db FROM PUBLIC;
GRANT  CONNECT ON DATABASE catalog_db TO catalog_svc;