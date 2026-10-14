-- One PostgreSQL instance, one database per service: each service owns its schema and only
-- ever connects to its own database.
CREATE DATABASE orders;
CREATE DATABASE payments;
CREATE DATABASE ledger;
