
CREATE CATALOG IF NOT EXISTS crypto;
USE CATALOG crypto;
CREATE SCHEMA IF NOT EXISTS core;
USE crypto.core;

-- a unified landing location for files
CREATE VOLUME IF NOT EXISTS crypto.core.landing;
