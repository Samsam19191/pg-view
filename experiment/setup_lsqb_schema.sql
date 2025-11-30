-- LSQB Schema Setup for pgview_lsqb database
-- Based on the pg-view repository schema

\echo [INFO] Create tables (Start)

-- Metadata tables
CREATE TABLE IF NOT EXISTS N_schema (_0 VARCHAR(1024));
CREATE TABLE IF NOT EXISTS E_schema (_0 VARCHAR(1024), _1 VARCHAR(1024), _2 VARCHAR(1024));
CREATE TABLE IF NOT EXISTS EGD (_0 VARCHAR(1024));
CREATE TABLE IF NOT EXISTS CATALOG_VIEW (_0 VARCHAR(1024), _1 VARCHAR(1024), _2 VARCHAR(1024), _3 VARCHAR(1024), _4 INT DEFAULT 0);
CREATE TABLE IF NOT EXISTS CATALOG_INDEX (_0 VARCHAR(1024), _1 VARCHAR(1024), _2 VARCHAR(1024));
CREATE TABLE IF NOT EXISTS CATALOG_SINDEX (_0 VARCHAR(1024), _1 VARCHAR(1024));

-- Node table: N_g(nid, label)
CREATE TABLE IF NOT EXISTS N_g (
    _0 INT DEFAULT 0, 
    _1 VARCHAR(1024),
    PRIMARY KEY(_0)
);

-- Edge table: E_g(eid, from_node, to_node, label)
CREATE TABLE IF NOT EXISTS E_g (
    _0 INT DEFAULT 0, 
    _1 INT DEFAULT 0, 
    _2 INT DEFAULT 0, 
    _3 VARCHAR(1024),
    PRIMARY KEY(_0)
);

-- Node Properties table: NP_g(nid, property, value)
CREATE TABLE IF NOT EXISTS NP_g (_0 INT DEFAULT 0, _1 VARCHAR(1024), _2 VARCHAR(1024));

-- Edge Properties table: EP_g(eid, property, value)
CREATE TABLE IF NOT EXISTS EP_g (_0 INT DEFAULT 0, _1 VARCHAR(1024), _2 VARCHAR(1024));

\echo [INFO] Create tables (End)

-- Create indexes for performance
\echo [INFO] Create indexes (Start)
CREATE INDEX IF NOT EXISTS N_g___1 ON N_g (_1);
CREATE INDEX IF NOT EXISTS E_g___1 ON E_g (_1);
CREATE INDEX IF NOT EXISTS E_g___2 ON E_g (_2);
CREATE INDEX IF NOT EXISTS E_g___3 ON E_g (_3);
\echo [INFO] Create indexes (End)
