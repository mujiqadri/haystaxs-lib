-- Table: gp_query_log

DROP TABLE IF EXISTS haystack.run_log cascade;
DROP TABLE IF EXISTS haystack.dbconnections cascade;
DROP TABLE IF EXISTS haystack.query_log cascade;
drop table if exists haystack.tables cascade;
DROP TABLE if exists haystack.columns cascade;
DROP TABLE IF EXISTS haystack.users CASCADE;

CREATE TABLE haystack.users
(
  userid       TEXT,
  organization TEXT,
  createdate   TEXT,
  lastlogin    TEXT,
  CONSTRAINT user_pkey PRIMARY KEY (userid)
) WITH (OIDS =FALSE
);

CREATE TABLE haystack.run_log
(
  run_id integer,
  run_date timestamp without time zone,
  run_user TEXT REFERENCES haystack.users,
  run_db text,
  run_schema text,
  CONSTRAINT run_log_pkey PRIMARY KEY (run_id)
)
WITH (
OIDS=FALSE
);

CREATE TABLE haystack.dbconnections
(
  name text NOT NULL,
  run_id INTEGER REFERENCES haystack.run_log,
  host text,
  port integer,
  databasename text,
  username text,
  password text,
  CONSTRAINT dbconnections_pkey PRIMARY KEY (name, run_id)
)
WITH (
OIDS=FALSE
);

CREATE TABLE haystack.query_log
(
  logtime     TIMESTAMP WITH TIME ZONE,
  run_id INTEGER REFERENCES haystack.run_log,
  loguser     TEXT,
  logdatabase TEXT,
  logpid      TEXT,
  logthread   TEXT,
  loghost     TEXT,
  logsegment  TEXT,
  sqltext     TEXT
)
WITH (
OIDS = FALSE
);


CREATE TABLE haystack.tables
(
  runid INTEGER REFERENCES haystack.run_log,
  table_oid integer,
  db_name text,
  schema_name text,
  table_name text,
  storage_mode text,
  noofcols integer,
  iscolumnar text,
  noofrows bigint,
  sizeinGB double precision,
  sizeinGBu double precision,
  compresstype text,
  compresslevel integer,
  compressratio integer,
  skew double precision,
  score float,
  dkarray text,
  CONSTRAINT tables_pkey PRIMARY KEY (table_oid, runid)
)
WITH (OIDS= FALSE
);

CREATE TABLE haystack.columns
(
  runid INTEGER ,
  table_oid integer ,
  column_name text,
  ordinal_position integer,
  data_type text,
  isdk boolean,
  character_maximum_length integer,
  numeric_precision integer,
  numeric_precision_radix integer,
  numeric_scale integer,
  CONSTRAINT columns_pkey PRIMARY KEY (table_oid, ordinal_position, runid),
  CONSTRAINT columns_fkey FOREIGN KEY (table_oid, runid) REFERENCES haystack.tables (table_oid, runid)
)
WITH (
OIDS=FALSE
);


