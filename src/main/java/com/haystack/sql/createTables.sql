-- Table: gp_query_log

DROP TABLE IF EXISTS haystack.run_log cascade;
DROP TABLE IF EXISTS haystack.dbconnections cascade;
DROP TABLE IF EXISTS haystack.query_log cascade;
drop table if exists haystack.tables cascade;
DROP TABLE if exists haystack.columns cascade;
DROP TABLE IF EXISTS haystack.users CASCADE;
DROP TABLE IF EXISTS haystack.gpsd CASCADE;

CREATE TABLE haystack.users
(
  userid       TEXT,
  password   TEXT,
  organization TEXT,
  createdate TIMESTAMP,
  lastlogin  TIMESTAMP,
  CONSTRAINT user_pkey PRIMARY KEY (userid)
) WITH (OIDS =FALSE
);


CREATE TABLE haystack.gpsd
(
  userid       text,
  dbname       text,
  seqkey       integer,
  filename     text,
  gpsd_db      text,
  gpsd_date    date,
  gpsd_params  text,
  gpsd_version text,
  noOflines    bigint
)
WITH (
OIDS =FALSE
)
  DISTRIBUTED BY (userid);



CREATE TABLE haystack.run_log
(
  run_id     integer NOT NULL,
  run_date timestamp without time zone,
  run_user   text,
  run_db text,
  model_json text,
  CONSTRAINT run_log_pkey PRIMARY KEY (run_id),
  CONSTRAINT run_log_run_user_fkey FOREIGN KEY (run_user)
  REFERENCES haystack.users (userid) MATCH SIMPLE
  ON UPDATE NO ACTION ON DELETE NO ACTION
)
WITH (
OIDS=FALSE
)
  DISTRIBUTED BY (run_id);


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
  run_id INTEGER REFERENCES haystack.run_log,
  logsession  TEXT,
  logcmdcount TEXT,
  logdatabase TEXT,
  loguser     TEXT,
  logpid      TEXT,
  logtimemin  TIMESTAMP WITH TIME ZONE,
  logtimemax  TIMESTAMP WITH TIME ZONE,
  logduration INTERVAL,
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


