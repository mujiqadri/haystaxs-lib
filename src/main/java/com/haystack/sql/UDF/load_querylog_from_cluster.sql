-- Function: haystack_ui.load_querylog_from_cluster(text, text, integer)

-- DROP FUNCTION haystack_ui.load_querylog_from_cluster(text, text, integer);

CREATE OR REPLACE FUNCTION haystack_ui.load_querylog_from_cluster(haystackschema TEXT, userschema TEXT, queryid INTEGER)
  RETURNS VOID AS
  $BODY$
  DECLARE
    mycommand     TEXT;
    sql           TEXT;
    tbl_exists    BOOLEAN;
    result        BOOLEAN;
    recCount      INT;
    rec           RECORD;
    ext_tblname   TEXT := '__gp_log_master_ext';
    qrylogtblname TEXT := 'queries';
  BEGIN -- outer function wrapper
    SET AUTOCOMMIT = ON;

    sql := 'set search_path = ' || haystackSchema || ',' || userSchema || ', public;';
    RAISE NOTICE 'Setting Searchpath, SQL:%', sql;
    EXECUTE sql;


    SELECT create_ast_tables(userschema, FALSE);
-- Create QueryLog Table if it doesn't exist

    SELECT count(*) > 0
    INTO tbl_exists
    FROM information_schema.tables
    WHERE upper(table_schema) = upper(userSchema) AND upper(table_name) = upper(qryLogTblName);

    RAISE NOTICE 'TABLE EXISTS:%', tbl_exists;
    IF tbl_exists = 'f'
    THEN
      RAISE INFO '%.% Doesnot exist, creating table', userSchema, qryLogTblName;
      mycommand := 'CREATE TABLE ' || userSchema || '.' || qryLogTblName || '
				(
				  id serial,
				  logsession text,
				  logcmdcount text,
				  logdatabase text,
				  loguser text,
				  logpid text,
				  logsessiontime timestamp with time zone,
				  logtimemin timestamp with time zone,
				  logtimemax timestamp with time zone,
				  logduration interval,
				  sql text,
				  qrytype text
				)
				WITH (APPENDONLY=true, COMPRESSTYPE=quicklz,
				  OIDS=FALSE
				)
				DISTRIBUTED BY (id)
				PARTITION BY RANGE(logsessiontime) ( START (date ''1900-01-01'') INCLUSIVE END ( date ''1900-01-02'') EXCLUSIVE
				EVERY (INTERVAL ''1 day''));';

      RAISE INFO 'SQL [%]', mycommand;
      EXECUTE mycommand;
    END IF;

-- CREATE PARTITION - find the distinct dates from the external table to create partitions
    sql := 'SELECT logtime::date FROM ' || 'gp_toolkit' || '.' || ext_TblName || ' group by logtime::date';
    RAISE NOTICE 'sql %', sql;
    FOR rec IN EXECUTE sql LOOP
      RAISE NOTICE '--> MONTH %', rec.logtime;
      SELECT create_month_partition(lower(userSchema), lower(qryLogTblName), rec.logtime)
      INTO result;
      IF result = 'f'
      THEN RAISE NOTICE 'Partition Already Exists';
      ELSE RAISE NOTICE 'Partition Created';
      END IF;
      mycommand :='SELECT COUNT(*) FROM ' || haystackSchema || '.query_log_dates where query_log_id=' || queryID ||
                  ' AND log_date = ''' || rec.logtime || ''';';
      EXECUTE mycommand
      INTO recCount;
      IF (recCount = 0)
      THEN
        mycommand :=
        'INSERT INTO ' || haystackSchema || '.query_log_dates(query_log_id,log_date) values(' || queryID || ',''' ||
        rec.logtime || ''');';
        EXECUTE mycommand;
        RAISE NOTICE 'Inserted Date:% into %.Query_Log_Dates', rec.logtime, haystackSchema;
      ELSE
        RAISE NOTICE 'Found Date:% into %.Query_Log_Dates', rec.logtime, haystackSchema;
      END IF;
    END LOOP;


-- LOAD Queries calculate duration
    sql := 'INSERT INTO ' || userSchema || '.' || qryLogTblName || '(logsession, logcmdcount,logdatabase, loguser, logpid, logsessiontime, logtimemin, logtimemax, logduration, sql)
		 SELECT A.logsession, A.logcmdcount, A.logdatabase, A.loguser, A.logpid, min(A.logtime) logsessiontime, min(A.logtime) AS logtimemin,
                 max(A.logtime) AS logtimemax, max(A.logtime) - min(A.logtime) AS logduration, min(logdebug) as sql
		FROM  ' || 'gp_toolkit' || '.' || ext_TblName || ' A
		WHERE A.logsession IS NOT NULL AND A.logcmdcount IS NOT NULL AND A.logdatabase IS NOT NULL
		GROUP BY A.logsession, A.logcmdcount, A.logdatabase, A.loguser, A.logpid
		HAVING length(min(logdebug)) > 0;';
    RAISE INFO 'INSERTING QUERIES %.%', userSchema, qryLogTblName;
    EXECUTE sql;

--Categorizing the queries by type

    sql := 'UPDATE ' || userSchema || '.' || qryLogTblName || '
		SET QRYTYPE = case
	   	        when upper(sql) like ''%SET%'' THEN ''SET CONFIGURATION''
			when upper(sql) like ''%SELECT%'' THEN ''SELECT''
			when upper(sql) like ''%INSERT INTO%'' THEN ''INSERT''
			when upper(sql) like ''%COMMIT%'' THEN ''COMMIT''
			when upper(sql) like ''%SELECT%FROM%GPTEXT.SEARCH_COUNT%'' THEN ''GPTEXT.SEARCH_COUNT''
			when upper(sql) like ''%SELECT%FROM%GPTEXT.INDEX%'' THEN ''GPTEXT.INDEX''
			when upper(sql) like ''%SELECT%FROM%GPTEXT.INDEX_STATISTICS%'' THEN ''GPTEXT.IDX_STATS''
			when upper(sql) like ''%DROP TABLE%'' THEN ''DROP TABLE''
			when upper(sql) like ''%BEGIN WORK%LOCK TABLE%'' THEN ''EXCLUSIVE LOCK''
			when upper(sql) like ''%CREATE TABLE%'' THEN ''CREATE TABLE''
			when upper(sql) like ''%DROP TABLE%'' THEN ''DROP TABLE''
			when upper(sql) like ''%TRUNCATE%'' THEN ''TRUNCATE TABLE''
			when sql like ''unlisten *'' THEN ''INTERNAL''
			when upper(sql) like ''%UPDATE%'' THEN ''UPDATE''
			when upper(sql) like ''%CREATE%EXTERNAL%TABLE%'' THEN ''CREATE EXTERNAL TABLE''
			when upper(sql) like ''%DELETE%FROM%'' THEN ''DELETE''
			when upper(sql) like ''%BEGIN%'' THEN ''TRANSACTION-OPERATION''
			when upper(sql) like ''%ROLLBACK%'' THEN ''TRANSACTION-OPERATION''
			when upper(sql) like ''%SAVEPOINT%'' THEN ''TRANSACTION-OPERATION''
			when upper(sql) like ''%RELEASE%'' THEN ''TRANSACTION-OPERATION''
			when upper(sql) like ''%TRANSACTION%'' THEN  ''TRANSACTION-OPERATION''
			when upper(sql) like ''%SHOW%'' THEN ''SHOW''
			when sql like ''%;%'' THEN ''MULTIPLE SQL STATEMENTS''
		else ''OTHERS''
	end;';
    RAISE INFO 'CATEGORIZING QUERIES %.%', userSchema, qryLogTblName;
    EXECUTE sql;

-- Update Query Count and Sum Duration for Each Date for this QueryLogId
    sql := 'UPDATE ' || haystackSchema || '.query_log_dates
		set query_count = X.query_count, sum_duration = X.sum_duration
		FROM (select logsessiontime::date as log_date,count(*) as query_count, EXTRACT(EPOCH FROM sum(logduration)) as sum_duration
		from ' || userSchema || '.' || qryLogTblName || '
		group by logsessiontime::date ) as X
		where query_log_dates.log_date = X.log_date
		and query_log_id = ' || queryID || ';';
    RAISE INFO 'UPDATE QUERY_LOG_DATE STATS';
    EXECUTE sql;

-- Recreate query_metadata table
    sql:= 'DROP TABLE IF EXISTS ' || userSchema || '.query_metadata;';
    EXECUTE sql;

    sql := 'CREATE TABLE ' || userSchema ||
           '.query_metadata( type text, value text ) WITH ( OIDS=FALSE ) DISTRIBUTED BY (type);';
    EXECUTE sql;

    sql := 'INSERT INTO ' || userSchema || '.query_metadata ( type, value) SELECT distinct ''dbname'', logdatabase ' ||
           ' FROM ' || userSchema || '.queries;';
    EXECUTE sql;

    sql := 'INSERT INTO ' || userSchema || '.query_metadata ( type, value) SELECT distinct ''username'', loguser ' ||
           ' FROM ' || userSchema || '.queries;';
    EXECUTE sql;

    RAISE NOTICE 'Complete';

  END;
  -- outer function wrapper
  $BODY$
LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION haystack_ui.load_querylog_from_cluster( TEXT, TEXT, INTEGER )
OWNER TO gpadmin;
