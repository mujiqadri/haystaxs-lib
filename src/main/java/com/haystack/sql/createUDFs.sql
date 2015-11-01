-- Function: haystack_ui.load_querylog(text, text, text, integer)
--SELECT haystack_ui.load_querylog('haystack_ui','mujtaba_dot_qadri_at_gmail_dot_com','Queries','ext_7',7);

-- DROP FUNCTION haystack_ui.load_querylog(text, text, text, integer);

CREATE OR REPLACE FUNCTION haystack_ui.load_querylog(haystackSchema text, userSchema text, qrylogtblname text, ext_tblname text, queryid integer)
  RETURNS void AS
$BODY$
DECLARE
	    mycommand TEXT;
	    sql TEXT;
	    tbl_exists BOOLEAN;
	    result BOOLEAN;
	    recCount int;
	    rec     RECORD;
BEGIN -- outer function wrapper
	SET AUTOCOMMIT = ON;
	sql := 'set search_path = ' || haystackSchema || ',' || userSchema || ', public;';
	RAISE NOTICE 'Setting Searchpath, SQL:%', sql;
	EXECUTE sql;
	-- Create QueryLog Table if it doesn't exist

	select count(*) > 0 INTO tbl_exists
	from information_schema.tables
	where upper(table_schema) = upper(userSchema) and upper(table_name) = upper(qryLogTblName);

	RAISE NOTICE 'TABLE EXISTS:%', tbl_exists;
	IF tbl_exists = 'f' THEN
		RAISE INFO '%.% Doesnot exist, creating table', userSchema, qryLogTblName ;
		mycommand := 'CREATE TABLE ' || userSchema || '.' || qryLogTblName || '
				(
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
				DISTRIBUTED BY (logsession)
				PARTITION BY RANGE(logsessiontime) ( START (date ''1900-01-01'') INCLUSIVE END ( date ''1900-01-02'') EXCLUSIVE
				EVERY (INTERVAL ''1 day''));';

		RAISE INFO 'SQL [%]', mycommand;
		EXECUTE mycommand;
	END IF;

	-- CREATE PARTITION - find the distinct dates from the external table to create partitions
        sql := 'SELECT logtime::date FROM ' || userSchema || '.'  || ext_TblName || ' group by logtime::date';
        raise notice 'sql %',sql;
         for rec in execute sql loop
                 RAISE NOTICE '--> MONTH %', rec.logtime ;
                 SELECT create_month_partition(lower(userSchema),lower(qryLogTblName),rec.logtime) INTO result;
                 IF result = 'f'
			THEN RAISE NOTICE 'Partition Already Exists';
			ELSE RAISE NOTICE 'Partition Created';
                 END IF;
                 mycommand := 'SELECT COUNT(*) FROM ' || haystackSchema || '.query_log_dates where query_log_id=' || queryID || ' AND log_date = '''|| rec.logtime || ''';';
                 EXECUTE mycommand INTO recCount;
                 if (recCount = 0) THEN
			mycommand := 'INSERT INTO ' || haystackSchema || '.query_log_dates(query_log_id,log_date) values(' || queryID || ',''' || rec.logtime || ''');';
			EXECUTE mycommand;
			RAISE NOTICE 'Inserted Date:% into %.Query_Log_Dates', rec.logtime, haystackSchema;
		 else
			RAISE NOTICE 'Found Date:% into %.Query_Log_Dates', rec.logtime, haystackSchema;
		 end if;
         end loop;


	-- LOAD Queries calculate duration
	sql := 'INSERT INTO ' || userSchema || '.' || qryLogTblName || '(logsession, logcmdcount,logdatabase, loguser, logpid, logsessiontime, logtimemin, logtimemax, logduration, sql)
		 SELECT A.logsession, A.logcmdcount, A.logdatabase, A.loguser, A.logpid, min(A.logtime) logsessiontime, min(A.logtime) AS logtimemin,
                 max(A.logtime) AS logtimemax, max(A.logtime) - min(A.logtime) AS logduration, min(logdebug) as sql
		FROM  ' || userSchema || '.' || ext_TblName || ' A
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

         RAISE NOTICE 'Complete';

END; -- outer function wrapper
$BODY$
  LANGUAGE plpgsql VOLATILE;





  ---=============


CREATE OR REPLACE FUNCTION haystack.create_month_partition(schema_name text, table_name text, monthpartition_name text) RETURNS boolean
    AS $_$
DECLARE
    mycommand TEXT;
    monthpartition_exists BOOLEAN;

BEGIN
	-- Find how many days in the current month
	-- query to see if the target partition already exists

	SELECT count(*) > 0 INTO monthpartition_exists FROM pg_partitions
	WHERE partitionname = monthpartition_name
		AND tablename = table_name
		AND schemaname = schema_name;

	-- if the target partition does not exist create it and return

	IF monthpartition_exists = 't' THEN
		RAISE INFO 'Month Partition Already Exists';
		RETURN FALSE;
	END IF;

	-- if the target partition does not exist create it and return
	IF monthpartition_exists = 'f' THEN

		mycommand := 'ALTER TABLE ' || schema_name || '.' || table_name || ' ADD PARTITION "' || $3 || '" START (''' || $3 || ' 00:00:00.000'') '
		 || ' INCLUSIVE END (''' || $3 || ' 23:59:59.999'') EXCLUSIVE;';
		RAISE INFO 'Month partition does not exist.  Creating partition now using [%]', mycommand;
		EXECUTE mycommand;

	END IF;

	RETURN TRUE;

END;

$_$
    LANGUAGE plpgsql;
