
CREATE OR REPLACE FUNCTION haystack.capturetabledetailsgpdb(schemaname text, UserName text)
  RETURNS text AS
  $BODY$
DECLARE
    nbRows bigint;
    nbSkew float;
    rec record;

    nbSizeU float;
    nbSize float;
    nbCompRatio int;

    nboid int;
    nbStorageMode text;
    nbNoofCols int;
    nbColStore text;
    nbCompType text;
    nbCompLevel int;

    nbEstRows bigint;
    nbEstPages bigint;
    nbDK text;

    nbRunId int;
BEGIN
    -- Insert a new run_id in haystack.run_log
    SELECT COALESCE(max(run_id),0)+1
    FROM haystack.run_log INTO nbRunId;

    INSERT INTO haystack.run_log(run_id, run_date, run_user, run_db, run_schema)
    VALUES(nbRunId, now(), UserName, current_database(), schemaname);

    RAISE INFO 'RunId: %, DB: %: Schema: %',nbRunId, current_database(), schemaname;

   FOR rec IN
	select  table_schema, table_name, table_catalog
	from information_schema.tables
	where table_schema = schemaname
	and table_type = 'BASE TABLE'
	and is_insertable_into = 'YES'
	and table_name not like '%_1_prt_%'
	order by table_name
   LOOP

      RAISE INFO 'Processing table: %.%',schemaname, rec.table_name;

      -- TODO Put Skew as a separate function in the tool
      -- 1. Calculate Total Rows and Skew Variance for the table

      EXECUTE 'select round(coalesce(stddev(A.rows),0) / coalesce(sum(A.rows),1),2), coalesce(sum(A.rows),0)
		from (	select  (count(1)) as rows
			from ' || schemaname || '.' || rec.table_name ||
		' group by gp_segment_id ) as A' INTO nbSkew, nbRows;

      RAISE INFO 'Step 1: RowCount: % Skew: %',nbRows, nbSkew;
      -- 2. Get Size on Disk and Compression Ratio

      EXECUTE 'select
		to_char((sotailtablesizeuncompressed :: FLOAT8 / 1024 / 1024 / 1024),
			''FM9999999999.0000'') as "Size on Disk Uncompressed",
		to_char((sotailtablesizedisk :: FLOAT8 / 1024 / 1024 / 1024),
			''FM9999999999.0000'') as "Size on Disk",
		to_char((sotailtablesizeuncompressed - sotailtablesizedisk)
			* 100 / sotailtablesizeuncompressed, ''FM999'') as "% Compressed"
		from gp_toolkit.gp_size_of_table_and_indexes_licensing
		where sotailschemaname = ''' || schemaname || ''' and
			sotailtablename = ''' || rec.table_name || ''''
		INTO nbSizeU, nbSize, nbCompRatio;
      RAISE INFO 'Step 2: SizeOnDiskU: % CompressRatio: %',nbSizeU, nbCompRatio;

      -- 3. Get Table Nomenclature to get compressed tables stats, and column storage
      EXECUTE 	'SELECT
		  tbl.reltuples::numeric as EstRows,
		  tbl.relpages as EstPages,
		  tbl.oid                  AS tableId,
		  CASE WHEN tbl.relstorage = ''a'' THEN ''append-optimized''
		  WHEN tbl.relstorage = ''h'' THEN ''heap''
		  WHEN tbl.relstorage = ''p'' THEN ''parquet''
		  WHEN tbl.relstorage = ''c'' THEN ''columnar''
		  ELSE ''error''
		  END                               AS storageMode,
		tbl.relnatts AS noOfColumns,

		-- appendonly table stats,
		CASE WHEN columnstore = TRUE THEN ''columnar''
		ELSE ''heap'' END AS columnStore,
		col.compresstype AS compresstype,
		col.compresslevel AS compresslevel

		FROM pg_class AS tbl INNER JOIN pg_namespace AS sch
		ON tbl.relnamespace = sch.oid
		LEFT OUTER JOIN pg_appendonly col
		ON tbl.oid = col.relid
		LEFT OUTER JOIN pg_partition par
		ON tbl.oid = par.parrelid
		WHERE
		col.blocksize > 0 AND tbl.relchecks = 0
		AND sch.nspname = ''' || schemaname || '''
		AND relname = ''' || rec.table_name || ''''
		INTO nbEstRows, nbEstPages, nboid, nbStorageMode, nbNoofCols, nbColStore, nbCompType, nbCompLevel ;

      -- 4. Get Distribution keys for the table
	EXECUTE 'select array_to_string(attrnums, '','')
		from gp_distribution_policy
		where localoid = ' || nboid INTO nbDK;

      -- TODO Check Partitioned tables pg_partition, pg_partition columns

      -- Insert into haystack.tables
      INSERT INTO haystack.tables( runid, table_oid, db_name, schema_name, table_name, storage_mode,noOfCols, IsColumnar, noOfRows,
      sizeInGB, sizeInGBU, compressType, compressLevel, compressRatio, skew, dkarray)
      values( nbRunId, nbOid, rec.table_catalog, schemaname, rec.table_name, nbStorageMode, nbNoofCols, nbColStore,nbEstRows,
       nbSize, nbSizeU, nbCompType, nbCompLevel, nbCompRatio, nbSkew, nbDK);

      -- Insert into haystack.columns
      EXECUTE 'insert into haystack.columns (  runid ,  table_oid , column_name, ordinal_position, data_type , isdk ,
		character_maximum_length ,  numeric_precision , numeric_precision_radix ,  numeric_scale )

		select B.runid,  B.table_oid as table_oid, A.column_name, A.ordinal_position, A.data_type,  NOT(A.ordinal_position != ALL (D.attrnums)) as IsDK,
			A.character_maximum_length, A.numeric_precision, A.numeric_precision_radix, A.numeric_scale
		from information_schema.columns A
		inner join  haystack.tables B
		on A.table_catalog = B.db_name
		and A.table_schema  = B.schema_name
		and A.table_name = B.table_name
		and B.runid = ' || nbRunId || '
		and B.table_oid = ' || nbOid || '
		left outer join  gp_distribution_policy D
		on B.table_oid = D.localoid ';


	RAISE INFO 'COMPLETE Processing for Table %.%',schemaname, rec.table_name;
   END LOOP;


    RETURN 'done';
END$BODY$
LANGUAGE plpgsql VOLATILE;