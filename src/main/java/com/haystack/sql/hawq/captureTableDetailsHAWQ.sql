

CREATE OR REPLACE FUNCTION haystack.capturetabledetailshawq(schemaname text, prunid integer)
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
BEGIN
   FOR rec IN
	select  table_schema, table_name, table_catalog
	from information_schema.tables
	where table_schema = schemaname
	and table_type = 'BASE TABLE'
	and is_insertable_into = 'YES'
    and table_name not like '%_1_prt_%'
	order by table_name
   LOOP

      -- TODO Put Skew as a separate function in the tool
      -- 1. Calculate Total Rows and Skew Variance for the table

      /*EXECUTE 'select round(coalesce(stddev(A.rows),0) / coalesce(sum(A.rows),1),2), coalesce(sum(A.rows),0)
		from (	select  (count(1)) as rows
			from ' || schemaname || '.' || rec.table_name ||
		' group by gp_segment_id ) as A' INTO nbSkew, nbRows;
	*/

      -- 2. Get Size on Disk and Compression Ratio
      EXECUTE 'select sotusize as SizeUncompressed, sotdsize as SizeOnDisk,
		round((sotusize / (case when sotdsize = 0 then 1 else sotdsize end ))::numeric,0)
		from hawq_toolkit.hawq_size_of_table_disk A
		left outer join hawq_toolkit.hawq_size_of_table_uncompressed B
		on A.sotdoid  = b.sotuoid
		where sotdschemaname =  ''' || schemaname || ''' and sotutablename =  ''' || rec.table_name
		|| ''''
		INTO nbSizeU, nbSize, nbCompRatio;

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
      INSERT INTO haystack.tables( runid, oid, db_name, schema_name, table_name, storage_mode,noOfCols, IsColumnar, noOfRows,
      sizeInBytes, sizeInBytesU, compressType, compressLevel, compressRatio, skew, dkarray)
      values( pRunId, nbOid, rec.table_catalog, schemaname, rec.table_name, nbStorageMode, nbNoofCols, nbColStore,nbEstRows,
       nbSize, nbSizeU, nbCompType, nbCompLevel, nbCompRatio, nbSkew, nbDK);

      -- Insert into haystack.columns
      EXECUTE 'insert into haystack.columns (  runid ,  table_oid , column_name, ordinal_position, data_type , isdk ,
		character_maximum_length ,  numeric_precision , numeric_precision_radix ,  numeric_scale )

		select B.runid,  B.oid as table_oid, A.column_name, A.ordinal_position, A.data_type,  NOT(A.ordinal_position != ALL (D.attrnums)) as IsDK,
			A.character_maximum_length, A.numeric_precision, A.numeric_precision_radix, A.numeric_scale
		from information_schema.columns A
		inner join  haystack.tables B
		on A.table_catalog = B.db_name
		and A.table_schema  = B.schema_name
		and A.table_name = B.table_name
		left outer join  gp_distribution_policy D
		on B.oid = D.localoid

		inner join haystack.runlog C
			on B.runid = C.runid
		where table_schema = ''' || schemaname || '''

		and C.run_date = (
			select max(run_date)
			from haystack.runlog
			where run_schema = A.table_schema
		)';
   END LOOP;


    RETURN 'done';
END$BODY$
LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION haystack.capturetabledetailshawq(text, integer)
OWNER TO gpadmin;