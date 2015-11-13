--select * from pg_exttable

		SELECT tbl.oid, sch.nspname, relname,
		  tbl.reltuples::numeric as EstRows,
		  tbl.relpages as EstPages,
		  tbl.oid                  AS tableId, relkind ,
		  CASE WHEN tbl.relstorage = 'a' THEN 'append-optimized'
		  WHEN tbl.relstorage = 'h' THEN 'heap'
		  WHEN tbl.relstorage = 'p' THEN 'parquet'
		  WHEN tbl.relstorage = 'c' THEN 'columnar'
		  ELSE 'error'
		  END                               AS storageMode,
		tbl.relnatts AS noOfColumns,

		-- appendonly table stats,
		CASE WHEN columnstore = TRUE THEN 'columnar'
		ELSE 'heap' END AS columnStore,
		col.compresstype AS compresstype,
		col.compresslevel AS compresslevel

		FROM pg_class AS tbl INNER JOIN pg_namespace AS sch
		ON tbl.relnamespace = sch.oid
		LEFT OUTER JOIN pg_appendonly col
		ON tbl.oid = col.relid
		LEFT OUTER JOIN pg_partition par
		ON tbl.oid = par.parrelid
		WHERE   tbl.relname not in ( select partitiontablename from pg_partitions) -- Don't fetch leaf partitions
		and tbl.oid not in (select reloid from pg_exttable) -- exclude external tables
		and nspname not in ('information_schema','pg_catalog')
		and relkind = 'r'  -- Check if its a Table
		and relname not like 'rpt%'
		and relname not like  'es1_%'
		and tbl.reltuples > 10
		--AND tbl.relchecks = 0


		order by 3 -- desc


		=====
		--select * from pg_exttable

		SELECT tbl.oid, sch.nspname, relname,
		  tbl.reltuples::numeric as EstRows,
		  tbl.relpages as EstPages,
		  tbl.oid                  AS tableId, relkind ,
		  CASE WHEN tbl.relstorage = 'a' THEN 'append-optimized'
		  WHEN tbl.relstorage = 'h' THEN 'heap'
		  WHEN tbl.relstorage = 'p' THEN 'parquet'
		  WHEN tbl.relstorage = 'c' THEN 'columnar'
		  ELSE 'error'
		  END                               AS storageMode,
		tbl.relnatts AS noOfColumns,

		-- appendonly table stats,
		CASE WHEN columnstore = TRUE THEN 'columnar'
		ELSE 'heap' END AS columnStore,
		col.compresstype AS compresstype,
		col.compresslevel AS compresslevel

		FROM pg_class AS tbl INNER JOIN pg_namespace AS sch
		ON tbl.relnamespace = sch.oid
		LEFT OUTER JOIN pg_appendonly col
		ON tbl.oid = col.relid
		LEFT OUTER JOIN pg_partition par
		ON tbl.oid = par.parrelid
		WHERE   tbl.relname not in ( select partitiontablename from pg_partitions) -- Don't fetch leaf partitions
		and tbl.oid not in (select reloid from pg_exttable) -- exclude external tables
		and nspname not in ('information_schema','pg_catalog')
		and relkind = 'r'  -- Check if its a Table
		and relname not like 'rpt%'
		and relname not like  'es1_%'
		and tbl.reltuples > 10
		--AND tbl.relchecks = 0


		order by 3 -- desc