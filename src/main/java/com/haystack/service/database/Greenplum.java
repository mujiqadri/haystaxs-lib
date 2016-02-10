package com.haystack.service.database;

import com.haystack.domain.*;
import com.haystack.util.Credentials;
import com.haystack.util.DBConnectService;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;

/**
 * Created by qadrim on 16-02-04.
 */
public class Greenplum extends Cluster {

    public Greenplum() {

        this.dbtype = DBConnectService.DBTYPE.GREENPLUM;
    }

    // Get Tables from Cluster along with thier Structure and Stats
    // This method works when connected to the cluster
    // this doesnot work when GPSD file is uploaded since this information
    // should be extracted from the stats - call getTableDetailsfromStats


    public String loadTablesfromCatalog(boolean returnJson) {

        Tables tablelist = new Tables();

        log.info("Populating table properties for Schemas where threshold has passed");

        Integer schemaThreshold = Integer.parseInt(this.configProperties.properties.getProperty("schema.change.threshold.days"));

        try {
            // Check if schema.change.threshold has passed from the last run_log timestamp

            String sqlSchemas = "select a.schema_name, B.max_run_date, now()\n" +
                    "from information_schema.schemata A\n" +
                    "\n" +
                    "left outer join \n" +
                    "\t(\n" +
                    "\tselect run_schema, max(run_date) as max_run_date\n" +
                    "\tfrom " + haystackSchema + ".run_log\n" +
                    "\tgroup by run_schema\n" +
                    "\t) AS B\n" +
                    "on A.schema_name = B.run_schema\n" +
                    "where A.schema_name not in ('pg_toast','pg_bitmapindex','madlib','pg_aoseg','pg_catalog'\n" +
                    ",'gp_toolkit','information_schema','" + haystackSchema + "')\n" +
                    "and now() > COALESCE(B.max_run_date, '1900-01-01')::DATE  + INTERVAL '" + schemaThreshold.toString() + " days' ";
            ResultSet rsSchemas = dbConn.execQuery(sqlSchemas);

            while (rsSchemas.next()) {

                String sRunID = "";
                String schemaName = rsSchemas.getString("schema_name");
                Date runDate = rsSchemas.getDate("max_run_date");

                log.info("Processing tables in Schema:" + schemaName);

                String sqlRunStats = "select " + haystackSchema + ".capturetabledetailsgpdb('" + schemaName + "','" + this.properties.getProperty("haystack.ds_cluster.username") + "')";
                ResultSet rsRes = dbConn.execQuery(sqlRunStats);
                while (rsRes.next()) {
                    sRunID = rsRes.getString(1);
                }
                rsRes.close();

                // Now Update SizeOnDisk for all tables in schema
                String sqlSize = "\t\tselect sotailtablename as tableName, \n" +
                        "\t\tto_char((sotailtablesizeuncompressed :: FLOAT8 / 1024 / 1024 / 1024),\n" +
                        "\t\t\t'FM9999999999.0000') as SizeOnDiskU,\n" +
                        "\t\tto_char((sotailtablesizedisk :: FLOAT8 / 1024 / 1024 / 1024),\n" +
                        "\t\t\t'FM9999999999.0000') as SizeOnDisk,\n" +
                        "\t\tto_char((sotailtablesizeuncompressed - sotailtablesizedisk) \n" +
                        "\t\t\t* 100 / sotailtablesizeuncompressed, 'FM999') as CompressRatio\n" +
                        "\t\t\t\n" +
                        "\t\tfrom gp_toolkit.gp_size_of_table_and_indexes_licensing \n" +
                        "\t\twhere sotailschemaname = '" + schemaName + "'";

                ResultSet rsSize = dbConn.execQuery(sqlSize);
                while (rsSize.next()) {
                    String sTableName = rsSize.getString("tableName");
                    Float sizeOnDiskU = rsSize.getFloat("SizeOnDiskU");
                    Float sizeOnDisk = rsSize.getFloat("SizeOnDisk");
                    Float compressRatio = rsSize.getFloat("CompressRatio");

                    String sqlUpdTbl = "update " + haystackSchema + ".tables set sizeInGB = " + sizeOnDisk +
                            ", sizeInGBU = " + sizeOnDiskU + ", compressRatio = " + compressRatio +
                            " where schema_name = '" + schemaName + "' and table_name = '" + sTableName + "'";
                    dbConn.execNoResultSet(sqlUpdTbl);
                }
                rsSize.close();
            }
            log.info("Schema Processing Complete");
            rsSchemas.close();

            // Load Tables into Memory
            tablelist.load(dbConn);

        } catch (Exception e) {
            log.error("Error while fetching table details from cluster:" + e.toString());
        }
        return null;
    }


    @Override
    public String loadTables(boolean return_Json) {
        String jsonResult = "";
        try {
            String sqlTbl = "\t\tSELECT tbl.oid as table_oid, sch.nspname as schema_name, relname as table_name,\n" +
                    // Changed Muji 23Jan 2016 :tbl.reltuples::bigint
                    //"\t\t  to_char(tbl.reltuples::numeric,'9999999999999999D99') as NoOfRows,\n" +
                    "\t\t  tbl.reltuples::bigint as NoOfRows,\n" +

                    "\t\t  tbl.relpages as relPages,\n" +
                    "\t\t  ((tbl.relpages::numeric * 32) /(1024*1024)) sizeinGB,\n" +
                    "\t\t  tbl.oid                  AS tableId, relkind ,\n" +
                    "\t\t  CASE WHEN tbl.relstorage = 'a' THEN 'append-optimized'\n" +
                    "\t\t  WHEN tbl.relstorage = 'h' THEN 'heap'\n" +
                    "\t\t  WHEN tbl.relstorage = 'p' THEN 'parquet'\n" +
                    "\t\t  WHEN tbl.relstorage = 'c' THEN 'columnar'\n" +
                    "\t\t  ELSE 'error'\n" +
                    "\t\t  END                               AS storage_Mode,\n" +
                    "\t\ttbl.relnatts AS noOfColumns,\n" +
                    "\n" +
                    "\t\t-- appendonly table stats,\n" +
                    "\t\tCASE WHEN columnstore = TRUE THEN 't' ELSE 'f' END as IsColumnar,\n" +
                    "\t\tcol.compresstype AS compresstype,\n" +
                    "\t\tCOALESCE(col.compresslevel,0) AS compresslevel,\n" +
                    "\t\tcase when length(coalesce(par.tablename,'')) = 0 then 0 else 1 end as IsPartitioned,\n" +
                    "\t\tarray_to_string(dk.attrnums, ',') as dkarray \n" +
                    "\t\tFROM pg_class AS tbl INNER JOIN pg_namespace AS sch\n" +
                    "\t\tON tbl.relnamespace = sch.oid\n" +
                    "\t\tLEFT OUTER JOIN gp_distribution_policy dk ON tbl.oid = dk.localoid\n" +
                    "\t\tLEFT OUTER JOIN pg_appendonly col ON tbl.oid = col.relid\n" +
                    "\t\t\n" +
                    "\t\tLEFT OUTER JOIN ( select distinct schemaname, tablename from pg_partitions ) as par\n" +
                    "\t\t ON sch.nspname = par.schemaname AND tbl.relname = par.tablename\n" +
                    "\t\tWHERE   tbl.relname not in ( select partitiontablename from pg_partitions) -- Don't fetch leaf partitions\n" +
                    "\t\tand tbl.oid not in (select reloid from pg_exttable) -- exclude external tables\n" +
                    "\t\tand nspname not in ('information_schema','pg_catalog','gptext')\n" +
                    "\t\tand relkind = 'r'  -- Check if its a Table\n" +
                    "\t\tand relname not like 'rpt%'\n" +
                    "\t\tand relname not like  'es1_%'\n" +
                    "\t\tand ((tbl.relpages > 0 ) OR (tbl.relpages = 0 and par.tablename is not null))\n" +
                    "\t\t--AND tbl.relchecks = 0\n" +
                    "\t\torder by 3 -- desc\n";
            ResultSet rsTbl = dbConn.execQuery(sqlTbl);

            while (rsTbl.next()) {   // Fetch all parent level tables, if table is partitioned then load all Partitions
                Table tbl = new Table();
                tbl.oid = rsTbl.getString("table_oid");
                tbl.database = dbConn.getDB();
                tbl.schema = rsTbl.getString("schema_name");
                tbl.tableName = rsTbl.getString("table_name");

                tbl.stats = new TableStats();
                tbl.stats.storageMode = rsTbl.getString("storage_mode");
                tbl.stats.relPages = rsTbl.getInt("relpages");
                tbl.stats.noOfColumns = Integer.parseInt(rsTbl.getString("noOfColumns"));
                tbl.stats.isColumnar = (rsTbl.getString("IsColumnar").equals("t")) ? true : false;
                String sNoOfRows = rsTbl.getString("NoOfRows");

                tbl.stats.noOfRows = Double.valueOf(sNoOfRows);
                tbl.stats.sizeOnDisk = Float.parseFloat(rsTbl.getString("sizeinGB"));
                tbl.stats.sizeUnCompressed = Float.parseFloat(rsTbl.getString("sizeinGB"));
                tbl.stats.compressType = rsTbl.getString("compressType");
                tbl.stats.compressLevel = Integer.parseInt(rsTbl.getString("compressLevel"));
                //tbl.stats.compressionRatio = Float.parseFloat(rsTbl.getString("compressRatio"));
                //tbl.stats.skew = Float.parseFloat(rsTbl.getString("skew"));
                tbl.stats.sizeForDisplayCompressed = getSizePretty(tbl.stats.sizeOnDisk); // Normalize size and unit to display atleast 2 digits
                tbl.stats.sizeForDisplayUnCompressed = getSizePretty(tbl.stats.sizeUnCompressed); // Normalize size and unit to display atleast 2 digits
                tbl.dkArray = rsTbl.getString("dkarray");

                if (rsTbl.getInt("IsPartitioned") == 1) {   // Table is partitioned fetch partition details, and Rollup NoOfRows and size to Topmost Parent Table
                    // Load Partitions
                    String partSQL = "\n" +
                            "\t\t\tselect C.relpages, C.reltuples, partitiontablename, partitionname, parentpartitiontablename, parentpartitionname, partitiontype, partitionlevel, partitionrank,\n" +
                            "\t\t\tpartitionposition, partitionlistvalues, partitionrangestart, partitionstartinclusive, partitionrangeend, partitionendinclusive, \n" +
                            "\t\t\tpartitioneveryclause, partitionisdefault, partitionboundary\n" +
                            "\t\t\tfrom pg_partitions A \n" +
                            "\t\t\t\tINNER JOIN pg_namespace B ON B.nspname = A.schemaname \n" +
                            "\t\t\t\tINNER JOIN pg_class C ON C.relname = A.partitiontablename AND C.relnamespace = B.oid\n" +
                            "\t\t\twhere A.schemaname = '" + tbl.schema + "' and A.tablename = '" + tbl.tableName + "' order by partitionlevel, partitionposition; \n";

                    ResultSet rsPar = dbConn.execQuery(partSQL);

                    while (rsPar.next()) {


                        Partition partition = new Partition();
                        partition.relPages = rsPar.getInt("relpages");
                        partition.relTuples = rsPar.getInt("reltuples");
                        partition.tableName = rsPar.getString("partitiontablename");
                        partition.partitionName = rsPar.getString("partitionname");
                        partition.type = rsPar.getString("partitiontype");
                        partition.level = rsPar.getInt("partitionlevel");
                        partition.rank = rsPar.getInt("partitionrank");
                        partition.position = rsPar.getInt("partitionposition");
                        partition.listValues = rsPar.getString("partitionlistvalues");
                        partition.rangeStart = rsPar.getString("partitionrangestart");
                        partition.rangeStartInclusive = rsPar.getBoolean("partitionstartinclusive");
                        partition.rangeEnd = rsPar.getString("partitionrangeend");
                        partition.rangeEndInclusive = rsPar.getBoolean("partitionendinclusive");
                        partition.everyClause = rsPar.getString("partitioneveryclause");
                        partition.isDefault = rsPar.getBoolean("partitionisdefault");
                        partition.boundary = rsPar.getString("partitionboundary");

                        partition.parentPartitionTableName = rsPar.getString("parentpartitiontablename");
                        partition.parentPartitionName = rsPar.getString("parentpartitionname");

                        tbl.stats.addChildStats(partition.relPages, partition.relTuples);
                        tbl.addPartition(tbl.partitions, partition);


                    }
                    rsPar.close();

                    // Recalculate TableSize based on partitions
                    tbl.stats.sizeOnDisk = ((tbl.stats.relPages * 32) / (1024 * 1024));
                    tbl.stats.sizeUnCompressed = tbl.stats.sizeOnDisk;
                    tbl.stats.sizeForDisplayCompressed = getSizePretty(tbl.stats.sizeOnDisk); // Normalize size and unit to display atleast 2 digits
                    tbl.stats.sizeForDisplayUnCompressed = getSizePretty(tbl.stats.sizeUnCompressed); // Normalize size and unit to display atleast 2 digits
                }

                // Get Column Details
                String sqlCol = "\n" +
                        "\t\tselect  A.column_name, A.ordinal_position, A.data_type,  NOT(A.ordinal_position != ALL (D.attrnums)) as IsDK,\n" +
                        "\t\t\tA.character_maximum_length, A.numeric_precision, A.numeric_precision_radix, A.numeric_scale, \n" +
                        "\t\t\tcase when E.Partitionlevel is null then false else true end as IsPartitioned, E.partitionlevel, position_in_partition_key\n" +
                        "\t\tfrom information_schema.columns A\n" +
                        "\t\t\tINNER JOIN pg_class B ON  B.relname = A.table_name\n" +
                        "\t\t\tINNER JOIN pg_namespace C ON  B.relnamespace = C.oid and C.nspname = A.table_schema\n" +
                        "\t\t\tLEFT OUTER JOIN gp_distribution_policy D on B.oid = D.localoid \n" +
                        "\t\t\tLEFT OUTER JOIN pg_partition_columns E on E.schemaname = A.table_schema AND E.tablename = A.table_name AND E.columnname = A.column_name\n" +
                        "\t\twhere A.table_schema = '" + tbl.schema + "' and A.table_name = '" + tbl.tableName + "' \n" +
                        "\t\torder by A.ordinal_position";

                ResultSet rsCol = dbConn.execQuery(sqlCol);

                while (rsCol.next()) {
                    Column col = new Column();
                    col.column_name = rsCol.getString("column_name");
                    col.ordinal_position = rsCol.getInt("ordinal_position");
                    col.data_type = rsCol.getString("data_type");
                    col.isDK = Boolean.parseBoolean(rsCol.getString("isdk"));
                    col.character_maximum_length = rsCol.getInt("character_maximum_length");
                    col.numeric_precision = rsCol.getInt("numeric_precision");
                    col.numeric_precision_radix = rsCol.getInt("numeric_precision_radix");
                    col.numeric_scale = rsCol.getInt("numeric_scale");
                    col.isPartitioned = rsCol.getBoolean("IsPartitioned");
                    col.partitionLevel = rsCol.getInt("partitionlevel");
                    col.positionInPartitionKey = rsCol.getInt("position_in_partition_key");
                    if (col.isPartitioned == true) {
                        tbl.partitionColumn.put(col.column_name, col);
                    }
                    tbl.columns.put(col.column_name, col);
                }
                rsCol.close();
                String key = tbl.schema + ":" + tbl.tableName;
                tbl.setDistributionKey();
                tableHashMap.put(key, tbl);
            }
            rsTbl.close();

            if (return_Json) {
                jsonResult = getJSON();
            }
        } catch (Exception e) {
            log.error("Error in loading tables from Stats" + e.toString());
        }
        return jsonResult;
    }


    @Override
    public void loadQueries(Integer clusterId, Timestamp lastRefreshTime) {

        // Connect to the Cluster to fetch the queries,
        // Load the queries in a temp table in haystack Database
        // From this temp table create partitions for the queries table in userSchema in Haystack DB
        try {
            String sql = "SELECT A.logsession, A.logcmdcount, A.logdatabase, A.loguser, A.logpid, min(A.logtime) logsessiontime, min(A.logtime) AS logtimemin,\n" +
                    "                 max(A.logtime) AS logtimemax, max(A.logtime) - min(A.logtime) AS logduration, min(logdebug) as sql\n" +
                    "\t\tFROM gp_toolkit.__gp_log_master_ext A\n" +
                    "\t\tWHERE A.logsession IS NOT NULL AND A.logcmdcount IS NOT NULL AND A.logdatabase IS NOT NULL and logsessiontime > '" + lastRefreshTime + "' " +
                    "\t\tGROUP BY A.logsession, A.logcmdcount, A.logdatabase, A.loguser, A.logpid\n" +
                    "\t\tHAVING length(min(logdebug)) > 0;';";
            ResultSet rs = dbConn.execQuery(sql);

            // Create a new connection to Haystack Database, recreate a temp table using gpsd_id and load queries into that table


            String tmpTblName = "public.tmp_queries_gpsd_id_" + clusterId;
            sql = "DROP TABLE IF EXISTS " + tmpTblName + ";";
            haystackDBConn.execNoResultSet(sql);

            sql = "CREATE TABLE " + tmpTblName + "(" +
                    "  logsession text,\n" +
                    "  logcmdcount text,\n" +
                    "  logdatabase text,\n" +
                    "  loguser text,\n" +
                    "  logpid text,\n" +
                    "  logsessiontime timestamp with time zone,\n" +
                    "  logtimemin timestamp with time zone,\n" +
                    "  logtimemax timestamp with time zone,\n" +
                    "  logduration interval,\n" +
                    "  sql text);";
            haystackDBConn.execNoResultSet(sql);

            while (rs.next()) {
                sql = String.format("INSERT INTO %s ( logsession, logcmdcount, logdatabase, loguser, logpid, logsessiontime,"
                                + "logtimemin, logtimemax, logduration, sql) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        tmpTblName, rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getString(5),
                        rs.getString(6), rs.getString(7), rs.getString(8), rs.getString(9), rs.getString(10));
                haystackDBConn.execNoResultSet(sql);
            }
            rs.close();

            // Process queries by calling parent function
            super.processQueries(tmpTblName, clusterId);

        } catch (Exception e) {
            log.error("Error is loading Queries for GPSD_ID" + clusterId);
        }

    }


    public void refreshTableStats(Integer clusterId) {

    }

}
