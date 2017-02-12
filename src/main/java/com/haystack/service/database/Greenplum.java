package com.haystack.service.database;

import com.haystack.domain.*;
import com.haystack.util.ConfigProperties;
import com.haystack.util.Credentials;
import com.haystack.util.DBConnectService;

import javax.swing.text.StyledEditorKit;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;

import org.postgresql.util.PGInterval;

/**
 * Created by qadrim on 16-02-04.
 */
public class Greenplum extends Cluster {

    public Greenplum() {

        this.dbtype = DBConnectService.DBTYPE.GREENPLUM;
    }

/*
    @Override
    public Tables loadTables(Credentials credentials, Boolean isGPSD) {
        if (isGPSD){
            return loadTablesfromStats(credentials);
        } else {
            return loadTablesfromCatalog(credentials);
        }
    }
*/
    // Get Tables from Cluster along with thier Structure and Stats
    // This method works when connected to the cluster
    // this doesnot work when GPSD file is uploaded since this information
    // should be extracted from the stats - call getTableDetailsfromStats

    @Override
    public Tables loadTables(Credentials credentials, Boolean isGPSD, Integer cluster_id) {

        Tables tables = new Tables();

        Integer maxClusterLogId = 0;
        String jsonResult = "";
        try {
            ConfigProperties configProperties = new ConfigProperties();
            configProperties.loadProperties();
            double analyzePercentage = Double.parseDouble(configProperties.properties.getProperty("table.AnalyzePercentage"));

            dbConn = new DBConnectService(this.dbtype);
            dbConn.connect(credentials);

            String sql = "select user_name, B.user_id, C.dbname\n" +
                    "from " + haystackSchema + ".cluster_users A, " + haystackSchema + ".users B, " + haystackSchema + ".cluster C\n" +
                    "where A.user_id = B.user_id  \n" +
                    "and A.cluster_id = C.cluster_id\n" +
                    "and A.cluster_id = " + cluster_id;

            ResultSet rsUser = haystackDBConn.execQuery(sql);
            rsUser.next();
            String userSchema = rsUser.getString("user_name");
            Integer userId = rsUser.getInt("user_id");
            String gpsd_dbName = rsUser.getString("dbname");

            // Get Max GPSDLogId
            sql = "select nextval('" + haystackSchema + ".seq_gpsd_log');";
            ResultSet rsClusterLogId = haystackDBConn.execQuery(sql);
            rsClusterLogId.next();

            maxClusterLogId = rsClusterLogId.getInt(1);

            // Insert record in QUERY_LOG table
            sql = "INSERT INTO " + haystackSchema + ".cluster_logs(cluster_log_id, user_id, status, original_file_name," +
                    " file_checksum,submitted_on, cluster_id) VALUES (" + maxClusterLogId + "," + userId + ",'SUBMITTED','SCHEDULED_REFRESH HOST=" + credentials.getHostName() +
                    " Database=" + credentials.getDatabase() + "'," +
                    " '" + maxClusterLogId + "=' || now(), now()," + cluster_id + " );";
            haystackDBConn.execNoResultSet(sql);

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
                    "\t\torder by 4 desc\n";

            // Count Rows to Update Progress

            ResultSet rsCountQueries = dbConn.execQuery("SELECT COUNT(*) FROM (" + sqlTbl + ") AS X;");
            rsCountQueries.next();
            Integer totalNoOfQueries = rsCountQueries.getInt(1);

            ResultSet rsTbl = dbConn.execQuery(sqlTbl);

            Integer currCounter = 0;
            Integer lastPercentageUpdated = -1;
            Integer noOfTablesWithZeroSize = 0;

            while (rsTbl.next()) {   // Fetch all parent level tables, if table is partitioned then load all Partitions
                currCounter++;

                Integer currPercentage = (currCounter * 100 / totalNoOfQueries);
                if (currPercentage > lastPercentageUpdated) {
                    sql = "UPDATE " + haystackSchema + ".cluster_logs SET last_updated = now(), percent_processed = " + currPercentage + " where cluster_log_id = " + maxClusterLogId + ";";
                    haystackDBConn.execNoResultSet(sql);
                    lastPercentageUpdated = currPercentage;
                }

                Table tbl = new Table();
                try {
                    tbl.oid = rsTbl.getString("table_oid");
                    tbl.database = dbConn.getDB();
                    tbl.schema = rsTbl.getString("schema_name");
                    tbl.tableName = rsTbl.getString("table_name");

                    // TODO For Debugging
                    if (tbl.tableName.contains("ci_audience_profile")) {
                        int i = 0;
                    }

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
                    String tmpDKArr = rsTbl.getString("dkarray");
                    if (tmpDKArr == null) {
                        tbl.dkArray = "RANDOM";
                    } else {
                        tbl.dkArray = rsTbl.getString("dkarray");
                    }
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
                        tbl.stats.sizeOnDisk = ((tbl.stats.relPages.floatValue() * 32) / (1024 * 1024));
                        tbl.stats.sizeUnCompressed = tbl.stats.sizeOnDisk;
                        tbl.stats.sizeForDisplayCompressed = getSizePretty(tbl.stats.sizeOnDisk); // Normalize size and unit to display atleast 2 digits
                        tbl.stats.sizeForDisplayUnCompressed = getSizePretty(tbl.stats.sizeUnCompressed); // Normalize size and unit to display atleast 2 digits
                    }

                    //If size of the table is 0
                    if(tbl.stats.sizeUnCompressed == 0){
                        noOfTablesWithZeroSize++;

                        //calculate the percentage of those tables which have 0 size
                        float percentage = (noOfTablesWithZeroSize / totalNoOfQueries) * 100;

                        //if percentage  25% or greater then throw an exception
                        if(percentage >= analyzePercentage){
                            throw new RuntimeException("Statistics not collected, please run Analyze on the Tables");
                        }
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
                    tables.tableHashMap.put(key, tbl);
                } catch (Exception e) {
                    log.error("Error in loading Table:" + tbl.schema + "." + tbl.tableName + " ERR=" + e.toString());
                }
            }
            rsTbl.close();

            // Check if last gpsd_refresh_time is greater than interval
            sql = "select coalesce(last_schema_refreshed_on,'1900-01-01') as last_schema_refreshed_on , now() as current_time  from " + haystackSchema +
                    ".cluster where host is not null and is_active = true and cluster_id = " + cluster_id + ";";
            ResultSet rs = haystackDBConn.execQuery(sql);
            rs.next();
            Timestamp lastSchemaRefreshTime = rs.getTimestamp("last_schema_refreshed_on");
            Timestamp currentTime = rs.getTimestamp("current_time");
            Integer schemaRefreshIntervalHours = Integer.parseInt(this.properties.getProperty("schema.refresh.interval.hours"));
            lastSchemaRefreshTime.setTime(lastSchemaRefreshTime.getTime() + (schemaRefreshIntervalHours * 60 * 60 * 1000));
            if (lastSchemaRefreshTime.compareTo(currentTime) < 0) {// if this time is less than current_time, if yes then refresh schema
                super.saveClusterStats(maxClusterLogId, tables);
                sql = "update " + haystackSchema + ".cluster set last_schema_refreshed_on = now () where cluster_id = " + cluster_id + ";";
                haystackDBConn.execNoResultSet(sql);
            }
            sql = "UPDATE " + haystackSchema + ".cluster_logs SET status = 'COMPLETED', completed_on = now() where cluster_log_id = " + maxClusterLogId + ";";
            haystackDBConn.execNoResultSet(sql);

        } catch (Exception e) {
            log.error("Error in loading tables from Stats" + e.toString());
        }
        return tables;
    }


    protected String getQueryType(String input) {
        List<String> delimiters = Arrays.asList("(", ")", "=");


        String query = input.trim().toUpperCase();
        String queryType = "";

        StringTokenizer countSQL = new StringTokenizer(query, ";");
        Integer noOfSQLStatements = countSQL.countTokens();

        if (noOfSQLStatements > 1) {
            queryType = "MULTIPLE SQL STATEMENTS";
        }
        // Check Generic Statements which don't start with a particular Type Delimiter
        if (query.contains("LOCK TABLE")) {
            queryType = "LOCK";
        }
        if (query.contains("UNLISTEN")) {
            queryType = "INTERNAL";
        }

        if (queryType.length() > 0) {
            return queryType;
        }


        StringTokenizer tokens = new StringTokenizer(query, " ;");
        while (tokens.hasMoreTokens()) {
            String token = tokens.nextToken().trim();
            // Ignore white space and punctuations

            if (delimiters.contains(token)) {
                // Skip this token
                continue;
            }
            switch (token) {
                case "SET":
                    queryType = "SET CONFIGURATION";
                    break;
                case "SHOW":
                    queryType = "SHOW";
                    break;
                case "CREATE":
                    queryType = "CREATE";
                    break;
                case "VACUUM":
                    queryType = "MAINTENANCE";
                    break;
                case "ALTER":
                    queryType = "ALTER";
                    break;
                case "ANALYZE":
                    queryType = "MAINTENANCE";
                    break;
                case "GRANT":
                    queryType = "SECURITY";
                    break;
                case "REVOKE":
                    queryType = "SECURITY";
                    break;
                case "COMMENT":
                    queryType = "COMMENT";
                    break;
                case "TRUNCATE":
                    queryType = "TRUNCATE";
                    break;
                case "UPDATE":
                    queryType = "UPDATE";
                    break;
                case "DELETE":
                    queryType = "DELETE";
                    break;
                case "DROP":
                    if (query.contains("TABLE")) {
                        queryType = "DROP TABLE";
                    } else queryType = "UNRESOLVED";
                case "SELECT":
                    queryType = "SELECT";
                    break;
                case "WITH":
                    if (query.contains("SELECT")) {
                        queryType = "SELECT";
                    } else queryType = "UNRESOLVED";
                case "INSERT":
                    queryType = "INSERT";
                    if (query.contains("VALUES")) {
                        queryType = "SIMPLE INSERT";
                    } else {
                        if (query.contains("SELECT")) {
                            queryType = "INSERT SELECT";
                        } else queryType = "UNRESOLVED";
                        break;
                    }
                case "BEGIN":
                    if (query.contains("LOCK TABLE")) {
                        queryType = "LOCK";
                    } else {
                        queryType = "TRANSACTION";
                    }
                    ;
                    break;
                case "ROLLBACK":
                    queryType = "TRANSACTION";
                    break;
                case "SAVEPOINT":
                    queryType = "TRANSACTION";
                    break;
                case "RELEASE":
                    queryType = "TRANSACTION";
                    break;
                case "TRANSACTION":
                    queryType = "TRANSACTION";
                    break;
                case "DEALLOCATE":
                    queryType = "TRANSACTION";
                    break;
                case "COPY":
                    queryType = "COPY";
                    break;
                case "COMMIT":
                    queryType = "COMMIT";
                    break;
                default:
                    queryType = "UNRESOLVED";
            }
            if (queryType.length() > 0)
                return queryType;
        }

        return queryType;
    }


    @Override
    public void loadQueries(Integer clusterId, Timestamp lastRefreshTime) {

        // Connect to the Cluster to fetch the queries,
        // Load the queries in a temp table in haystack Database
        // From this temp table create partitions for the queries table in userSchema in Haystack DB
        String sql = "";

        Integer maxQryLogId = -1;
        try {
            sql = "select user_name, B.user_id, C.dbname\n" +
                    "from " + haystackSchema + ".cluster_users A, " + haystackSchema + ".users B, " + haystackSchema + ".cluster C\n" +
                    "where A.user_id = B.user_id \n" +
                    "and A.cluster_id = C.cluster_id\n" +
                    "and A.cluster_id = " + clusterId;

            ResultSet rsUser = haystackDBConn.execQuery(sql);
            rsUser.next();
            String userSchema = rsUser.getString("user_name");
            Integer userId = rsUser.getInt("user_id");
            String gpsd_dbName = rsUser.getString("dbname");

            // Get Max QueryLogId
            //sql = "select max(query_log_id)+1 max_qry_log_id FROM " + haystackSchema + ".query_logs;";
            sql = "select nextval('" + haystackSchema + ".seq_query_log');";
            ResultSet rsQryLogId = haystackDBConn.execQuery(sql);
            rsQryLogId.next();

            maxQryLogId = rsQryLogId.getInt(1);

            // Insert record in QUERY_LOG table
            sql = "INSERT INTO " + haystackSchema + ".query_logs(query_log_id, user_id, status, original_file_name," +
                    " file_checksum,submitted_on, cluster_id) VALUES (" + maxQryLogId + "," + userId + ",'SUBMITTED','SCHEDULED_REFRESH HOST=" + super.gpsd_Credentials.getHostName() +
                    " Database=" + gpsd_dbName + "'," +
                    " '" + maxQryLogId + "=' || now(), now()," + clusterId + " );";
            haystackDBConn.execNoResultSet(sql);

            // Create Schema and Tables with Partitions
            try {
                super.createUserSchemaTables(userSchema);
            } catch (Exception ex) {
                log.warn("User schema {} has already been created.", userSchema);
            }
            sql = "UPDATE " + haystackSchema + ".query_logs SET status = 'PROCESSING', last_updated = now() where query_log_id = " + maxQryLogId + ";";


            // TODO Comment out partitioning code for Postgres
            // CREATE PARTITION - find the distinct dates from the external table to create partitions

            sql = "SELECT distinct(logsessiontime::date)\n" +
                    "\t\tFROM gp_toolkit.__gp_log_master_ext A\n" +
                    "\t\tWHERE A.logsession IS NOT NULL AND A.logcmdcount IS NOT NULL AND A.logdatabase = '" + gpsd_dbName + "'  and logsessiontime > '" + lastRefreshTime + "' " +
                    "\t\tAND length(logdebug) > 0 AND logdebug not like '%pg_class%';";

            ResultSet rsDates = dbConn.execQuery(sql);

            while (rsDates.next()) {
                String monthpartition_name = rsDates.getString("logsessiontime");
                /*sql = "SELECT count(*) as count FROM pg_partitions WHERE partitionname = '" + monthpartition_name + "' " +
                        " AND lower(tablename) = lower('queries') " + " AND lower(schemaname) = '" + userSchema + "';";
                ResultSet rsCount = haystackDBConn.execQuery(sql);
                rsCount.next();

                if (rsCount.getInt("count") == 0) {
                    // Create Month Partition
                    sql = "ALTER TABLE " + userSchema + ".queries ADD PARTITION \"" + monthpartition_name + "\" START ('" + monthpartition_name + " 00:00:00.000') " +
                            " INCLUSIVE END ('" + monthpartition_name + " 23:59:59.999') EXCLUSIVE;";
                    haystackDBConn.execNoResultSet(sql);
                } */


                // Check if query_log_dates has entry for the current date, if not insert a row
                // Insert record in query_log and query_log_dates tables
                sql = "SELECT COUNT(*) as count FROM " + haystackSchema + ".query_log_dates where query_log_id=" + maxQryLogId + " AND log_date = '" + monthpartition_name + "';";
                ResultSet rsCount = haystackDBConn.execQuery(sql);
                rsCount.next();

                if (rsCount.getInt("count") == 0) {
                    sql = " INSERT INTO " + haystackSchema + ".query_log_dates(query_log_id, log_date) VALUES(" + maxQryLogId + ",'" + monthpartition_name + "');";
                    haystackDBConn.execNoResultSet(sql);
                }
            }


            // Now Process Queries Generating AST for each query

            sql = "SELECT  A.logsession, A.logcmdcount, A.logdatabase, A.loguser, A.logpid, min(A.logtime) logsessiontime, min(A.logtime) AS logtimemin,\n" +
                    "                 max(A.logtime) AS logtimemax, max(A.logtime) - min(A.logtime) AS logduration, min(logdebug) as sql, A.loghost\n" +
                    "\t\tFROM gp_toolkit.__gp_log_master_ext A\n" +
                    "\t\tWHERE A.logsession IS NOT NULL AND A.logcmdcount IS NOT NULL AND A.logdatabase ='" + gpsd_dbName + "' and logsessiontime > '" + lastRefreshTime + "' "
                    + " AND logdebug not like '%pg_class%'" +
                    // TODO Added Where for Debug
                    //" and lower(logdebug) like '%where%' " +
                    "\t\tGROUP BY A.logsession, A.logcmdcount, A.logdatabase, A.loguser, A.logpid, A.loghost\n" +
                    "\t\tHAVING length(min(logdebug)) > 0";

            ResultSet rsCountQueries = dbConn.execQuery("SELECT COUNT(*) FROM (" + sql + ") AS X;");
            rsCountQueries.next();
            Integer totalNoOfQueries = rsCountQueries.getInt(1);

            ResultSet rs = dbConn.execQuery(sql);


            PreparedStatement statement = haystackDBConn.prepareStatement("INSERT INTO " + userSchema + ".queries  ( logsession, "
                    + " logcmdcount, logdatabase, loguser, logpid, logsessiontime, logtimemin, logtimemax, logduration, sql, id, cluster_id, qrytype, query_log_id, "
                    + " loghost) "
                    + " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

            Integer currCounter = 0;
            Integer lastPercentageUpdated = -1;
            while (rs.next()) {
                currCounter++;

                Integer currPercentage = (currCounter * 100 / totalNoOfQueries);
                if (currPercentage > lastPercentageUpdated) {
                    sql = "UPDATE " + haystackSchema + ".query_logs SET last_updated = now(), percent_processed = " + currPercentage + " where query_log_id = " + maxQryLogId + ";";
                    haystackDBConn.execNoResultSet(sql);
                    lastPercentageUpdated = currPercentage;
                }
                // Escape Quote in SQL Statement
                String logdebug = rs.getString(10);
                String escapedQuery = logdebug.replace("'", "\\\\'").trim();

                try {
                    String sQryType = this.getQueryType(escapedQuery);
                    ResultSet rsQryId = haystackDBConn.execQuery("select nextval('" + userSchema + ".seq_queries');");
                    rsQryId.next();
                    Integer query_id = rsQryId.getInt(1);
                    statement.clearParameters();
                    statement.setString(1, rs.getString(1));
                    statement.setString(2, rs.getString(2));
                    statement.setString(3, rs.getString(3));
                    statement.setString(4, rs.getString(4));
                    statement.setString(5, rs.getString(5));
                    statement.setTimestamp(6, rs.getTimestamp(6));
                    statement.setTimestamp(7, rs.getTimestamp(7));
                    statement.setTimestamp(8, rs.getTimestamp(8));

                    PGInterval pgi = (PGInterval) rs.getObject(9);
                    statement.setObject(9, pgi);
                    statement.setString(10, escapedQuery);
                    statement.setInt(11, query_id);
                    statement.setInt(12, clusterId);
                    statement.setString(13, sQryType);
                    statement.setInt(14, maxQryLogId);
                    statement.setString(15, rs.getString("loghost"));
                    statement.executeUpdate();

                    super.generateAST2(query_id, escapedQuery, userSchema);

                } catch (Exception e) {
                    log.error("Exception in processing query SQL='" + escapedQuery + "  Exception=" + e.toString());
                }
            }
            rs.close();

            // Calculate AST Summary Stats
            sql = "update " + userSchema + ".ast \n" +
                    "set count = X.count, total_duration = X.total_duration\n" +
                    "FROM \n" +
                    "\t(select ast_id, count(*) as count , sum(extract(epoch from C.logduration)) as total_duration\n" +
                    "\tfrom " + userSchema + ".ast_queries B, " + userSchema + ".queries C\n" +
                    "\twhere B.queries_id = C.id and query_log_id = " + maxQryLogId + " \n" +
                    "\tgroup by ast_id ) X\n" +
                    "WHERE ast.ast_id = X.ast_id;";
            haystackDBConn.execNoResultSet(sql);

            // Update Query Count and Sum Duration for Each Date for this QueryLogId
            sql = "UPDATE " + haystackSchema + ".query_log_dates set query_count = X.query_count, sum_duration = X.sum_duration " +
                    " FROM (select logsessiontime::date as log_date,count(*) as query_count, EXTRACT(EPOCH FROM sum(logduration)) as sum_duration " +
                    " FROM  " + userSchema + ".queries group by logsessiontime::date ) as X " +
                    " where query_log_dates.log_date = X.log_date and query_log_id = " + maxQryLogId + " ;";
            haystackDBConn.execNoResultSet(sql);

            // Recreate query_metadata table
            sql = " DROP TABLE IF EXISTS " + userSchema + ".query_metadata;";
            haystackDBConn.execNoResultSet(sql);

            sql = "CREATE TABLE " + userSchema + ".query_metadata( type text, value text );";
            haystackDBConn.execNoResultSet(sql);

            sql = "INSERT INTO " + userSchema + ".query_metadata ( type, value) SELECT distinct 'dbname', logdatabase FROM " + userSchema + ".queries;";
            haystackDBConn.execNoResultSet(sql);

            sql = "INSERT INTO " + userSchema + ".query_metadata ( type, value) SELECT distinct 'username', loguser FROM " + userSchema + ".queries;";
            haystackDBConn.execNoResultSet(sql);

            sql = "INSERT INTO " + userSchema + ".query_metadata ( type, value) SELECT distinct 'querytype', qrytype FROM " + userSchema + ".queries;";
            haystackDBConn.execNoResultSet(sql);

            // Update gpsd with query refresh date
            sql = "UPDATE " + haystackSchema + ".cluster set last_queries_refreshed_on = now() where cluster_id =" + clusterId + ";";
            haystackDBConn.execNoResultSet(sql);

            sql = "UPDATE " + haystackSchema + ".query_logs SET status = 'COMPLETED', completed_on = now() where query_log_id = " + maxQryLogId + ";";
            haystackDBConn.execNoResultSet(sql);

        } catch (Exception e) {
            log.error("Error is loading Queries for CLUSTER_ID" + clusterId +" Exception: " +e.toString());
            sql = "UPDATE " + haystackSchema + ".query_logs SET status = 'ERROR', last_updated = now() where query_log_id = " + maxQryLogId + ";";
            try {
                haystackDBConn.execNoResultSet(sql);
            } catch (Exception e1) {
                log.error("Error is updating query_log table for Cluster_ID" + clusterId + " Err=" + e1.toString());

            }
        }

    }

    public void generateRecommendations(int cluster_id, Tables tablelist){

        try {

            Credentials credentials = getCredentials(cluster_id);

            // Fetch Recommendation Engine settings from config.properties file
            Double columnarThresholdPercent = Double.valueOf(configProperties.properties.getProperty("re.columnarThresholdPercent"));
            Double topNPercent = Double.valueOf(configProperties.properties.getProperty("re.topNPercent"));
            Double bottomNPercent = Double.valueOf(configProperties.properties.getProperty("re.bottomNPercent"));

            Integer recId = 1;

            Iterator<Map.Entry<String, Table>> entries = tablelist.tableHashMap.entrySet().iterator();
            while (entries.hasNext()) {
                Map.Entry<String, Table> entry = entries.next();
                String currKey = entry.getKey();
                Table currTable = entry.getValue();

                if (currTable.joins.size() > 0) { // If there are no joins then ignore this Table
                    // A) Distribution Key:
                    //     Check if the current distibution key is being used in most of the join,
                    //     if not then recommend DK which is used in joins (higher workload score) for larger tables
                    Iterator<Map.Entry<String, Join>> joins = currTable.joins.entrySet().iterator();
                    float maxConfidence = -1;
//                    ArrayList<String> maxConfidenceCandidateDK = new ArrayList<String>();
                    TreeMap<Float, String> maxConfidenceCandidateDK = new TreeMap<Float, String>(new Comparator<Float>() {
                        @Override
                        public int compare(Float o1, Float o2) {
                            //Sorting in decending order.
                            return o2.compareTo(o1);
                        }
                    }); //Key = Confidence of Column, Value = Name Of Column

                    while (joins.hasNext()) {

                        Map.Entry<String, Join> join = joins.next();
                        String joinKey = join.getKey();
                        Join currJoin = join.getValue();

                        if (currJoin.getConfidence() > maxConfidence) {  // Current Join Usage Score is greater makes this the max one
                            for (Map.Entry<String, JoinTuple> entryJT : currJoin.joinTuples.entrySet()) {
                                if (currTable.tableName.equals(currJoin.leftTable)) {  // Add left column
//                                    maxConfidenceCandidateDK.add(entryJT.getValue().leftcolumn);
                                    maxConfidenceCandidateDK.put(currJoin.getConfidence(), entryJT.getValue().leftcolumn);
                                } else { // Add Right Column
//                                    maxConfidenceCandidateDK.add(entryJT.getValue().rightcolumn);
                                    maxConfidenceCandidateDK.put(currJoin.getConfidence(), entryJT.getValue().rightcolumn);
                                }
                            }
                            maxConfidence = currJoin.getConfidence();
                        }
                        // A.2) Check if attribute data types match for this join, if not add this recommendation
                        for (Map.Entry<String, JoinTuple> entryJT : currJoin.joinTuples.entrySet()) {
                            JoinTuple currJT = entryJT.getValue();
                            Column leftColumn = tablelist.findColumn(currJoin.leftSchema, currJoin.leftTable, currJT.leftcolumn, "");
                            Column rightColumn = tablelist.findColumn(currJoin.rightSchema, currJoin.rightTable, currJT.rightcolumn, "");

                            Boolean mismatch = false;
                            String desc = "Datatype mismatch for join between " + currJoin.leftSchema + "." + currJoin.leftTable + "." + currJT.leftcolumn +
                                    " and " + currJoin.rightSchema + "." + currJoin.rightTable + "." + currJT.rightcolumn;
                            String anamoly = leftColumn.isMatchDataType(rightColumn);

                            if (anamoly.length() > 0) {
                                Recommendation recommendation = createNewRecommendation(currTable, Recommendation.RecommendationType.DATATYPE);
                                recommendation.description = desc;
                                recommendation.anamoly = anamoly;
                                tablelist.recommendations.put(recId.toString(), recommendation);
                                recId++;
                            }
                        }
                    }
                    // A.1) Get Distribution Key, if it matches the candidateDK then ignore else add Recommendation with the CandidateDK

                    Iterator<Map.Entry<Float, String>> maxConfidenceCandidateDKIterator = maxConfidenceCandidateDK.entrySet().iterator();

                    String recDescTxt = "";
                    String recAnamolyTxt = "";

                    while(maxConfidenceCandidateDKIterator.hasNext()){
                        Map.Entry<Float, String> columnAndConfidenceValue = maxConfidenceCandidateDKIterator.next();
                        Float confidenceOfColumn = columnAndConfidenceValue.getKey();
                        String candidateColumn = columnAndConfidenceValue.getValue();

                        if (currTable.dk.containsKey(candidateColumn) == false) {
                            // DK and Candidate Key Mismatch, add recommendation for Candidate Key
                            recDescTxt += candidateColumn +",";
                            recAnamolyTxt += candidateColumn +": " +confidenceOfColumn +",";
                        }
                    }

                    //if recDescTxt ends with ',' then remove the trailling ','
                    if(recDescTxt.endsWith(",")){
                        recDescTxt = recDescTxt.substring(0, recDescTxt.lastIndexOf(","));
                    }

                    if(recAnamolyTxt.endsWith(",")){
                        recAnamolyTxt = recAnamolyTxt.substring(0, recAnamolyTxt.lastIndexOf(","));
                    }

                    //If recDescTxt is not empty that mean it contain a column which need to be recommended
                    if(recDescTxt.isEmpty() == false){
                        Recommendation recommendation = createNewRecommendation(currTable, Recommendation.RecommendationType.DK);
                        recommendation.description = "Distribution key should be set to " +recDescTxt +".";
                        recommendation.anamoly = "Confidence of key(s) " +recAnamolyTxt +".";
                        tablelist.recommendations.put(recId.toString(), recommendation);
                        recId++;
                    }
                }

                // Objective Measure of Interestingness, Recommendation should be generated for only TopNPercent of tables
                // B) Columnar & Compression Rule:
                //     Check to see if less than 30% (rs.ColumnarThresholdPercent) of attributes are used,
                //     if yes then check if the table is in TopNPercent in rows (re.topNPercent) threshold
                //     if yes then
                //             recommend columnar, if row storage
                //             recommend compression, if uncompressed
                Double avgColUsagePercent = currTable.stats.avgColUsage * 100 / currTable.stats.noOfColumns;

                if (avgColUsagePercent <= columnarThresholdPercent) {  // ColAvgUsage is less than ColumnThreshold
                    if ((100 - currTable.stats.percentile) < topNPercent) { // If Table in TopNPercent then
                        if (!currTable.stats.isColumnar) { // If Table is heap then recommend columnar
                            Recommendation recommendation = createNewRecommendation(currTable, Recommendation.RecommendationType.STORAGE);
                            recommendation.description = "Table " + currTable.schema + "." + currTable.tableName + " should be changed to columnar.";
                            recommendation.anamoly = "Table Percentile:" + currTable.stats.percentile + " and average column usage in queries:" + avgColUsagePercent + " Threshold: AvgColUsage="
                                    + columnarThresholdPercent + " topNPercent=" + topNPercent;
                            tablelist.recommendations.put(recId.toString(), recommendation);
                            recId++;
                        }
                        if (!currTable.stats.isCompressed) { // Recommend compressing the table if Cluster CPU Usage is less than 70% most of the time
                            Recommendation recommendation = createNewRecommendation(currTable, Recommendation.RecommendationType.COMPRESSION);
                            recommendation.description = "Table " + currTable.schema + "." + currTable.tableName + " should be compressed if cluster CPU is less than 70% threshold";
                            recommendation.anamoly = "Table Percentile:" + currTable.stats.percentile + " and average column usage in queries:" + avgColUsagePercent + " Threshold: AvgColUsage="
                                    + columnarThresholdPercent + " topNPercent=" + topNPercent;
                            tablelist.recommendations.put(recId.toString(), recommendation);
                            recId++;
                        }
                    }

                } else {
                    // B.2) If average column usage is greater than the threshold  (rs.ColumnarThresholdPercent) then
                    //      check if the storage type is columnar, then recommend heap storage
                    if (currTable.stats.isColumnar) {
                        Recommendation recommendation = createNewRecommendation(currTable, Recommendation.RecommendationType.STORAGE);
                        recommendation.description = "Table " + currTable.schema + "." + currTable.tableName + " should be changed to heap storage.";
                        recommendation.anamoly = "Table Percentile:" + currTable.stats.percentile + " and average column usage in queries:" + avgColUsagePercent + " Threshold: AvgColUsage="
                                + columnarThresholdPercent + " topNPercent=" + topNPercent;
                        tablelist.recommendations.put(recId.toString(), recommendation);
                        recId++;
                    }
                }

                // B.3) If the table is in bottomNPercent and if its columnar, then recommend heap
                //      storage and uncompressed
                if (currTable.stats.percentile <= bottomNPercent) {
                    if (currTable.stats.isColumnar) {
                        Recommendation recommendation = createNewRecommendation(currTable, Recommendation.RecommendationType.STORAGE);
                        recommendation.description = "Table " + currTable.schema + "." + currTable.tableName + " should be changed to heap storage.";
                        recommendation.anamoly = "Table Percentile:" + currTable.stats.percentile + " is in the BottomN% smallest tables in the Database. Threshold: bottomNPercent=" + bottomNPercent;
                        tablelist.recommendations.put(recId.toString(), recommendation);
                        recId++;
                    }
                }

                //Step1: Find That Table Is In TopNPercent Using Perito Principle

                HashMap<String, Column> columnsForSuggestion = new HashMap<String, Column>();

                if((100-topNPercent) <= currTable.stats.percentile){

                    //Step2: Get Those Columns Which Are Used In Where Clause And Add Them In 'columnsForSuggestion' List.

                    Iterator<Column> columnsIterator = currTable.columns.values().iterator();

                    while(columnsIterator.hasNext()){
                        Column column = columnsIterator.next();
                        int whereUsage = column.getWhereUsage();

                        if(whereUsage > 0){
                            columnsForSuggestion.put( column.column_name ,column);
                        }
                    }
                }

                //Step3: Find The Column With Max Cardenality For Recommendation.

                Column recommendedColumn = null;
                double maxCardenality = 0;
                int recommendedColumnDistinctNoOfRows = 0;

                Collection<Column> columns = columnsForSuggestion.values();

                for(Column column : columns){
                    int distinctNoOfRows = getDistinctNoOfRows(currTable.schema, currTable.tableName, column.column_name, credentials);

                    double cardenality = currTable.stats.noOfRows / distinctNoOfRows; //Finding Cardenality using formula : total no of rows in table / total no of distinct rows in column

                    if(cardenality > maxCardenality){
                        maxCardenality = cardenality;
                        recommendedColumn = column;
                        recommendedColumnDistinctNoOfRows = distinctNoOfRows;
                    }
                }

                // C) Partitions:
                //     Check to see if the table is partititioned
                if (currTable.stats.isPartitioned) {
                    //    if YES
                    //         then check if the partitioned attribute is used in most of the where clauses
                    //         give bias to date attribute, recommend two or three possible options for partition columns

                    //Recommandtaion type= Partition
                    //Desc: why we select the column, where uses = , cardinality = , distinct no of value = , rank # .

                    if(recommendedColumn != null) {

                        //If Table Is Partitioned On Any Column
                        if(!currTable.partitionColumn.isEmpty()) {
                            if(!currTable.partitionColumn.containsKey(recommendedColumn.column_name)){
                                Recommendation recommendation = createNewRecommendation(currTable, Recommendation.RecommendationType.PARTITION);
                                recommendation.description = String.format("Why we selected the column %s.%s, Where usage : %d, Cardinality : %.2f, Distinct no of values : %d", recommendedColumn.getResolvedTableName(), recommendedColumn.column_name, recommendedColumn.getWhereUsage(), maxCardenality, recommendedColumnDistinctNoOfRows);
                                tablelist.recommendations.put(recId.toString(), recommendation);
                                recId++;
                            }
                        }
                    }

                } else {
                    //     if NO
                    //         then check if table is in re.TopNPercent tables by rows
                    //         if YES
                    //             then identify the attribute which is used most frequently in where clauses

                    //Check if table is in topNPercent tables by row
                    if(recommendedColumn != null) {
                        if((100-topNPercent) <= currTable.stats.percentile) {
                            Recommendation recommendation = createNewRecommendation(currTable, Recommendation.RecommendationType.PARTITION);
                            recommendation.description = String.format("Why we selected the column %s.%s, Where usage : %d, Cardinality : %.2f, Distinct no of values : %d", recommendedColumn.getResolvedTableName(), recommendedColumn.column_name, recommendedColumn.getWhereUsage(), maxCardenality, recommendedColumnDistinctNoOfRows);
                            tablelist.recommendations.put(recId.toString(), recommendation);
                            recId++;
                        }
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private int getDistinctNoOfRows(String schema, String tableName, String columnName, Credentials credentials) throws SQLException, IOException, ClassNotFoundException {
        DBConnectService dbConnectService = new DBConnectService(DBConnectService.DBTYPE.POSTGRES);
        dbConnectService.setCredentials(credentials);

        int totalNoDistinctValues = 0;

        String sql = "SELECT count (DISTINCT " +tableName +"." +columnName +") as no_of_distinct_value FROM " +schema +"." +tableName;

        dbConnectService.connect(credentials);

        ResultSet result = dbConnectService.execQuery(sql);

        result.next();
        totalNoDistinctValues = result.getInt("no_of_distinct_value");

        return totalNoDistinctValues;
    }

}
