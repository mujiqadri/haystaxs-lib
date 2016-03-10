package com.haystack.service.database;

import com.haystack.domain.*;
import com.haystack.util.Credentials;
import com.haystack.util.DBConnectService;

import javax.swing.text.StyledEditorKit;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
    public Tables loadTables(Credentials credentials, Boolean isGPSD) {

        Tables tables = new Tables();

        String jsonResult = "";
        try {
            dbConn = new DBConnectService(this.dbtype);
            dbConn.connect(credentials);

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
                    tables.tableHashMap.put(key, tbl);
                } catch (Exception e) {
                    log.error("Error in loading Table:" + tbl.schema + "." + tbl.tableName + " ERR=" + e.toString());
                }
            }
            rsTbl.close();
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
            sql = "select user_name, B.user_id from haystack_ui.gpsd_users A, haystack_ui.users B\n" +
                    "where A.user_id = B.user_id and is_default = true and gpsd_id = " + clusterId;
            ResultSet rsUser = haystackDBConn.execQuery(sql);
            rsUser.next();
            String userSchema = rsUser.getString("user_name");
            Integer userId = rsUser.getInt("user_id");

            // Get Max QueryLogId
            //sql = "select max(query_log_id)+1 max_qry_log_id FROM " + haystackSchema + ".query_logs;";
            sql = "select nextval('" + haystackSchema + ".seq_query_log');";
            ResultSet rsQryLogId = haystackDBConn.execQuery(sql);
            rsQryLogId.next();

            maxQryLogId = rsQryLogId.getInt(1);

            // Insert record in QUERY_LOG table
            sql = "INSERT INTO " + haystackSchema + ".query_logs(query_log_id, user_id, status, original_file_name," +
                    " file_checksum,submitted_on, gpsd_id) VALUES (" + maxQryLogId + "," + userId + ",'SUBMITTED','SCHEDULED_REFRESH'," +
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
                    "\t\tWHERE A.logsession IS NOT NULL AND A.logcmdcount IS NOT NULL AND A.logdatabase IS NOT NULL and logsessiontime > '" + lastRefreshTime + "' " +
                    "\t\tAND length(logdebug) > 0;";

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

            sql = "SELECT A.logsession, A.logcmdcount, A.logdatabase, A.loguser, A.logpid, min(A.logtime) logsessiontime, min(A.logtime) AS logtimemin,\n" +
                    "                 max(A.logtime) AS logtimemax, max(A.logtime) - min(A.logtime) AS logduration, min(logdebug) as sql\n" +
                    "\t\tFROM gp_toolkit.__gp_log_master_ext A\n" +
                    "\t\tWHERE A.logsession IS NOT NULL AND A.logcmdcount IS NOT NULL AND A.logdatabase IS NOT NULL and logsessiontime > '" + lastRefreshTime + "' " +
                    "\t\tGROUP BY A.logsession, A.logcmdcount, A.logdatabase, A.loguser, A.logpid\n" +
                    "\t\tHAVING length(min(logdebug)) > 0";

            ResultSet rsCountQueries = dbConn.execQuery("SELECT COUNT(*) FROM (" + sql + ") AS X;");
            rsCountQueries.next();
            Integer totalNoOfQueries = rsCountQueries.getInt(1);

            ResultSet rs = dbConn.execQuery(sql);


            PreparedStatement statement = haystackDBConn.prepareStatement("INSERT INTO " + userSchema + ".queries  ( logsession, "
                    + " logcmdcount, logdatabase, loguser, logpid, logsessiontime, logtimemin, logtimemax, logduration, sql, id, gpsd_id, qrytype) "
                    + " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)");

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
                String escapedQuery = logdebug.replace("'", "\\'");

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
                    statement.executeUpdate();

                    super.generateAST(query_id, escapedQuery, userSchema);

                } catch (Exception e) {
                    log.error("Exception in processing query SQL='" + escapedQuery + "  Exception=" + e.toString());
                }
            }
            rs.close();


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
            sql = "UPDATE " + haystackSchema + ".gpsd set last_queries_refreshed_on = now() where gpsd_id =" + clusterId + ";";
            haystackDBConn.execNoResultSet(sql);

            sql = "UPDATE " + haystackSchema + ".query_logs SET status = 'COMPLETED', completed_on = now() where query_log_id = " + maxQryLogId + ";";
            haystackDBConn.execNoResultSet(sql);

        } catch (Exception e) {
            log.error("Error is loading Queries for GPSD_ID" + clusterId);
            sql = "UPDATE " + haystackSchema + ".query_logs SET status = 'ERROR', last_updated = now() where query_log_id = " + maxQryLogId + ";";
            try {
                haystackDBConn.execNoResultSet(sql);
            } catch (Exception e1) {
                log.error("Error is updating query_log table for GPSD_ID" + clusterId + " Err=" + e1.toString());

            }
        }

    }


}
