package com.haystack.service.database;

import com.haystack.domain.*;
import com.haystack.util.ConfigProperties;
import com.haystack.util.Credentials;
import com.haystack.util.DBConnectService;
import org.postgresql.util.PGInterval;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Created by qadrim on 16-02-04.
 */
public class Redshift extends Cluster {

    public Redshift() {

        this.dbtype = DBConnectService.DBTYPE.REDSHIFT;
    }

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

            String sqlTbl = "SELECT tbl.table_id as table_oid, tbl.schema as schema_name, tbl.table as table_name,\n" +
                    "\ttbl.tbl_rows as NoOfRows,\n" +
                    "\ttbl.size as relPages, -- Size of the table, in 1 MB data blocks.\n" +
                    "\t(tbl.size::numeric / (1024)) as SizeInGB,\n" +
                    "\t'columnar' as storage_Mode,\n" +
                    "\tCOL.num_cols as noOfColumns,\n" +
                    "\t't' as IsColumnar,\n" +
                    "\tencoded as CompressTYpe,\n" +
                    "\t0 as IsPartitioned,\n" +
                    "\ttbl.diststyle as diststyle\n" +
                    "\n" +
                    "FROM SVV_TABLE_INFO TBL \n" +
                    "INNER JOIN ( select nspname, relname, max(attnum) as num_cols\n" +
                    "from pg_attribute a, pg_namespace n, pg_class c\n" +
                    "where n.oid = c.relnamespace and  a.attrelid = c.oid\n" +
                    "and c.relname not like '%pkey'\n" +
                    "and n.nspname not like 'pg%'\n" +
                    "and n.nspname not like 'information%'\n" +
                    "group by 1, 2 ) as COL\n" +
                    "ON TBL.schema = COL.nspname AND TBL.table = COL.relname";

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
                    //tbl.stats.compressLevel = Integer.parseInt(rsTbl.getString("compressLevel"));
                    //tbl.stats.compressionRatio = Float.parseFloat(rsTbl.getString("compressRatio"));
                    //tbl.stats.skew = Float.parseFloat(rsTbl.getString("skew"));
                    tbl.stats.sizeForDisplayCompressed = getSizePretty(tbl.stats.sizeOnDisk); // Normalize size and unit to display atleast 2 digits
                    tbl.stats.sizeForDisplayUnCompressed = getSizePretty(tbl.stats.sizeUnCompressed); // Normalize size and unit to display atleast 2 digits
                    tbl.distributionType = rsTbl.getString("diststyle"); // EVEN, KEY, ALL


                    //If size of the table is 0
                    if (tbl.stats.sizeUnCompressed == 0) {
                        noOfTablesWithZeroSize++;

                        //calculate the percentage of those tables which have 0 size
                        float percentage = (noOfTablesWithZeroSize / totalNoOfQueries) * 100;

                        //if percentage  25% or greater then throw an exception
                        if (percentage >= analyzePercentage) {
                            throw new RuntimeException("Statistics not collected, please run Analyze on the Tables");
                        }
                    }

                    // Get Column Details

                    String sqlCol = "    select cast(column_name as varchar(100)), cast(ordinal_position as int), cast(column_default as varchar(100)), cast(is_nullable as varchar(20)) ," +
                            " cast(udt_name as varchar(50)) as data_type  ,cast(character_maximum_length as int),\n" +
                            "\tc.numeric_precision, c.numeric_precision_radix, c.numeric_scale, \n" +
                            "     sort_col_order  , decode(d.colname,null,0,1) isdk , e.enc\n" +
                            "    from \n" +
                            "\t (-- need tableid\n" +
                            "\t SELECT substring(n.nspname,1,100) as schemaname, substring(c.relname,1,100) as tablename, c.oid as tableid \n" +
                            "\t FROM pg_namespace n, pg_class c\n" +
                            "\t WHERE n.oid = c.relnamespace \n" +
                            "\t   AND nspname NOT IN ('pg_catalog', 'pg_toast', 'information_schema')" +
                            "     AND n.nspname = '" + tbl.schema + "' and c.relname = '" + tbl.tableName + "'  \n" +
                            "\t ) t \n" +
                            "    left join\n" +
                            "    (select * from information_schema.columns) c\n" +
                            "\ton  c.table_schema= t.schemaname and c.table_name=t.tablename\n" +
                            "    left join \n" +
                            "    (-- gives sort cols\n" +
                            "    select attrelid as tableid, attname as colname, attsortkeyord as sort_col_order from pg_attribute a where \n" +
                            "     a.attnum > 0  AND NOT a.attisdropped AND a.attsortkeyord > 0\n" +
                            "    ) s on t.tableid=s.tableid and c.column_name=s.colname\n" +
                            "    left join \n" +
                            "    (-- gives encoding\n" +
                            "    select attrelid as tableid, attname as colname, format_encoding(a.attencodingtype::integer) AS enc from pg_attribute a where \n" +
                            "     a.attnum > 0  AND NOT a.attisdropped \n" +
                            "    ) e on t.tableid=e.tableid and c.column_name=e.colname\n" +
                            "    left join \n" +
                            "    -- gives dist col\n" +
                            "    (select attrelid as tableid, attname as colname from pg_attribute a where\n" +
                            "     a.attnum > 0 AND NOT a.attisdropped  AND a.attisdistkey = 't'\n" +
                            "    ) d on t.tableid=d.tableid and c.column_name=d.colname\n" +
                            "\twhere table_name is not null\n" +
                            "    order by 1,5\n";


                    ResultSet rsCol = dbConn.execQuery(sqlCol);

                    while (rsCol.next()) {
                        Column col = new Column();
                        col.column_name = rsCol.getString("column_name");
                        col.ordinal_position = rsCol.getInt("ordinal_position");
                        col.data_type = rsCol.getString("data_type");
                        col.isDK = Boolean.parseBoolean(rsCol.getString("isdk"));
                        if (col.isDK) {
                            tbl.dkArray = col.ordinal_position.toString();
                        }
                        col.character_maximum_length = rsCol.getInt("character_maximum_length");
                        col.numeric_precision = rsCol.getInt("numeric_precision");
                        col.numeric_precision_radix = rsCol.getInt("numeric_precision_radix");
                        col.numeric_scale = rsCol.getInt("numeric_scale");
                        col.sort_col_order = rsCol.getInt("sort_col_order");
                        col.encoding = rsCol.getString("enc");
                        tbl.columns.put(col.column_name, col);
                    }
                    rsCol.close();
                    String key = tbl.schema + ":" + tbl.tableName;
                    //tbl.setDistributionKey(); Not Required for Redshfit
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


            // Now Process Queries Generating AST for each query

            sql = "select X.query as logsession, NoOfLines as logcmdcount, X.database as logdatabase, usr.usename as loguser, X.pid as logpid, starttime as logsessiontime,\n" +
                    "starttime as logtimemin, endtime as logtimemax, (endtime - starttime) as logduration,\n" +
                    "Y.listagg as sql , Z.remotehost as loghost, len(Y.listagg),cast(extract(epoch from (endtime - starttime)*1000) as numeric(18,3)) as logdurationmilliseconds\n" +
                    "from stl_query X INNER JOIN pg_user usr\n" +
                    "\tON X.userid = usr.usesysid\n" +
                    "inner join (\n" +
                    "\tselect query, count(*) NoOfLines, listagg(text)\n" +
                    "\twithin group(order by sequence)\n" +
                    "\tfrom stl_querytext\n" +
                    "\tgroup by query\n" +
                    "\n" +
                    "     )  Y USING (query) \n" +
                    "left outer join (\n" +
                    "\t\t\tselect pid, remotehost, username\n" +
                    "\t\t\tfrom STL_CONNECTION_LOG \n" +
                    "\t\t\twhere event = 'initiating session'\n" +
                    "\t\t) Z ON (X.pid = Z.pid AND usr.usename = Z.username) \n" +
                    "where X.starttime > '" + lastRefreshTime + "' and X.database = '" + gpsd_dbName + "'\n" +
                    "order by starttime asc";


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

            // Insert QUERY_LOG_DATES
            sql = "insert into haystack_ui.query_log_dates  select query_log_id, cast(logsessiontime as date), count(*), sum(extract(epoch from logtimemax-logtimemin))\n" +
                    "\n" +
                    "from cluster_dot_admin_at_haystaxs_dot_com.queries \n" +
                    "where query_log_id = " + maxQryLogId + "  \n" +
                    "group by query_log_id,cast(logsessiontime as date) ";

            haystackDBConn.execNoResultSet(sql);

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
            log.error("Error is loading Queries for CLUSTER_ID" + clusterId + " Exception: " + e.toString());
            sql = "UPDATE " + haystackSchema + ".query_logs SET status = 'ERROR', last_updated = now() where query_log_id = " + maxQryLogId + ";";
            try {
                haystackDBConn.execNoResultSet(sql);
            } catch (Exception e1) {
                log.error("Error is updating query_log table for Cluster_ID" + clusterId + " Err=" + e1.toString());

            }
        }

    }

    @Override
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


}
