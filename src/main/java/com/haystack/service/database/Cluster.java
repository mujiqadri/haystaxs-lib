package com.haystack.service.database;

import com.google.gson.*;
import com.haystack.domain.Table;
import com.haystack.domain.Tables;
import com.haystack.parser.util.IntTypeAdapter;
import com.haystack.util.ConfigProperties;
import com.haystack.util.Credentials;
import com.haystack.util.DBConnectService;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Properties;

/**
 * Created by qadrim on 16-02-04.
 */
public abstract class Cluster {
    protected ConfigProperties configProperties;
    protected Properties properties;
    protected String haystackSchema;
    protected DBConnectService.DBTYPE dbtype;

    protected DBConnectService dbConn;
    DBConnectService haystackDBConn = new DBConnectService(DBConnectService.DBTYPE.POSTGRES);


    protected final Logger log = LoggerFactory.getLogger(Cluster.class.getName());

    public boolean connect(Credentials credentials) {
        try {
            dbConn = new DBConnectService(dbtype);
            dbConn.connect(credentials);
        } catch (Exception e) {
            log.error("Error connecting to cluster, " + credentials.toString() + " Exception:" + e.toString());
            return false;
        }
        return true;
    }

    public abstract Tables loadTables(Credentials credentials, Boolean isGPSD);

    public abstract void loadQueries(Integer clusterId, Timestamp lastRefreshTime);

    public Cluster() {
        try {

            dbConn = new DBConnectService(dbtype, "");
            configProperties = new ConfigProperties();
            configProperties.loadProperties();
            properties = configProperties.properties;
            this.haystackSchema = this.properties.getProperty("main.schema");
            haystackDBConn.connect(this.configProperties.getHaystackDBCredentials());

        } catch (Exception e) {

        }
    }

    public void saveGpsdStats(int gpsdId, Tables tables) {
        try {
            // TODO: This should be kept so that the Schema size can be compared later on..
            String sql = "delete from " + haystackSchema + ".gpsd_stats where gpsd_id = " + gpsdId;

            // Don't delete gpsd_stats keep it for history
            // haystackDBConn.execNoResultSet(sql);

            for (Table table : tables.tableHashMap.values()) {
                HashMap<String, Object> mapValues = new HashMap<>();

                mapValues.put("gpsd_id", gpsdId);
                mapValues.put("schema_name", table.schema);
                mapValues.put("table_name", table.tableName);
                mapValues.put("size_in_mb", table.stats.sizeOnDisk * 1024);
                mapValues.put("no_of_rows", table.stats.noOfRows);

                try {
                    haystackDBConn.insert(haystackSchema + ".gpsd_stats", mapValues);
                } catch (Exception ex) {
                    log.error("Error inserting gpsd stats in gpsd_stats table for gpsd_id = " + gpsdId + " and for table = " + table.tableName + " ;Exception:" + ex.toString());
                    //HSException hsException = new HSException("CatalogService.getGPSDJson()", "Error in getting Json for GPSD", e.toString(), "gpsd_id=" + gpsd_id, user_id);
                }
            }
        } catch (Exception e) {
            log.error("Error deleting rows from " + haystackSchema + ".gpsd_stats for gpsd_id = " + gpsdId + " ;Exception:" + e.toString());
        }
    }
    protected void processQueries(String tempQueryTable, Integer clusterId) {
        String sql = "";
        try {
            sql = "select user_name, B.user_id from haystack_ui.gpsd_users A, haystack_ui.users B\n" +
                    "where A.user_id = B.user_id and gpsd_id = " + clusterId;
            ResultSet rsUser = haystackDBConn.execQuery(sql);
            rsUser.next();
            String userSchema = rsUser.getString("user_name");
            Integer userId = rsUser.getInt("user_id");

            // Get Max QueryLogId
            sql = "select max(query_log_id)+1 max_qry_log_id FROM " + haystackSchema + ".query_logs;";
            ResultSet rsQryLogId = haystackDBConn.execQuery(sql);
            rsQryLogId.next();

            Integer maxQryLogId = rsQryLogId.getInt("max_qry_log_id");

            // Insert record in QUERY_LOG table
            sql = "INSERT INTO " + haystackSchema + ".query_logs(query_log_id, submitted_on, user_id, status, original_file_name," +
                    " file_checksum,created_on, gpsd_id) VALUES (" + maxQryLogId + ",now()," + userId + ",'SUBMITTED','SCHEDULED_REFRESH'," +
                    " '" + maxQryLogId + "' || now(), now()," + clusterId + " );";
            haystackDBConn.execNoResultSet(sql);

            // Create Schema and Tables with Partitions
            try {
                createUserSchemaTables(userSchema, tempQueryTable, maxQryLogId);
            } catch(Exception ex) {
                log.warn("User schema {} has already been created.", userSchema);
            }

            sql = "INSERT INTO " + userSchema + ".queries (logsession, logcmdcount,logdatabase, loguser, logpid, logsessiontime, logtimemin, logtimemax, logduration, sql, gpsd_id)" +
                    "SELECT logsession, logcmdcount,logdatabase, loguser, logpid, logsessiontime, logtimemin, logtimemax, logduration, sql," + clusterId + " FROM " + tempQueryTable + ";";
            haystackDBConn.execNoResultSet(sql);

            // Categorize queries
            sql = "UPDATE " + userSchema + ".queries SET QRYTYPE = case " +
                    " when upper(sql) like '%SET%' THEN 'SET CONFIGURATION'" +
                    " when upper(sql) like '%SELECT%' THEN 'SELECT' " +
                    " when upper(sql) like '%INSERT INTO%' THEN 'INSERT' " +
                    " when upper(sql) like '%COMMIT%' THEN 'COMMIT' " +
                    " when upper(sql) like '%SELECT%FROM%GPTEXT.SEARCH_COUNT%' THEN 'GPTEXT.SEARCH_COUNT' " +
                    " when upper(sql) like '%SELECT%FROM%GPTEXT.INDEX%' THEN 'GPTEXT.INDEX' " +
                    " when upper(sql) like '%SELECT%FROM%GPTEXT.INDEX_STATISTICS%' THEN 'GPTEXT.IDX_STATS' " +
                    " when upper(sql) like '%DROP TABLE%' THEN 'DROP TABLE' " +
                    " when upper(sql) like '%BEGIN WORK%LOCK TABLE%' THEN 'EXCLUSIVE LOCK' " +
                    " when upper(sql) like '%CREATE TABLE%' THEN 'CREATE TABLE' " +
                    " when upper(sql) like '%DROP TABLE%' THEN 'DROP TABLE'" +
                    " when upper(sql) like '%TRUNCATE%' THEN 'TRUNCATE TABLE'" +
                    " when sql like 'unlisten *' THEN 'INTERNAL' " +
                    " when upper(sql) like '%UPDATE%' THEN 'UPDATE' " +
                    " when upper(sql) like '%CREATE%EXTERNAL%TABLE%' THEN 'CREATE EXTERNAL TABLE' " +
                    " when upper(sql) like '%DELETE%FROM%' THEN 'DELETE' " +
                    " when upper(sql) like '%BEGIN%' THEN 'TRANSACTION-OPERATION' " +
                    " when upper(sql) like '%ROLLBACK%' THEN 'TRANSACTION-OPERATION' " +
                    " when upper(sql) like '%SAVEPOINT%' THEN 'TRANSACTION-OPERATION' " +
                    " when upper(sql) like '%RELEASE%' THEN 'TRANSACTION-OPERATION' " +
                    " when upper(sql) like '%TRANSACTION%' THEN  'TRANSACTION-OPERATION' " +
                    " when upper(sql) like '%SHOW%' THEN 'SHOW' " +
                    " when sql like '%;%' THEN 'MULTIPLE SQL STATEMENTS' else 'OTHERS' end;";
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

            sql = "CREATE TABLE " + userSchema + ".query_metadata( type text, value text )WITH ( OIDS=FALSE ) DISTRIBUTED BY (type);";
            haystackDBConn.execNoResultSet(sql);

            sql = "INSERT INTO " + userSchema + ".query_metadata ( type, value) SELECT distinct 'dbname', logdatabase FROM " + userSchema + ".queries;";
            haystackDBConn.execNoResultSet(sql);

            sql = "INSERT INTO " + userSchema + ".query_metadata ( type, value) SELECT distinct 'username', loguser FROM " + userSchema + ".queries;";
            haystackDBConn.execNoResultSet(sql);

            sql = "INSERT INTO " + userSchema + ".query_metadata ( type, value) SELECT distinct 'querytype', qrytype FROM " + userSchema + ".queries;";
            haystackDBConn.execNoResultSet(sql);

        } catch (Exception e) {
            log.error("Error is creating schema and tables to ProcessQueryLog, gpsd_id={}; Exception msg: ", clusterId, e.getMessage());
        }
    }

    public void createUserSchemaTables(String userSchema) {
        // 01. Check User Schema Exists
        String sql = "select count(*) as count from pg_catalog.pg_namespace where nspname = '" + userSchema + "'";

        try {
            haystackDBConn.connect(configProperties.getHaystackDBCredentials());

            ResultSet rsCount = haystackDBConn.execQuery(sql);
            rsCount.next();
            if (rsCount.getInt("count") == 0) { // Create Schema
                sql = "CREATE SCHEMA " + userSchema + ";";
                haystackDBConn.execNoResultSet(sql);
            }

            // 02. Create AST Table
            sql = "select count(*) as count  from information_schema.tables where upper(table_schema) = upper('" + userSchema + "') and upper(table_name) = upper('ast')";
            rsCount = haystackDBConn.execQuery(sql);
            rsCount.next();
            if (rsCount.getInt("count") == 0) { // Create AST Table
                sql = "CREATE TABLE " + userSchema + ".ast ( ast_id serial , ast_json text, checksum text) WITH (APPENDONLY=true, COMPRESSTYPE=quicklz, OIDS=FALSE )DISTRIBUTED BY (ast_id);";
                haystackDBConn.execNoResultSet(sql);
            }

            // 03. Create AST Queries Table
            sql = "select count(*) as count  from information_schema.tables where upper(table_schema) = upper('" + userSchema + "') and upper(table_name) = upper('ast_queries')";
            rsCount = haystackDBConn.execQuery(sql);
            rsCount.next();
            if (rsCount.getInt("count") == 0) { // Create AST Queries Table
                sql = "CREATE TABLE " + userSchema + ".ast_queries ( ast_queries_id serial , queries_id integer NOT NULL,ast_json text,checksum text,ast_id integer NOT NULL)WITH (APPENDONLY=true, COMPRESSTYPE=quicklz, \n" +
                        " OIDS=FALSE)DISTRIBUTED BY (queries_id);";
                haystackDBConn.execNoResultSet(sql);
            }

            // 04. Create Queries Table
            sql = "select count(*) as count  from information_schema.tables where upper(table_schema) = upper('" + userSchema + "') and upper(table_name) = upper('queries')";
            rsCount = haystackDBConn.execQuery(sql);
            rsCount.next();
            if (rsCount.getInt("count") == 0) { // Create Queries Table
                sql = "CREATE TABLE " + userSchema + ".queries ( id serial ,logsession text,logcmdcount text, logdatabase text," +
                        "loguser text,logpid text,logsessiontime timestamp with time zone,logtimemin timestamp with time zone, " +
                        " logtimemax timestamp with time zone,logduration interval,sql text,qrytype text, gpsd_id integer)WITH (APPENDONLY=true, COMPRESSTYPE=quicklz, \n" +
                        " OIDS=FALSE) DISTRIBUTED BY (id) " +
                        " PARTITION BY RANGE(logsessiontime) ( START (date '1900-01-01') INCLUSIVE END ( date '1900-01-02') EXCLUSIVE\n" +
                        " EVERY (INTERVAL '1 day'));";
                haystackDBConn.execNoResultSet(sql);

                //Create query_metadata table
                sql = "CREATE TABLE " + userSchema + ".query_metadata( type text, value text )WITH ( OIDS=FALSE ) DISTRIBUTED BY (type);";
                haystackDBConn.execNoResultSet(sql);
            }
        } catch (Exception e) {
            log.error(e.toString());
        }
    }
    private void createUserSchemaTables(String userSchema, String tempQueryTable, Integer maxQryLogId) throws Exception {

        // 01. Check User Schema Exists
        String sql = "select count(*) as count from pg_catalog.pg_namespace where nspname = '" + userSchema + "'";

        try {
            ResultSet rsCount = haystackDBConn.execQuery(sql);
            rsCount.next();
            if (rsCount.getInt("count") == 0) { // Create Schema
                sql = "CREATE SCHEMA " + userSchema + ";";
                haystackDBConn.execNoResultSet(sql);
            }

            // 02. Create AST Table
            sql = "select count(*) as count  from information_schema.tables where upper(table_schema) = upper('" + userSchema + "') and upper(table_name) = upper('ast')";
            rsCount = haystackDBConn.execQuery(sql);
            rsCount.next();
            if (rsCount.getInt("count") == 0) { // Create AST Table
                sql = "CREATE TABLE " + userSchema + ".ast ( ast_id serial , ast_json text, checksum text) WITH (APPENDONLY=true, COMPRESSTYPE=quicklz, OIDS=FALSE )DISTRIBUTED BY (ast_id);";
                haystackDBConn.execNoResultSet(sql);
            }

            // 03. Create AST Queries Table
            sql = "select count(*) as count  from information_schema.tables where upper(table_schema) = upper('" + userSchema + "') and upper(table_name) = upper('ast_queries')";
            rsCount = haystackDBConn.execQuery(sql);
            rsCount.next();
            if (rsCount.getInt("count") == 0) { // Create AST Queries Table
                sql = "CREATE TABLE " + userSchema + ".ast_queries ( ast_queries_id serial , queries_id integer NOT NULL,ast_json text,checksum text,ast_id integer NOT NULL)WITH (APPENDONLY=true, COMPRESSTYPE=quicklz, \n" +
                        " OIDS=FALSE)DISTRIBUTED BY (queries_id);";
                haystackDBConn.execNoResultSet(sql);
            }

            // 04. Create Queries Table
            sql = "select count(*) as count  from information_schema.tables where upper(table_schema) = upper('" + userSchema + "') and upper(table_name) = upper('queries')";
            rsCount = haystackDBConn.execQuery(sql);
            rsCount.next();
            if (rsCount.getInt("count") == 0) { // Create Queries Table
                sql = "CREATE TABLE " + userSchema + ".queries ( id serial ,logsession text,logcmdcount text, logdatabase text," +
                        "loguser text,logpid text,logsessiontime timestamp with time zone,logtimemin timestamp with time zone, " +
                        " logtimemax timestamp with time zone,logduration interval,sql text,qrytype text, gpsd_id integer )WITH (APPENDONLY=true, COMPRESSTYPE=quicklz, \n" +
                        " OIDS=FALSE) DISTRIBUTED BY (id) " +
                        " PARTITION BY RANGE(logsessiontime) ( START (date '1900-01-01') INCLUSIVE END ( date '1900-01-02') EXCLUSIVE\n" +
                        " EVERY (INTERVAL '1 day'));";
                haystackDBConn.execNoResultSet(sql);
            }

            // CREATE PARTITION - find the distinct dates from the external table to create partitions
            sql = "SELECT logsessiontime::date as logsessiontime FROM " + tempQueryTable + " group by logsessiontime::date";

            ResultSet rsDates = haystackDBConn.execQuery(sql);

            while (rsDates.next()) {
                String monthpartition_name = rsDates.getString("logsessiontime");
                sql = "SELECT count(*) as count FROM pg_partitions WHERE partitionname = '" + monthpartition_name + "' " +
                        " AND lower(tablename) = lower('queries') " + " AND lower(schemaname) = '" + userSchema + "';";
                rsCount = haystackDBConn.execQuery(sql);
                rsCount.next();

                if (rsCount.getInt("count") == 0) {
                    // Create Month Partition
                    sql = "ALTER TABLE " + userSchema + ".queries ADD PARTITION \"" + monthpartition_name + "\" START ('" + monthpartition_name + " 00:00:00.000') " +
                            " INCLUSIVE END ('" + monthpartition_name + " 23:59:59.999') EXCLUSIVE;";
                    haystackDBConn.execNoResultSet(sql);
                }

                // Check if query_log_dates has entry for the current date, if not insert a row
                // Insert record in query_log and query_log_dates tables
                sql = "SELECT COUNT(*) as count FROM " + haystackSchema + ".query_log_dates where query_log_id=" + maxQryLogId + " AND log_date = '" + monthpartition_name + "';";
                rsCount = haystackDBConn.execQuery(sql);
                rsCount.next();

                if (rsCount.getInt("count") == 0) {
                    sql = " INSERT INTO " + haystackSchema + ".query_log_dates(query_log_id, log_date) VALUES(" + maxQryLogId + ",'" + monthpartition_name + "');";
                    haystackDBConn.execNoResultSet(sql);
                }

                sql = "CREATE TABLE " + userSchema + ".query_metadata( type text, value text )WITH ( OIDS=FALSE ) DISTRIBUTED BY (type);";
                haystackDBConn.execNoResultSet(sql);
            }
        } catch (Exception e) {
            throw e;
        }

    }

    protected String getSizePretty(float sizeOnDisk) {
        String result = "";
        float newSize = 0;

        if (sizeOnDisk > 1024) { // size greater than a Terabyte, convert it to TB
            newSize = sizeOnDisk / 1024;
            result = String.format("%.2f", newSize);
            result += " TB";
        }
        if ((sizeOnDisk < 1024) && (sizeOnDisk >= 1)) { // size should remain in GB
            result = String.format("%.2f", sizeOnDisk);
            result += " GB";
        }
        if (sizeOnDisk < 1) {  // size should be in MB
            newSize = sizeOnDisk * 1024;
            result = String.format("%.2f", newSize);
            result += " MB";
        }

        return result;
    }

    protected String getJSON() {
        ObjectMapper mapper = new ObjectMapper();
        String sw = "";
        try {
            //Gson objGson = new Gson();
            GsonBuilder gsonBuilder = new GsonBuilder();

            gsonBuilder.registerTypeAdapter(Double.class, new JsonSerializer<Double>() {
                @Override
                public JsonElement serialize(final Double src, final Type typeOfSrc, final JsonSerializationContext context) {
                    try {
                        BigDecimal value = BigDecimal.valueOf(src);
                        return new JsonPrimitive(value);
                    } catch (Exception e) {
                        return new JsonPrimitive(-1);
                    }

                }
            });
            gsonBuilder.registerTypeAdapter(int.class, new IntTypeAdapter());
            gsonBuilder.registerTypeAdapter(Integer.class, new IntTypeAdapter()).create();

            //gson = gsonBuilder.create();

            Gson objGson = gsonBuilder.setPrettyPrinting().create();
            sw = objGson.toJson(this);
        } catch (Exception e) {
            log.error("Error generating json " + e.toString());
        }
        return sw.toString();
    }
}
