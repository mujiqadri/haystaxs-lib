package com.haystack.service.database;

import com.google.gson.*;
import com.haystack.domain.Table;
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


    protected HashMap<String, Table> tableHashMap;

    protected static Logger log = LoggerFactory.getLogger(Cluster.class.getName());

    public boolean connect(Credentials credentials) {
        try {
            dbConn.connect(credentials);
            haystackDBConn.connect(this.configProperties.getHaystackDBCredentials());
        } catch (Exception e) {
            log.error("Error connecting to cluster, " + credentials.toString() + " Exception:" + e.toString());
            return false;
        }
        return true;
    }

    public abstract String loadTables(boolean returnJson);

    public abstract void loadQueries(Integer clusterId, Timestamp lastRefreshTime);

    public abstract void refreshTableStats(Integer clusterId);

    public Cluster() {
        try {
            dbConn = new DBConnectService(dbtype, "");
            configProperties = new ConfigProperties();
            configProperties.loadProperties();
            properties = configProperties.properties;
            this.haystackSchema = this.properties.getProperty("main.schema");

        } catch (Exception e) {

        }
    }

    protected void processQueries(String tempQueryTable, Integer clusterId) {
        try {
            String sql = "select user_name from haystack_ui.gpsd_users A, haystack_ui.users B\n" +
                    "where A.user_id = B.user_id and gpsd_id = " + clusterId;
            ResultSet rsUser = haystackDBConn.execQuery(sql);
            rsUser.next();
            String userSchema = rsUser.getString("user_name");

            // Create Schema and Tables with Partitions
            createUserSchemaTables(userSchema, tempQueryTable);

            sql = "INSERT INTO " + userSchema + ".queries (logsession, logcmdcount,logdatabase, loguser, logpid, logsessiontime, logtimemin, logtimemax, logduration, sql)" +
                    "SELECT logsession, logcmdcount,logdatabase, loguser, logpid, logsessiontime, logtimemin, logtimemax, logduration, sql FROM " + tempQueryTable + ";";
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
                    " where query_log_dates.log_date = X.log_date;";
            haystackDBConn.execNoResultSet(sql);

            // Recreate query_metadata table
            sql = " DROP TABLE IF EXISTS " + userSchema + ".query_metadata;";
            haystackDBConn.execNoResultSet(sql);

            sql = "CREATE TABLE " + userSchema + ".query_metadata( type text, value text ); ";
            haystackDBConn.execNoResultSet(sql);

            sql = "INSERT INTO " + userSchema + ".query_metadata ( type, value) SELECT distinct 'dbname', logdatabase FROM " + userSchema + ".queries;";
            haystackDBConn.execNoResultSet(sql);

            sql = "INSERT INTO " + userSchema + ".query_metadata ( type, value) SELECT distinct 'username', loguser FROM " + userSchema + ".queries;";
            haystackDBConn.execNoResultSet(sql);

            sql = "INSERT INTO " + userSchema + ".query_metadata ( type, value) SELECT distinct 'querytype', qrytype FROM " + userSchema + ".queries;";
            haystackDBConn.execNoResultSet(sql);

        } catch (Exception e) {

        }
    }

    private void createUserSchemaTables(String userSchema, String tempQueryTable) throws Exception {

        // 01. Check User Schema Exists
        String sql = "select count(*) as count from pg_catalog.pg_namespace where nspname = '" + userSchema + "'";
        ResultSet rsCount = haystackDBConn.execQuery(sql);
        rsCount.next();
        if (rsCount.getInt("count") == 0) { // Create Schema
            sql = "CREATE SCHEMA " + userSchema + ";";
            haystackDBConn.execNoResultSet(sql);
        }

        // 02. Create AST Table
        sql = "select count(*) as count  from information_schema.tables where upper(table_schema) = upper('" + userSchema + "') and upper(table_name) = upper('ast'))";
        rsCount = haystackDBConn.execQuery(sql);
        rsCount.next();
        if (rsCount.getInt("count") == 0) { // Create AST Table
            sql = "CREATE TABLE " + userSchema + ".ast ( ast_id serial PRIMARY KEY, ast_json text, checksum text)";
            haystackDBConn.execNoResultSet(sql);
        }

        // 03. Create AST Queries Table
        sql = "select count(*) as count  from information_schema.tables where upper(table_schema) = upper('" + userSchema + "') and upper(table_name) = upper('ast_queries'))";
        rsCount = haystackDBConn.execQuery(sql);
        rsCount.next();
        if (rsCount.getInt("count") == 0) { // Create AST Queries Table
            sql = "CREATE TABLE " + userSchema + ".ast_queries ( ast_queries_id serial primary key, queries_id integer NOT NULL,ast_json text,checksum text,ast_id integer NOT NULL);";
            haystackDBConn.execNoResultSet(sql);
        }

        // 04. Create Queries Table
        sql = "select count(*) as count  from information_schema.tables where upper(table_schema) = upper('" + userSchema + "') and upper(table_name) = upper('queries'))";
        rsCount = haystackDBConn.execQuery(sql);
        rsCount.next();
        if (rsCount.getInt("count") == 0) { // Create Queries Table
            sql = "CREATE TABLE " + userSchema + ".queries ( id serial primary key ,logsession text,logcmdcount text, logdatabase text," +
                    "loguser text,logpid text,logsessiontime timestamp with time zone,logtimemin timestamp with time zone, " +
                    " logtimemax timestamp with time zone,logduration interval,sql text,qrytype text);";
            haystackDBConn.execNoResultSet(sql);
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
