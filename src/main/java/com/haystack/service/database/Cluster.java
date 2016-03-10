package com.haystack.service.database;

import com.google.gson.*;
import com.haystack.domain.Table;
import com.haystack.domain.Tables;
import com.haystack.parser.JSQLParserException;
import com.haystack.parser.parser.CCJSqlParserUtil;
import com.haystack.parser.parser.TokenMgrError;
import com.haystack.parser.statement.Statement;
import com.haystack.parser.statement.select.Select;
import com.haystack.parser.statement.update.Update;
import com.haystack.parser.util.ASTGenerator;
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
import java.util.*;

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

    protected abstract String getQueryType(String input);


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

    public void generateAST(Integer queryId, String query, String userSchema) {
        String jsonAST = "";
        Statement statement = null;
        try {
            try {
                statement = CCJSqlParserUtil.parse(query);
                Select selectStatement = null;

                if (statement == null) {
                    jsonAST = query;
                } else {
                    selectStatement = (Select) statement;
                    Select selectObjForJson = new Select();
                    // AST Generation Processing
                    selectObjForJson = selectStatement;
                    // Create AST for the query and return it
                    ASTGenerator astGen = new ASTGenerator();
                    astGen.removeWhereExpressions(selectObjForJson, "1");
                    jsonAST = getStatementJSON(selectObjForJson);
                }

            } catch (TokenMgrError e) {
                // Statement is not a select/update or supported by Parser, store as is;
                jsonAST = query;
            }

        } catch (JSQLParserException e) {
            jsonAST = query;
        }

        // AST JSON Is generated now PersistAST
        persistAST(userSchema, queryId, jsonAST);
    }

    private void persistAST(String userSchemaName, Integer queryId, String jsonAST) {
        try {

            String sql = "select count(*) as cnt_rows from " + userSchemaName + ".ast_queries where queries_id =" + queryId + ";";
            ResultSet rsCnt = haystackDBConn.execQuery(sql);
            rsCnt.next();

            if (rsCnt.getInt("cnt_rows") == 0) {
                String jsonMD5 = getMD5(jsonAST);

                // Check if another AST exists with the same MD5
                sql = "select min(ast_id) as ast_id, count(ast_id) as count_ast from " + userSchemaName + ".ast where checksum ='" + jsonMD5 + "';";
                ResultSet rsAST = haystackDBConn.execQuery(sql);
                rsAST.next();

                Integer cntMatchedAST = rsAST.getInt("count_ast");
                Integer astID = rsAST.getInt("ast_id");

                if (cntMatchedAST == 0) { // No Matching AST found insert new row in schema.ast table and get the new id to save in schema.ast_queries table
                    sql = "select nextval('" + userSchemaName + ".seq_ast');";
                    ResultSet rsASTId = haystackDBConn.execQuery(sql);
                    rsASTId.next();
                    astID = rsASTId.getInt(1);

                    sql = "insert into " + userSchemaName + ".ast(ast_id, ast_json, checksum) values(" + astID + ",'" + jsonAST + "','" + jsonMD5 + "');";
                    haystackDBConn.execNoResultSet(sql);
                    // Get new generated AST_ID
                    /*sql = "select max(ast_id) as ast_id from " + userSchemaName + ".ast where checksum ='" + jsonMD5 + "'";
                    ResultSet rsASTId = haystackDBConn.execQuery(sql);
                    rsASTId.next();
                    astID = rsASTId.getInt("ast_id");
                    */
                }
                sql = "select nextval('" + userSchemaName + ".seq_ast_queries')";
                rsAST = haystackDBConn.execQuery(sql);
                rsAST.next();
                Integer ast_query_id = rsAST.getInt(1);
                sql = "insert into " + userSchemaName + ".ast_queries(ast_queries_id, queries_id, ast_json,checksum,ast_id) values(" + ast_query_id + "," +
                        queryId + ",'" + jsonAST + "','" + jsonMD5 + "'," + astID + ");";
                haystackDBConn.execNoResultSet(sql);
                // Update the AST in the queryies_ast table and the Unique AST Table
            }
        } catch (Exception e) {
            log.error("Error in persisting AST for queryID:" + queryId);
        }
    }

    public String getMD5(String md5) {
        try {
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("MD5");
            byte[] array = md.digest(md5.getBytes());
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < array.length; ++i) {
                sb.append(Integer.toHexString((array[i] & 0xFF) | 0x100).substring(1, 3));
            }
            return sb.toString();
        } catch (java.security.NoSuchAlgorithmException e) {
            log.error("Error in generating MD5:" + e.toString());
        }
        return null;
    }

    private String getStatementJSON(Select selectStatement) {
        String json = "";
        try {
            GsonBuilder gsonBuilder = new GsonBuilder();

            gsonBuilder.registerTypeAdapter(Double.class, new JsonSerializer<Double>() {
                @Override
                public JsonElement serialize(final Double src, final Type typeOfSrc, final JsonSerializationContext context) {
                    BigDecimal value = BigDecimal.valueOf(src);

                    return new JsonPrimitive(value);
                }
            });

            //Gson objGson = gsonBuilder.setPrettyPrinting().create();
            Gson objGson = gsonBuilder.create();
            json = objGson.toJson(selectStatement);

        } catch (Exception e) {
            log.error("Error generating json " + e.toString());
        }

        return json;
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
            sql = "update " + haystackSchema + ".gpsd set last_schema_refreshed_on = now () where gpsd_id = " + gpsdId + ";";
            haystackDBConn.execNoResultSet(sql);
        } catch (Exception e) {
            log.error("Error saving gpsd_stats rows from " + haystackSchema + ".gpsd_stats for gpsd_id = " + gpsdId + " ;Exception:" + e.toString());
        }
    }

    public void createUserSchemaTables(String userSchema) throws Exception {

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
                sql = "CREATE TABLE " + userSchema + ".ast ( ast_id int primary key , ast_json text, checksum text);";
                haystackDBConn.execNoResultSet(sql);
                sql = "CREATE SEQUENCE " + userSchema + ".seq_ast INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1;";
                haystackDBConn.execNoResultSet(sql);
            }

            // 03. Create AST Queries Table
            sql = "select count(*) as count  from information_schema.tables where upper(table_schema) = upper('" + userSchema + "') and upper(table_name) = upper('ast_queries')";
            rsCount = haystackDBConn.execQuery(sql);
            rsCount.next();
            if (rsCount.getInt("count") == 0) { // Create AST Queries Table
                sql = "CREATE TABLE " + userSchema + ".ast_queries ( ast_queries_id int primary key , queries_id integer not null,ast_json text,checksum text,ast_id integer " +
                        " NOT NULL);";
                haystackDBConn.execNoResultSet(sql);
                sql = "CREATE SEQUENCE " + userSchema + ".seq_ast_queries INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1;";
                haystackDBConn.execNoResultSet(sql);
            }

            // 04. Create Queries Table
            sql = "select count(*) as count  from information_schema.tables where upper(table_schema) = upper('" + userSchema + "') and upper(table_name) = upper('queries')";
            rsCount = haystackDBConn.execQuery(sql);
            rsCount.next();
            if (rsCount.getInt("count") == 0) { // Create Queries Table
                sql = "CREATE TABLE " + userSchema + ".queries ( id int ,logsession text,logcmdcount text, logdatabase text," +
                        "loguser text,logpid text,logsessiontime timestamp with time zone,logtimemin timestamp with time zone, " +
                        " logtimemax timestamp with time zone,logduration interval,sql text,qry_type text, gpsd_id integer );";
                       /* " PARTITION BY RANGE(logsessiontime) ( START (date '1900-01-01') INCLUSIVE END ( date '1900-01-02') EXCLUSIVE\n" +
                        " EVERY (INTERVAL '1 day'));";
                        */
                haystackDBConn.execNoResultSet(sql);
                sql = "CREATE SEQUENCE " + userSchema + ".seq_queries INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1;";
                haystackDBConn.execNoResultSet(sql);
            }

            // 05. Create Query_Metadata Table
            sql = "CREATE TABLE " + userSchema + ".query_metadata( type text, value text );";
            haystackDBConn.execNoResultSet("DROP TABLE IF EXISTS " + userSchema + ".query_metadata;");
            haystackDBConn.execNoResultSet(sql);

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
