package com.haystack.service.database;

import com.google.gson.*;
import com.haystack.domain.Table;
import com.haystack.domain.Tables;
import com.haystack.parser.JSQLParserException;
import com.haystack.parser.parser.CCJSqlParserUtil;
import com.haystack.parser.parser.TokenMgrError;
import com.haystack.parser.statement.Statement;
import com.haystack.parser.statement.insert.Insert;
import com.haystack.parser.statement.select.Select;
import com.haystack.parser.statement.update.Update;
import com.haystack.parser.util.ASTGenerator;
import com.haystack.parser.util.IntTypeAdapter;
import com.haystack.util.ConfigProperties;
import com.haystack.util.Credentials;
import com.haystack.util.DBConnectService;
import com.sun.org.apache.xpath.internal.operations.Bool;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.plaf.nimbus.State;
import javax.swing.text.StyledEditorKit;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
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
    protected Credentials gpsd_Credentials;

    protected final Logger log = LoggerFactory.getLogger(Cluster.class.getName());

    public boolean connect(Credentials credentials) {
        try {
            dbConn = new DBConnectService(dbtype);
            dbConn.connect(credentials);
            gpsd_Credentials = credentials;
        } catch (Exception e) {
            log.error("Error connecting to cluster, " + credentials.toString() + " Exception:" + e.toString());
            return false;
        }
        return true;
    }

    public abstract Tables loadTables(Credentials credentials, Boolean isGPSD, Integer gpsd_id);

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

    /*
        private String trimWhiteSpace(String input){
            String output = input.trim().toLowerCase();
            output = output.replace("\n")
        }
        */
    public void generateAST2(Integer queryId, String query, String userSchema) {

        String ret = null;
        String left = "";
        Boolean isWhere = false;

        try {
            query = query.trim().toLowerCase().replace("\\\\", "");
            StringTokenizer st = new StringTokenizer(query, " \n\t()", true);

            while (st.hasMoreTokens()) {

                String token = st.nextToken();

                while (isDelim(token) || isWhitespace(token)) {
                    if (isDelim(token))
                        left += token;
                    token = st.nextToken();
                }
                left += " " + token;
                if (token.contains("where")) {
                    isWhere = true;
                    while (isWhere) {

                        String currToken = st.nextToken();
                        while (isDelim(currToken) || isWhitespace(currToken)) {
                            if (isDelim(currToken))
                                left += currToken;
                            currToken = st.nextToken();
                        }
                        left += " " + currToken;
                        String leftExp = currToken;
                        String condition = "";

                        currToken = st.nextToken();
                        while (isDelim(currToken) || isWhitespace(currToken)) {
                            if (isDelim(currToken))
                                left += currToken;
                            currToken = st.nextToken();
                        }
                        left += " " + currToken;
                        if (isCondition(currToken)) {
                            Boolean found = false;
                            if (currToken.equals("!~")) {  // For debugging Only
                                currToken = currToken;
                            }
                            if (currToken.equals("is")) {
                                currToken = st.nextToken();
                                while (isDelim(currToken) || isWhitespace(currToken)) {
                                    if (isDelim(currToken))
                                        left += currToken;
                                    currToken = st.nextToken();
                                }
                                if (currToken.equals("not")) {
                                    found = false;
                                } else {
                                    found = true;
                                }
                            }
                            if (!found) {
                                currToken = st.nextToken();
                                while (isDelim(currToken) || isWhitespace(currToken)) {
                                    if (isDelim(currToken))
                                        left += currToken;
                                    currToken = st.nextToken();
                                }
                            }
                            String rightExt = currToken;
                            //  Check if right expression is not a join condition and is a literal
                            if (isJoinCondition(rightExt)) {
                                left += " " + rightExt;
                            } else {
                                left += " ™";
                            }
                            // Check if next token is where condition joiner
                            if (st.hasMoreTokens()) {
                                currToken = st.nextToken();
                            } else {
                                break;
                            }
                            while (isDelim(currToken) || isWhitespace(currToken)) {
                                if (isDelim(currToken))
                                    left += currToken;
                                if (st.hasMoreTokens()) {
                                    currToken = st.nextToken();
                                } else {
                                    break;
                                }
                            }
                            left += " " + currToken;
                            if (isThereAnotherWhereExpr(currToken)) {
                                isWhere = true;
                            } else {
                                isWhere = false;
                            }

                        } else {
                            switch (currToken) {
                                case "in":
                                    isWhere = false;
                                    break;
                                case "group":
                                    isWhere = false;
                                    break;
                                case "order":
                                    isWhere = false;
                                    break;
                                case "limit":
                                    isWhere = false;
                                    break;
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("ASTGenerator2: Ran out of tokens=" + e.toString());
        }
        // AST JSON Is generated now PersistAST
        persistAST(userSchema, queryId, left, false);
    }

    private Boolean isThereAnotherWhereExpr(String token) {
        switch (token) {
            case "and":
                return true;
            case "or":
                return true;
        }
        return false;
    }

    private Boolean isJoinCondition(String rightExp) {
        rightExp = rightExp.trim();

        if (isStringLiteral(rightExp)) {
            return false;
        }
        if (isNumber(rightExp)) {
            return false;
        }
        if (isParameter(rightExp)) {
            return false;
        }
        return true;
    }

    private String collapseDelimOrWhiteSpace(String token) {
        if (isDelim(token))
            return token;
        if (isWhitespace(token))
            return "";
        else
            return "™";
    }

    private Boolean isParameter(String rightExp) {
        if (rightExp.charAt(0) == '$') {
            return true;
        }
        return false;
    }

    private Boolean isStringLiteral(String token) {
        int start = token.indexOf("'");
        if (start > -1) {
            int end = token.indexOf("'", start + 1);
            if (end > -1) {
                return true;
            }
        }
        return false;
    }

    private Boolean isNumber(String token) {
        Boolean result = true;
        for (int i = 0; i < token.length(); i++) {
            char currChar = token.charAt(i);
            switch (currChar) {
                case '0':
                    break;
                case '1':
                    break;
                case '2':
                    break;
                case '3':
                    break;
                case '4':
                    break;
                case '5':
                    break;
                case '6':
                    break;
                case '7':
                    break;
                case '8':
                    break;
                case '9':
                    break;
                case '.':
                    break;
                case '-':
                    break;
                case 'E':
                    break;
                default:
                    return false;
            }
        }
        return result;
    }

    private Boolean isCondition(String token) {
        switch (token) {
            case "=":
                return true;
            case "<":
                return true;
            case ">":
                return true;
            case "!=":
                return true;
            case "!~":
                return true;
            case "is":
                return true;
            case "like":
                return true;

        }
        return false;
    }

    private Boolean isDelim(String token) {
        Boolean res = false;
        switch (token) {
            case ")":
                return true;
            case "(":
                return true;
            case ",":
                return true;
            case "|":
                return true;

        }
        return false;
    }

    private Boolean isWhitespace(String token) {
        Boolean res = false;
        switch (token) {
            case " ":
                return true;
            case "\n":
                return true;
            case "\t":
                return true;
        }
        return false;
    }

    public void generateAST(Integer queryId, String query, String userSchema) {
        String jsonAST = "";
        Statement statement = null;
        Boolean is_json = true;
        Boolean isProcessed = false;
        try {
            try {

                query = query.trim();
                // Remove \\ double backslash from the input to avoid CCJSqlParser lexical error
                if (query.contains("\\")) {
                    // Check if query has E\\
                    String sTrimmedQry = query.replaceAll("\\\\", "");
                    query = sTrimmedQry;
                }
                try {
                    statement = CCJSqlParserUtil.parse(query);
                } catch (TokenMgrError te) {
                    te = te;
                }
                if (statement == null) {
                    jsonAST = query;
                } else {
                    try {
                        Select selectStatement = null;
                        selectStatement = (Select) statement;
                        ASTGenerator astGen = new ASTGenerator();
                        astGen.removeWhereExpressions(selectStatement, "1");
                        jsonAST = getStatementJSON(selectStatement);
                        is_json = true;
                    } catch (ClassCastException ce) {
                        isProcessed = false;
                    }

                    if (isProcessed == false) {
                        Insert insertStatement = null;
                        insertStatement = (Insert) statement;
                        ASTGenerator astGen = new ASTGenerator();
                        astGen.removeWhereExpressions(insertStatement, "1");
                        jsonAST = getStatementJSON(insertStatement);
                        is_json = true;
                    }
                }

            } catch (Exception e) {
                // Statement is not a select/update or supported by Parser, store as is;
                jsonAST = query;
                is_json = false;
            }

        } catch (Exception e) {
            jsonAST = query;
        }

        // AST JSON Is generated now PersistAST
        persistAST(userSchema, queryId, jsonAST, is_json);
    }


    private void persistAST(String userSchemaName, Integer queryId, String jsonAST, Boolean is_json) {
        String sql = "";
        try {

            sql = "select count(*) as cnt_rows from " + userSchemaName + ".ast_queries where queries_id =" + queryId + ";";
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

                    PreparedStatement statement = haystackDBConn.prepareStatement("INSERT INTO " + userSchemaName + ".ast( ast_id, "
                            + " ast_json, checksum, is_json) VALUES(?,?,?,?)");

                    statement.setInt(1, astID);
                    statement.setString(2, jsonAST);
                    statement.setString(3, jsonMD5);
                    statement.setBoolean(4, is_json);
                    statement.executeUpdate();

                }
                sql = "select nextval('" + userSchemaName + ".seq_ast_queries')";
                rsAST = haystackDBConn.execQuery(sql);
                rsAST.next();
                Integer ast_query_id = rsAST.getInt(1);

                PreparedStatement statement = haystackDBConn.prepareStatement("INSERT INTO " + userSchemaName + ".ast_queries( ast_queries_id, "
                        + " queries_id, ast_json, checksum,ast_id ) VALUES(?,?,?,?,?)");

                statement.setInt(1, ast_query_id);
                statement.setInt(2, queryId);
                statement.setString(3, jsonAST);
                statement.setString(4, jsonMD5);
                statement.setInt(5, astID);
                statement.executeUpdate();

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

    private String getStatementJSON(Statement statement) {
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
            json = objGson.toJson(statement);

        } catch (Exception e) {
            log.error("Error generating json " + e.toString());
        }

        return json;
    }

    public void saveClusterStats(int cluster_Log_Id, Tables tables) {
        try {
            // TODO: This should be kept so that the Schema size can be compared later on..


            for (Table table : tables.tableHashMap.values()) {
                HashMap<String, Object> mapValues = new HashMap<>();

                mapValues.put("cluster_log_id", cluster_Log_Id);
                mapValues.put("schema_name", table.schema);
                mapValues.put("table_name", table.tableName);
                mapValues.put("size_in_mb", table.stats.sizeOnDisk * 1024);
                mapValues.put("no_of_rows", table.stats.noOfRows);
                mapValues.put("created_on", "now()");

                try {
                    haystackDBConn.insert(haystackSchema + ".cluster_stats", mapValues);
                } catch (Exception ex) {
                    log.error("Error inserting cluster stats in cluster_stats table for cluster_log_id = " + cluster_Log_Id + " and for table = " + table.tableName + " ;Exception:" + ex.toString());
                    //HSException hsException = new HSException("CatalogService.getGPSDJson()", "Error in getting Json for GPSD", e.toString(), "gpsd_id=" + gpsd_id, user_id);
                }
            }

        } catch (Exception e) {
            log.error("Error saving cluster_stats rows from " + haystackSchema + ".cluster_stats for cluster_log_id = " + cluster_Log_Id + " ;Exception:" + e.toString());
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
                sql = "CREATE TABLE " + userSchema + ".ast ( ast_id int primary key , ast_json text, checksum text, count int, total_duration bigint, is_json boolean);";
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
                        " logtimemax timestamp with time zone,logduration interval,sql text,qrytype text, cluster_id integer , query_log_id integer );";
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
