package com.haystack.service;

import com.haystack.domain.Query;
import com.haystack.domain.Tables;
import com.haystack.domain.Table;
import com.haystack.util.ConfigProperties;
import com.haystack.util.Credentials;
import com.haystack.util.DBConnectService;
import com.haystack.util.HSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

/**
 * CatalogService will deal with all functions related to Cloud Based Files (GPSD and GPDB-Log Files)
 * it will assume that we don't have a cluster to connect to in order get details of the queries
 * storing and retrieving Model in haystack internal
 * Postgres database
 */
public class CatalogService {
    private ConfigProperties configProperties;
    private Credentials credentials;
    private String sqlPath;
    private String haystackSchema;
    private static Logger log = LoggerFactory.getLogger(CatalogService.class.getName());
    private DBConnectService dbConnect;
    private String queryTblName = "queries";
    private DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

    public CatalogService(ConfigProperties configProperties) {
        this.configProperties = configProperties;
        this.credentials = configProperties.getHaystackDBCredentials();
        this.sqlPath = configProperties.properties.getProperty("haystack.sqlDirectory");
        this.haystackSchema = configProperties.properties.getProperty("main.schema");
        dbConnect = new DBConnectService(DBConnectService.DBTYPE.POSTGRES, this.sqlPath);
        try {
            dbConnect.connect(this.credentials);
        } catch (Exception e) {
            log.error("Exception while connecting to Catalog Database" + e.toString());
        }
        validateSchema();
    }


    public String getGPSDJson(int gpsd_id) {

        Integer user_id = null;
        String json = "";
        try {
            String sql = "select A.gpsd_db, C.user_id, C.user_name\n" +
                    "from " + haystackSchema + ".gpsd A , " + haystackSchema + ".users C \n" +
                    "where C.user_id = A.user_id AND A.gpsd_id = " + gpsd_id;

            ResultSet rs = dbConnect.execQuery(sql);

            rs.next();

            String gpsd_db = rs.getString("gpsd_db");
            user_id = rs.getInt("user_id");
            //Date startDate = rs.getDate("start_date");
            //Date endDate = rs.getDate("end_date");
            String schemaName = rs.getString("user_name");

            rs.close();

            ClusterService clusterService = new ClusterService(this.configProperties);
            Tables tablelist = clusterService.getTables(gpsd_id);

            saveGpsdStats(gpsd_id, tablelist);
            json = tablelist.getJSON();


        } catch (Exception e) {
            log.error("Error in getting Json (gpsd might not exist) from GPSD:" + gpsd_id + " Exception:" + e.toString());
            HSException hsException = new HSException("CatalogService.getGPSDJson()", "Error in getting Json for GPSD", e.toString(), "gpsd_id=" + gpsd_id, user_id);
        }
        return json;
    }

    private void saveGpsdStats(int gpsdId, Tables tables) {
        for (Table table : tables.tableHashMap.values()) {
            HashMap<String, Object> mapValues = new HashMap<>();

            mapValues.put("gpsd_id", gpsdId);
            mapValues.put("schema_name", table.schema);
            mapValues.put("table_name", table.tableName);
            mapValues.put("size_in_mb", table.stats.sizeOnDisk * 1024);
            mapValues.put("no_of_rows", table.stats.noOfRows);

            try {
                dbConnect.insert(haystackSchema + ".gpsd_stats", mapValues);
            } catch (Exception ex) {
                log.error("Error inserting gpsd stats in gpsd_stats table for gpsd_id = " + gpsdId + " and for table = " + table.tableName + " ;Exception:" + ex.toString());
                //HSException hsException = new HSException("CatalogService.getGPSDJson()", "Error in getting Json for GPSD", e.toString(), "gpsd_id=" + gpsd_id, user_id);
            }
        }
    }
    // ProcessWorkload Method, will take 1 GPSD DB and a date range and run Analysis on these queries
    // to generate a JSON model against the workload

    public String processWorkload(Integer workloadId) {
        Integer user_id = null;
        try {

            // Fetch Cluster Credentials from GPSD against the WorkloadID
            // Load this in the TableList


            // For Queries select the AdminUser and fetch all the queries against the user
            // Based on StartDate and EndDate of the Cluster

            // Fucking process the workload now !!


            // Get Database Name against GPSDId,-> DBName
            String sql = "select A.workload_id, A.gpsd_id, A.start_date, A.end_date, B.gpsd_db, B.dbname, C.user_id, C.user_name\n" +
                    "from " + haystackSchema + ".workloads A, " + haystackSchema + ".gpsd B , " + haystackSchema + ".users C \n" +
                    "where A.gpsd_id = B.gpsd_id and C.user_id = A.user_id AND A.workload_id = " + workloadId;

            ResultSet rs = dbConnect.execQuery(sql);

            rs.next();

            String gpsd_db = rs.getString("gpsd_db");
            String dbname = rs.getString("dbname");

            Integer gpsd_id = rs.getInt("gpsd_id");
            user_id = rs.getInt("user_id");
            Date startDate = rs.getDate("start_date");
            Date endDate = rs.getDate("end_date");
            String schemaName = rs.getString("user_name");

            ClusterService clusterService = new ClusterService(this.configProperties);
            Tables tablelist = clusterService.getTables(gpsd_id);

            ModelService ms = new ModelService();

            // Set the Cached Tables into the Model for future annotation
            ms.setTableList(tablelist);

            rs.close();

            // Create a UserInbox Message to Start Processing
            Date date = new Date();
            saveUserInboxMsg(user_id, "WORKLOAD_STATUS", "CREATED", "Workload Job Created @DateTime=" + dateFormat.format(date) + " For Workload: Id:" + workloadId + " dbname:" + dbname + " Start:" + startDate + " End:" + endDate
                    , "WORKLOAD JOB CREATED", "CatalogService.processWorkload");


            // Fetch the queries based on start and end date
            // Update: 11Jan2016= Order by date ASC -- So that we can set search_path while processing queries in the order they were executed
            // Update: 11Jan2016= Fetch only queries related to the gpsd_db linked to this workload
            Double minDurationSeconds;
            try {
                // Update: 01Feb2016= Read config.properties query.mindurationseconds and add criteria in fetching queries, this is called pruning
                // in order to minimize computation and relevant.
                minDurationSeconds = Double.parseDouble(this.configProperties.properties.getProperty("query.mindurationseconds"));
            } catch (Exception e) {
                minDurationSeconds = 0.0;
            }

            String whereSQL = schemaName + "." + queryTblName + " where logsessiontime >= '" + startDate +
                    "' and logsessiontime <='" + endDate + "'" +
                    " and  ( EXTRACT(EPOCH FROM logduration) > " + minDurationSeconds + " OR ( EXTRACT(EPOCH FROM logduration) <= " + minDurationSeconds + " AND lower(sql) like '%set%search_path%')) " +  // Commented this for queries which take milliseconds but have significance in parsing i.e. search_path
                    //" and sql like '%catalog_sales%' " + // To Test SQL with where clause
                    " and sql not like '%pg_catalog%'" +
                    " and logdatabase = '" + dbname + "'";        // Get queries related to the GPSD database to minimze repeat processing of queries
            //" and lower(sql) like 'set%search_path%'" +  // Added this to test search_path functionality
            String orderbySQL = " order by logsessiontime;";

            // Count # of Rows so that Workload Processing Percentage can be updated

            sql = "select count(1) as NoOfQueries FROM " + whereSQL;
            ResultSet rsCnt = dbConnect.execQuery(sql);

            rsCnt.next();
            int totalQueries = rsCnt.getInt("NoOfQueries");
            int percentProcessed = 0;
            int currQryCounter = 0;

            sql = "select id as queryId, sql, EXTRACT(EPOCH FROM logduration) as duration_Seconds, logduration from " + whereSQL + orderbySQL;
            ResultSet rsQry = dbConnect.execQuery(sql);

            // Create a UserInbox Message for Updated Processing
            Date date2 = new Date();
            saveUserInboxMsg(user_id, "WORKLOAD_STATUS", "PROCESSING", "Workload Processing Started @DateTime=" + dateFormat.format(date2) + " For Workload: Id:" + workloadId + " dbname:" + dbname + " Start:" + startDate + " End:" + endDate
                    , "WORKLOAD PROCESSING STARTED", "CatalogService.processWorkload");

            String current_search_path = "public";
            String nQry = "";
            while (rsQry.next()) {
                try {
                    currQryCounter++;
                    int currProcessingPercent = (currQryCounter * 100 / totalQueries);
                    if (currProcessingPercent > percentProcessed) {
                        updatePercentProcessedWorkload(currProcessingPercent, workloadId);
                        percentProcessed = currProcessingPercent;
                    }
                    String currQry = rsQry.getString("sql");
                    Integer queryId = rsQry.getInt("queryId");
                    currQry = currQry.toLowerCase(); // convert sql to lower case for ease of processing


                    Timestamp duration = rsQry.getTimestamp("logduration");
                    Integer durationSeconds = rsQry.getInt("duration_seconds");

                    String[] arrQry = currQry.split(";");

                    for (int i = 0; i < arrQry.length; i++) {
                        String sQry = arrQry[i];

                        // Check if the SQL statement is set search_path statement, if yes then store this in the search_path variable
                        if (sQry.trim().toLowerCase().startsWith("set search_path") == true) {
                            String[] s1 = sQry.split("=");
                            current_search_path = s1[1].toString().replaceAll("\\s+", "");
                        }
                        // Remove \\ double backslash from the input to avoid CCJSqlParser lexical error
                        if (sQry.contains("\\")) {
                            String sTrimmedQry = sQry.replaceAll("\\\\", "");
                            sQry = sTrimmedQry;
                        }

                        nQry = extractSelectFromInsert(sQry);  // extract select portion from insert statement
                        if (nQry.equals(sQry)) {
                            nQry = extractSelectFromGPText(sQry);       // extract subquery from gptext.index(TABLE(subquery)
                        }
                        nQry = removeScatterBy(nQry);

                        try {
                            String jsonAST = ms.processSQL(queryId, nQry, durationSeconds, user_id, current_search_path);
                            if (jsonAST.length() > 0) {
                                persistAST(schemaName, queryId, jsonAST);
                            }
                        } catch (Exception e) {
                            log.debug("Skip Statement in Processing WorkloadId:" + workloadId + " SQL:" + nQry.toString());
                        }
                    }
                } catch (Exception e) {
                    log.error("Error in Processing SQL:" + workloadId + ": search_path=" + current_search_path + " SQL=" + nQry + " Exception:" + e.toString());
                }
            }
            rsQry.close();

            ms.scoreModel();
            ms.generateRecommendations();
            String model_json = ms.getModelJSON();

            // Create a UserInbox Message for Completed Processing
            Date date3 = new Date();
            saveUserInboxMsg(user_id, "WORKLOAD_STATUS", "COMPLETED", "Workload Processing Completed @DateTime=" + dateFormat.format(date3) + " For Workload: Id:" + workloadId + " dbname:" + dbname + " Start:" + startDate + " End:" + endDate
                    , "WORKLOAD PROCESSING COMPLETED", "CatalogService.processWorkload");

            //sql = "update " + haystackSchema + ".workloads set model_json ='" + model_json + "' where workload_id =" + workloadId + ";";
            //dbConnect.execNoResultSet(sql);

            return model_json;

        } catch (Exception e) {
            // Create a UserInbox Message for Error in Processing
            Date date = new Date();
            saveUserInboxMsg(user_id, "WORKLOAD_STATUS", "ERROR", "Workload Processing Error @DateTime=" + dateFormat.format(date) + " For Workload: Id:" + workloadId + " Exception :" + e.toString()
                    , "WORKLOAD PROCESSING ERROR", "CatalogService.processWorkload");
            log.error("Error in Processing WorkloadId:" + workloadId + " Exception:" + e.toString());
            HSException hsException = new HSException("CatalogService.processWorkload()", "Error in processing workload", e.toString(), "WorkloadId=" + workloadId, user_id);
        }

        return null;
    }

    private void persistAST(String userSchemaName, Integer queryId, String jsonAST) {
        try {
            String jsonMD5 = getMD5(jsonAST);

            // Check if another AST exists with the same MD5
            String sql = "select min(ast_id) as ast_id, count(ast_id) as count from " + userSchemaName + ".ast where checksum ='" + jsonMD5 + "';";
            ResultSet rsAST = dbConnect.execQuery(sql);
            rsAST.next();

            Integer cntMatchedAST = rsAST.getInt("count");
            Integer astID = rsAST.getInt("ast_id");

            if (cntMatchedAST == 0) { // No Matching AST found insert new row in schema.ast table and get the new id to save in schema.ast_queries table
                sql = "insert into " + userSchemaName + ".ast(ast_json, checksum) values('" + jsonAST + "','" + jsonMD5 + "');";
                dbConnect.execNoResultSet(sql);
                // Get new generated AST_ID
                sql = "select max(ast_id) as ast_id from " + userSchemaName + ".ast where checksum ='" + jsonMD5 + "'";
                ResultSet rsASTId = dbConnect.execQuery(sql);
                rsASTId.next();
                astID = rsASTId.getInt("ast_id");
            }
            sql = "insert into " + userSchemaName + ".ast_queries(queries_id, ast_json,checksum,ast_id) values(" + queryId + ",'" + jsonAST + "','" + jsonMD5 + "'," + astID + ");";
            dbConnect.execNoResultSet(sql);
            // Update the AST in the queryies_ast table and the Unique AST Table
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

    private void updatePercentProcessedWorkload(int currProcessingPercent, Integer workloadId) {
        try {
            String sql = String.format("UPDATE %s.workloads SET percent_processed = %d where workload_id = %d;", haystackSchema.toString(), currProcessingPercent, workloadId);
            dbConnect.execNoResultSet(sql);
        } catch (Exception e) {
            log.error("Error in Updating Workload Percentage WorkloadId:" + workloadId + " Exception:" + e.toString());
        }
    }

    private String removeScatterBy(String input) {
        StringTokenizer st = new StringTokenizer(input, "()", true);
        String ret = null;
        String left = "";
        if (input.contains("scatter by")) {
            while (st.hasMoreTokens()) {

                String token = st.nextToken();
                int start = left.length();
                left += token;
                if (token.contains("scatter by")) {
                    boolean exit = false;
                    while (!exit) {
                        token = st.nextToken();
                        left += token;
                        if (token.equals(")")) {
                            int end = left.length();
                            ret = input.substring(0, start);
                            if (end < input.length()) {
                                ret += input.substring(end, input.length() - end);
                            }
                            return ret;
                        }
                    }
                }
            }
        } else {

            ret = input;
        }
        return ret;
    }

    private String extractSelectFromGPText(String input) {

        String ret = "";
        String currToken = null;
        input = input.toLowerCase();

        if (input.contains("gptext.index")) {
            StringTokenizer st = new StringTokenizer(input, "()", true);
            String leftString = "";
            while (st.hasMoreTokens()) {
                currToken = st.nextToken();
                leftString += currToken;

                if (currToken.equals("table")) {   // Start Extracting SELECT SQL from this point till

                    currToken = st.nextToken();
                    leftString += currToken;
                    int start = leftString.length();
                    boolean exit = false;
                    int countOpenBrackets = 0;
                    while (!exit) {
                        currToken = st.nextToken();
                        leftString += currToken;
                        if (currToken.equals("(")) {
                            countOpenBrackets++;
                        } else {
                            if (currToken.equals(")")) {
                                if (countOpenBrackets == 0) {
                                    ret = input.substring(start, leftString.length() - 1);
                                    return ret;
                                } else {
                                    countOpenBrackets--;
                                }
                            }
                        }
                    }
                }
            }
        } else {
            ret = input;
        }
        return ret;
    }

    private String extractSelectFromInsert(String input) {
        String sql = input.toLowerCase().trim();
        boolean isInsert = sql.startsWith("insert");
        String currToken = "";
        String ret = "";

        if (isInsert) {
            StringTokenizer st = new StringTokenizer(input, "()", true);
            String leftString = "";
            boolean foundInsert = false, selectStarted = false, bracketFound = false;
            int start = 0, end = 0;
            while (st.hasMoreTokens()) {
                currToken = st.nextToken();
                leftString += currToken;

                if (currToken.contains("insert")) {
                    foundInsert = true;
                }
                if (currToken.equals("(") && foundInsert) {
                    bracketFound = true;
                    start = leftString.length() + 1;
                }
                if (currToken.equals(")") && bracketFound) {
                    bracketFound = false;
                }
                if (currToken.contains("select") && foundInsert) {
                    if (!bracketFound) {
                        start = leftString.length() - currToken.length();
                        ret = input.substring(start, input.length() - start);
                    } else {
                        selectStarted = true;
                        start = leftString.length() - currToken.length();
                        boolean exit = false;
                        int countOpenBrackets = 0;
                        while (!exit) {
                            currToken = st.nextToken();
                            leftString += currToken;
                            if (currToken.equals("(")) {
                                countOpenBrackets++;
                            } else {
                                if (currToken.equals(")")) {
                                    if (countOpenBrackets == 0) {
                                        ret = input.substring(start, leftString.length() - 1);
                                        return ret;
                                    } else {
                                        countOpenBrackets--;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } else
            ret = input;
        return ret;
    }
    // ProcessQueryLog Method, reads the unzipped query log file(s) from the Upload Directory
    // Pass the QueryId, against which the QueryLogDates table will be populated
    // Pass the QueryLogDirectory, where the csv log files have been uncompressed

    public boolean processQueryLog(int queryLogId, String queryLogDirectory) {

        String userName = "";
        Integer userId = null;
        String extTableName = "";
        String original_file_name = "";

        // Fetch userName from Users Table against the queryLogID
        String sql = String.format("SELECT QL.original_file_name, US.user_name, US.user_id FROM %s.query_logs QL inner join %s.users US ON QL.user_id = US.user_id where QL.query_log_id = %d",
                haystackSchema, haystackSchema, queryLogId);

        try {
            try {
                ResultSet rs = dbConnect.execQuery(sql);
                rs.next();
                userName = rs.getString("user_name");
                userId = rs.getInt("user_id");
                original_file_name = rs.getString("original_file_name");

                Date date = new Date();

                // Create User Inbox Message
                saveUserInboxMsg(userId, "QUERYLOG_STATUS", "CREATED", "Query Log Job Created @DateTime=" + dateFormat.format(date) + " For Uploaded File:" + original_file_name, "JOB CREATED", "CatalogService.processQueryLog");
            } catch (SQLException e) {
                HSException hsException = new HSException("CatalogService.processQueryLog()", "Unable to fetch userNAme", e.toString(), "QueryLogId=" + queryLogId, userId);
                log.error("Unable to fetch user_id, Exception:" + e.toString());
                throw e;
            }
            try {
                extTableName = createExternalTableForQueries(queryLogDirectory, queryLogId, userName);
            } catch (Exception e) {
                log.error("Unable to create external table for queries, Exception:" + e.toString());
                HSException hsException = new HSException("CatalogService.processQueryLog()", "Unable to create external table for queries.", e.toString(),
                        "QueryLogId=" + queryLogId + ", QueryLogDirectory=" + queryLogDirectory, userId);
                throw e;
            }
            try {
                Date date = new Date();
                saveUserInboxMsg(userId, "QUERYLOG_STATUS", "PROCESSING", "Processing started for Query Log File:" + original_file_name + " @DateTime=" + dateFormat.format(date), "JOB PROCESSING", "CatalogService.processQueryLog");

                loadQueries(extTableName, queryLogId, userName);
            } catch (Exception e) {
                log.error("Unable to populate queries, Exception:" + e.toString());
                HSException hsException = new HSException("CatalogService.processQueryLog()", "Unable to load queries from external table.", e.toString(),
                        "QueryLogId=" + queryLogId + ", ExternalTableName=" + extTableName, userId);
                throw e;
            }
        } catch (Exception e) {
            Date date = new Date();
            saveUserInboxMsg(userId, "QUERYLOG_STATUS", "ERROR", "Error encountered while processing Query Log File, @DateTime=" + dateFormat.format(date) + " For Uploaded File:" + original_file_name, "JOB ERROR", "CatalogService.processQueryLog");
            return false;
        }
        Date date = new Date();
        saveUserInboxMsg(userId, "QUERYLOG_STATUS", "SUCCESS", "Processing of Query Log File:" + original_file_name + " is completed @DateTime=" + dateFormat.format(date), "JOB SUCCESS", "CatalogService.processQueryLog");

        return true;
    }

    public void saveUserInboxMsg(Integer userId, String msgType, String msgTypeStatus, String msg_Text, String msg_Title, String msg_Class) {
        try {
            String sql = String.format("INSERT INTO %s.user_inbox (user_id, msg_type, msg_type_status, msg_date, isRead, msg_text, msg_title, msg_class) VALUES(%d," +
                    "'%s','%s', now(), false,'%s','%s','%s');", haystackSchema.toString(), userId, msgType, msgTypeStatus, msg_Text, msg_Title, msg_Class);

            dbConnect.execNoResultSet(sql);
        } catch (Exception e) {
            log.error("Unable to save UserInbox Message, Exception:" + e.toString());
        }
    }

    private void loadQueries(String extTableName, Integer QueryId, String userId) throws SQLException {

        String strRunId = String.format("%05d", QueryId);
        String queryLogTableName = userId + ".qry" + strRunId;
        String schemaName = userId;
        ResultSet rs = null;

        String sql = "SELECT " + haystackSchema + ".load_querylog('" + haystackSchema + "','" + schemaName + "','" + queryTblName + "','" + extTableName + "'," + QueryId + ");";
        rs = dbConnect.execQuery(sql);


    }

    // Pass parameter queryLogDir example /mujtaba_dot_qadri_at_gmail_dot_com/querylogs/5
    // Make sure gpfdist is running, and gpfdist_host and gpfdist_port are configures in config.properties file

    private String createExternalTableForQueries(String queryLogDirectory, Integer queryId, String userid) throws SQLException, IOException {
        String extTableName = "ext_" + queryId;

        String gpfdist_host = configProperties.properties.getProperty("gpfdist_server");
        String gpfdist_port = configProperties.properties.getProperty("gpfdist_port");


        String sql = "\n" +
                "CREATE EXTERNAL  TABLE " + userid + "." + extTableName + "\n" +
                "(\n" +
                "    logtime text,\n" +
                "    loguser text,\n" +
                "    logdatabase text,\n" +
                "    logpid text,\n" +
                "    logthread text,\n" +
                "    loghost text,\n" +
                "    logport text,\n" +
                "    logsessiontime text,\n" +
                "    logtransaction text,\n" +
                "    logsession text,\n" +
                "    logcmdcount text,\n" +
                "    logsegment text,\n" +
                "    logslice text,\n" +
                "    logdistxact text,\n" +
                "    loglocalxact text,\n" +
                "    logsubxact text,\n" +
                "    logseverity text,\n" +
                "    logstate text,\n" +
                "    logmessage text,\n" +
                "    logdetail text,\n" +
                "    loghint text,\n" +
                "    logquery text,\n" +
                "    logquerypos int,\n" +
                "    logcontext text,\n" +
                "    logdebug text,\n" +
                "    logcursorpos text,\n" +
                "    logfunction text,\n" +
                "    logfile text,\n" +
                "    logline text,\n" +
                "    logstack text\n" +
                ")\n" +
                " LOCATION ( 'gpfdist://" + gpfdist_host + ":" + gpfdist_port + queryLogDirectory + "/*.csv' )\n" +
                "FORMAT 'CSV' (delimiter ',' null '' escape '\\\\' quote '\"' FILL MISSING FIELDS) " +
                " LOG ERRORS INTO " + userid + ".err_queries SEGMENT REJECT LIMIT 20 PERCENT;";

               /* "CREATE EXTERNAL  TABLE " + userid + "." + extTableName + "\n" +
                "(\n" +
                "    logtime timestamp with time zone,\n" +
                "    loguser text,\n" +
                "    logdatabase text,\n" +
                "    logpid text,\n" +
                "    logthread text,\n" +
                "    loghost text,\n" +
                "    logport text,\n" +
                "    logsessiontime timestamp with time zone,\n" +
                "    logtransaction int,\n" +
                "    logsession text,\n" +
                "    logcmdcount text,\n" +
                "    logsegment text,\n" +
                "    logslice text,\n" +
                "    logdistxact text,\n" +
                "    loglocalxact text,\n" +
                "    logsubxact text,\n" +
                "    logseverity text,\n" +
                "    logstate text,\n" +
                "    logmessage text,\n" +
                "    logdetail text,\n" +
                "    loghint text,\n" +
                "    logquery text,\n" +
                "    logquerypos int,\n" +
                "    logcontext text,\n" +
                "    logdebug text,\n" +
                "    logcursorpos int,\n" +
                "    logfunction text,\n" +
                "    logfile text,\n" +
                "    logline int,\n" +
                "    logstack text\n" +
                ")\n" +
                " LOCATION ( 'gpfdist://" + gpfdist_host + ":" + gpfdist_port + queryLogDirectory + "/*.csv' )\n" +
                "FORMAT 'CSV' (delimiter ',' null '' escape '\\\\' quote '\"');";
                */

        createSchema(userid);
        dbConnect.execNoResultSet("DROP EXTERNAL TABLE IF EXISTS " + userid + "." + extTableName + ";");
        dbConnect.execNoResultSet(sql);

        return extTableName;
    }


    public String executeGPSD(int gpsdId, String userName, String fileName) {
        String searchPath = this.haystackSchema;
        int lineNo = 0;
        boolean hadErrors = false;
        String sqlToExec;
        try {
            // NOTE: What's this for ?
            Integer batchSize = Integer.parseInt(configProperties.properties.getProperty("gpsd.batchsize"));
            StringBuilder sbBatchQueries = new StringBuilder();

            ArrayList<String> fileQueries = new ArrayList<String>();

            String gpsdDBName = userName.toLowerCase() + String.format("%04d", gpsdId);
            DBConnectService dbConnGPSD = new DBConnectService(DBConnectService.DBTYPE.GREENPLUM, this.sqlPath);

            // Create a database
            ConfigProperties tmpConfig = new ConfigProperties();
            tmpConfig.loadProperties();
            Credentials tmpCred = tmpConfig.getGPSDCredentials();
            // TODO: Should 'postgres' be hardcoded ?
            tmpCred.setDatabase("postgres");

            DBConnectService tmpdbConn = new DBConnectService(DBConnectService.DBTYPE.GREENPLUM, this.sqlPath);
            tmpdbConn.connect(tmpCred);

            try {
                // Generate a new Database for this User
                tmpdbConn.execNoResultSet("CREATE DATABASE " + gpsdDBName + ";");
                tmpdbConn.close();
            } catch (Exception e) {
                log.error("DATABASE:" + gpsdDBName + " already exists!\n");
                HSException hsException = new HSException("CatalogService.executeGPSD()", "Unable to create database to execute GPSD script.", e.toString(),
                        "GPSDId=" + gpsdId + ", FileName=" + fileName, userName);

            }

            // Get GPSD Credentials
            ConfigProperties gpsdConfig = new ConfigProperties();
            gpsdConfig.loadProperties();

            Credentials gpsdCred = gpsdConfig.getGPSDCredentials();
            gpsdCred.setDatabase(gpsdDBName);

            // Create a new connection object
            dbConnGPSD.connect(gpsdCred);

            String currQuery = "";
            String nextQuery = "";

            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            String line = null;

            String ls = System.getProperty("line.separator");

            Integer currBatchSize = 1;

            // Header Variables
            String gpsd_DB = "", gpsd_date = "", gpsd_params = "", gpsd_version = "";


            while ((line = reader.readLine()) != null) {
                lineNo++;
                if (line.trim().length() == 0) {
                    continue;
                }
                // If line has comments ignore the comments line, extract any characters before the comments line
                int x = line.indexOf("--");
                if (lineNo == 20) {
                    if (gpsd_date.length() == 0 && gpsd_version.length() == 0) {
                        // NOTE: Shouldn't this error propogate up to the UI so that the user knows that the GPSD submitted was in an incorrect format
                        Exception e = new Exception("GPSD File not in proper format");
                        throw e;
                    }
                }
                if (x >= 0) {
                    // Extract Header Info
                    if (lineNo < 11) {
                        int i = line.indexOf("-- Database:");
                        if (i >= 0) {
                            gpsd_DB = line.substring(13);
                        }
                        i = line.indexOf("-- Date:");
                        if (i >= 0) {
                            gpsd_date = line.substring(8);
                        }
                        i = line.indexOf("-- CmdLine:");
                        if (i >= 0) {
                            gpsd_params = line.substring(11);
                        }
                        i = line.indexOf("-- Version:");
                        if (i >= 0) {
                            gpsd_version = line.substring(11);
                            // Update  GPSD  with the Header Information for the new Database
                            /*dbConnect.execNoResultSet(String.format("update haystack.gpsd set gpsd_db = '" + gpsd_DB + "', gpsd_date = '" + gpsd_date + "' , gpsd_params='"
                                    + gpsd_params + "', gpsd_version = '" + gpsd_version + "', filename ='" + fileName + "' where dbname ='" + gpsdDBName + "';");*/
                            sqlToExec = String.format("UPDATE %s.gpsd SET dbname='%s', gpsd_date='%s', gpsd_params='%s', gpsd_version='%s' WHERE gpsd_id = %d;",
                                    searchPath, gpsd_DB, gpsd_date, gpsd_params, gpsd_version, gpsdId);
                            dbConnect.execNoResultSet(sqlToExec);
                        }
                    }

                    if (x > 0) {
                        String[] strSplit = line.split("--");
                        currQuery = currQuery + " " + strSplit[0];
                    }
                } else {

                    // Check if its CREATE FUNCTION STATEMENT (Readline till function is ignore) (to avoid semi-colon problems)

                    int isFunction = line.toUpperCase().indexOf("CREATE FUNCTION");

                    Boolean foundFirstDollar = false;
                    Boolean foundSecondDollar = false;
                    Boolean continueIgnore = false;

                    if (isFunction >= 0) {
                        continueIgnore = true;
                    }

                    while (continueIgnore) {
                        line = reader.readLine();
                        lineNo++;
                        if (line.trim().length() == 0) {
                            continue;
                        }

                        if (line.indexOf("$$") >= 0) {
                            if (foundFirstDollar == true) {
                                foundSecondDollar = true;
                            } else {
                                foundFirstDollar = true;
                            }
                        }
                        if (foundFirstDollar && foundSecondDollar && line.indexOf(";") >= 0) {
                            continueIgnore = false;
                            foundFirstDollar = false;
                            foundSecondDollar = false;
                        }
                        if (line.toUpperCase().indexOf("LANGUAGE") >= 0) {
                            if (line.indexOf(";") >= 0) {
                                foundFirstDollar = false;
                                foundSecondDollar = false;
                                continueIgnore = false; // If LANGUAGE is found, function end has reached
                            }
                        }
                    }

                    if (isFunction >= 0) {
                        continue;
                    }

                    // ====================== End Avoid Create Function ====================
                    int isChangingDB = line.indexOf("\\connect");


                    if (isChangingDB < 0) {



                        int y = line.indexOf(";");
                        if (y >= 0) {

                            // Check if semi-colon in between double quotes, if yes then ignore this semi-colon
                            //String line = "foo,bar,c;qual=\"baz,blurb\",d;junk=\"quux,syzygy\"";
                            String[] strSplit = line.split(";(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                            //for(String t : tokens) {
                            //    System.out.println("> "+t);
                            //}
                            //String[] strSplit = line.split(";");
                            if (strSplit.length > 0) {
                                currQuery = currQuery + " " + strSplit[0] + ";";
                            } else {
                                currQuery = currQuery + " " + ";";
                            }

                            // Check if query is insert then Batch it otherwise execute it
                            int isInsert = currQuery.toLowerCase().indexOf("insert");

                            if (isInsert >= 0) {
                                sbBatchQueries.append(currQuery);
                                if ((currBatchSize >= batchSize)) {
                                    try {
                                        dbConnGPSD.execNoResultSet(sbBatchQueries.toString());
                                        log.trace(sbBatchQueries.toString());
                                    } catch (Exception e) {
                                        hadErrors = true;
                                        log.debug(e.getMessage());
                                        HSException hsException = new HSException("CatalogService.executeGPSD()", "Error in executing GPSD Query Batch", e.toString(),
                                                "GPSDId=" + gpsdId + ", FileName=" + fileName + " ,SQL=" + sbBatchQueries.toString(), userName);
                                        // do nothing
                                    }
                                    currBatchSize = 0;
                                    sbBatchQueries.setLength(0);
                                }
                                currBatchSize++;
                            } else { // Not an insert query execute it;
                                try {
                                    dbConnGPSD.execNoResultSet(currQuery);
                                    log.trace(currQuery);
                                } catch (Exception e) {
                                    hadErrors = true;
                                    log.debug(e.getMessage());
                                    HSException hsException = new HSException("CatalogService.executeGPSD()", "Error in executing GPSD Query", e.toString(),
                                            "GPSDId=" + gpsdId + ", FileName=" + fileName + " ,SQL=" + currQuery, userName);
                                    // do nothing
                                }
                            }
                            // Query Extracted Now Check BatchSize and execute the query

                            currQuery = "";
                            if (strSplit.length > 1) {
                                currQuery = strSplit[1];
                            }
                        } else {  // No comment or end of line character append to CurrQuery
                            currQuery = currQuery + " " + line;
                        }
                    }
                }
            }
            ClusterService clusterService = new ClusterService(this.configProperties);
            Tables tablelist = clusterService.getTables(gpsdId);
            String json_res = tablelist.getJSON();
            dbConnGPSD.close();
            //======================================================
            // Update  GPSD  with the Header Information for the new Database
            //dbConnect.execNoResultSet("update  haystack.gpsd set noOflines = " + lineNo + " where dbname ='" + gpsdDBName + "';");
            sqlToExec = String.format("UPDATE %s.gpsd SET nooflines=%d, gpsd_db='%s' WHERE gpsd_id=%d;", haystackSchema, lineNo, gpsdDBName, gpsdId);
            dbConnect.execNoResultSet(sqlToExec);

            sqlToExec = "select user_id from " + haystackSchema + ".users where user_name = '" + userName + "';";
            ResultSet rsUser = dbConnect.execQuery(sqlToExec);
            rsUser.next();

            //TODO  Check if cluster entry exists for the same dbname,
            sqlToExec = "INSERT INTO " + haystackSchema + ".cluster(  cluster_id ,cluster_name ,host ,dbname ,port ,password ,username ,\n" +
                    "  query_refresh_schedule ,created_on ,cluster_type ,user_id ) VALUES (999" + gpsdId + ",'GPSD-" + gpsdId + "-" + gpsdDBName +
                    "','" + gpsdCred.getHostName() + "','" + gpsdDBName + "'," + gpsdCred.getPort() + ",'" + gpsdCred.getPassword() +
                    "','" + gpsdCred.getUserName() + "','24hour',now(),'GREENPLUM'," + rsUser.getInt("user_id");

            dbConnect.execNoResultSet(sqlToExec);
            dbConnect.close();
            return json_res;

        } catch (Exception e) {
            hadErrors = true;
            log.error(e.toString());
            HSException hsException = new HSException("CatalogService.executeGPSD()", "Error in parsing GPSD File or updating GPSD table", e.toString(),
                    "GPSDId=" + gpsdId + ", FileName=" + fileName, userName);
        }

        return "None";
    }

    private void createUser(String userId, String password, String org) {
        try {

            String sql = "insert into " + haystackSchema + ".users(userid, password, organization, createddate) values('" + userId +
                    "','" + password + "','" + org + "', now());";
            dbConnect.execNoResultSet(sql);
        } catch (Exception e) {
            log.error(e.toString());
        }
    }

    private Integer getRunId(String userId) {
        Integer l_RunId = 0;
        try {
            validateSchema();
            // Insert a row in runlog, so that next time, only new queries get inserted in log
            String sql = "SELECT coalesce(max(run_id) + 1, 1) AS max_run_id\n" +
                    "FROM " + haystackSchema + ".run_log;\n";
            //ResultSet run_id = dbConnect.execQuery(dbConnect.getSQLfromFile("getMaxRunId"));
            ResultSet run_id = dbConnect.execQuery(sql);
            run_id.next();

            l_RunId = run_id.getInt("max_run_id");

            log.info("RUNLOG=" + l_RunId);
            //String sql = "insert into haystack.run_log(run_id, , run_db, run_date, run_user) values(" + l_RunId +
            //        ",'" + credentials.getDatabase() + "', now(),'" + userId + "' );";
            //dbConnect.execNoResultSet(sql);
        } catch (Exception e) {
            log.error(e.toString());
        }
        return l_RunId;
    }

    private void createSchema(String schemaName) throws SQLException, IOException {

        try {
            // Create Schema
            String qry = "select exists (select * from pg_catalog.pg_namespace where nspname = '" + schemaName + "')as result;";
            ResultSet rs = dbConnect.execQuery(qry);
            rs.next();
            if (rs.getBoolean("result") == false) {

                String sqlQry = "create schema " + schemaName + ";";
                int result = dbConnect.execNoResultSet(sqlQry);
                // Create Model Tables
                if (schemaName == haystackSchema) {
                    dbConnect.execScript("createTables");
                }
            }
        } catch (Exception e) {
            log.error(e.toString());
        }

    }

    private void validateSchema() {
        try {
            log.info("Validate schema");
            boolean exists = false;
            String sqlQry = "select exists (select * from pg_catalog.pg_namespace where nspname = '" + haystackSchema + "');";
            ResultSet rs = dbConnect.execQuery(sqlQry);

            while (rs.next()) {
                String res = rs.getString(1);
                if (res.contains("f")) {
                    log.info("Haystack schema doesnt exist in postgres, creating it now");
                    createSchema(haystackSchema);
                    exists = false;

                } else {
                    exists = true;
                }
            }
        } catch (Exception e) {
            log.error(e.toString());
        }
    }


}
