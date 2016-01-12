package com.haystack.service;

import com.haystack.domain.Query;
import com.haystack.domain.Table;
import com.haystack.domain.Tables;
import com.haystack.parser.expression.TimestampValue;
import com.haystack.parser.statement.select.IntersectOp;
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

            ClusterService clusterService = new ClusterService(this.configProperties, gpsd_db);
            Tables tablelist = clusterService.getTablefromGPDBStats(gpsd_id);

            saveGpsdStats(gpsd_id, tablelist);

            json = tablelist.getJSON();
        } catch (Exception e) {
            log.error("Error in getting Json (gpsd might not exist) from GPSD:" + gpsd_id + " Exception:" + e.toString());
            HSException hsException = new HSException("CatalogService.getGPSDJson()", "Error in getting Json for GPSD", e.toString(), "gpsd_id=" + gpsd_id, user_id);
        }
        return json;
    }

    private void saveGpsdStats(int gpsdId, Tables tables) {
        for(Table table : tables.tableHashMap.values()) {
            HashMap<String, Object> mapValues = new HashMap<>();

            mapValues.put("gpsd_id", gpsdId);
            mapValues.put("schema_name", table.schema);
            mapValues.put("table_name", table.tableName);
            mapValues.put("size_in_mb", table.stats.sizeOnDisk * 1024);
            mapValues.put("no_of_rows", table.stats.noOfRows);

            try {
                dbConnect.insert(haystackSchema + ".gpsd_stats", mapValues);
            } catch(Exception ex) {
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
            // Get Database Name against GPSDId,-> DBName
            String sql = "select A.workload_id, A.gpsd_id, A.start_date, A.end_date, B.gpsd_db, C.user_id, C.user_name\n" +
                    "from " + haystackSchema + ".workloads A, " + haystackSchema + ".gpsd B , " + haystackSchema + ".users C \n" +
                    "where A.gpsd_id = B.gpsd_id and C.user_id = A.user_id AND A.workload_id = " + workloadId;

            ResultSet rs = dbConnect.execQuery(sql);

            rs.next();

            String gpsd_db = rs.getString("gpsd_db");
            Integer gpsd_id = rs.getInt("gpsd_id");
            user_id = rs.getInt("user_id");
            Date startDate = rs.getDate("start_date");
            Date endDate = rs.getDate("end_date");
            String schemaName = rs.getString("user_name");

            ClusterService clusterService = new ClusterService(this.configProperties, gpsd_db);
            Tables tablelist = clusterService.getTablefromGPDBStats(gpsd_id);

            ModelService ms = new ModelService();

            // Set the Cached Tables into the Model for future annotation
            ms.setTableList(tablelist);

            rs.close();

            // Fetch the queries based on start and end date
            sql = "select sql, EXTRACT(EPOCH FROM logduration) as duration_Seconds, logduration from " + schemaName + "." + queryTblName + " where logsessiontime >= '" + startDate +
                    "' and logsessiontime <='" + endDate + "' and  EXTRACT(EPOCH FROM logduration) > 0;";
            ResultSet rsQry = dbConnect.execQuery(sql);

            while (rsQry.next()) {
                String currQry = rsQry.getString("sql");
                Timestamp duration = rsQry.getTimestamp("logduration");
                Integer durationSeconds = rsQry.getInt("duration_seconds");

                String[] arrQry = currQry.split(";");

                for (int i = 0; i < arrQry.length; i++) {
                    String sQry = arrQry[i];
                    String nQry = extractSelectFromInsert(sQry);
                    ms.processSQL(nQry, durationSeconds, user_id);
                }
            }
            rsQry.close();

            ms.scoreModel();
            String model_json = ms.getModelJSON();


            //sql = "update " + haystackSchema + ".workloads set model_json ='" + model_json + "' where workload_id =" + workloadId + ";";
            //dbConnect.execNoResultSet(sql);

            return model_json;

        } catch (Exception e) {
            log.error("Error in Processing WorkloadId:" + workloadId + " Exception:" + e.toString());
            HSException hsException = new HSException("CatalogService.processWorkload()", "Error in processing workload", e.toString(), "WorkloadId=" + workloadId, user_id);
        }

        return null;
    }

    private String extractSelectFromInsert(String input) {
        String sql = input.toLowerCase().trim();
        boolean isInsert = sql.startsWith("insert");
        if (isInsert) {
            int i = sql.indexOf("into");
            String sql_sub = sql.substring(i);
            int j = sql_sub.indexOf("select");
            if (j > 0) {
                String result = sql_sub.substring(j);
                return result;
            }
        }
        return input;

    }
    // ProcessQueryLog Method, reads the unzipped query log file(s) from the Upload Directory
    // Pass the QueryId, against which the QueryLogDates table will be populated
    // Pass the QueryLogDirectory, where the csv log files have been uncompressed

    public boolean processQueryLog(int queryLogId, String queryLogDirectory) {

        String userName = "";
        Integer userId = null;
        String extTableName = "";
        String original_file_name = "";
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
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
            ClusterService clusterService = new ClusterService(this.configProperties, gpsdDBName);
            Tables tablelist = clusterService.getTablefromGPDBStats(gpsdId);
            String json_res = tablelist.getJSON();
            dbConnGPSD.close();
            //======================================================
            // Update  GPSD  with the Header Information for the new Database
            //dbConnect.execNoResultSet("update  haystack.gpsd set noOflines = " + lineNo + " where dbname ='" + gpsdDBName + "';");
            sqlToExec = String.format("UPDATE %s.gpsd SET nooflines=%d, gpsd_db='%s' WHERE gpsd_id=%d;", searchPath, lineNo, gpsdDBName, gpsdId);
            dbConnect.execNoResultSet(sqlToExec);
            dbConnect.close();
            //======================================================
            // Save GPSD Stats for UI
            saveGpsdStats(gpsdId, tablelist);
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

    public void saveQueries(ArrayList<Query> querylist) {
        // Save all queries in the Haystack Schema
    }
}
