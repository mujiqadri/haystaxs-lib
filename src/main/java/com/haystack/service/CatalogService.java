package com.haystack.service;

import com.haystack.domain.Query;
import com.haystack.parser.statement.select.IntersectOp;
import com.haystack.util.ConfigProperties;
import com.haystack.util.Credentials;
import com.haystack.util.DBConnectService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * CatalogService will deal with all functions related to storing and retrieving Model in haystack internal
 * Postgres database
 */
public class CatalogService {
    private ConfigProperties configProperties;
    private Credentials credentials;
    private String sqlPath;
    private String haystackSchema;
    private static Logger log = LoggerFactory.getLogger(CatalogService.class.getName());
    private DBConnectService dbConnect;

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

    // ProcessWorkload Method, will take 1 GPSD DB and a date range and run Analysis on these queries
    // to generate a JSON model against the workload

    public String processWorkload(Integer workloadId, Integer gpsdId, Date fromdate, Date todate) {

        try {
            // Get Database Name against GPSDId,-> DBName


        } catch (Exception e) {

        }
        return null;
    }

    // ProcessQueryLog Method, reads the unzipped query log file(s) from the Upload Directory
    // Pass the QueryId, against which the QueryLogDates table will be populated
    // Pass the QueryLogDirectory, where the csv log files have been uncompressed

    public boolean processQueryLog(int queryLogId, String queryLogDirectory) {

        String userName = "";
        String extTableName = "";
        // Fetch userName from Users Table against the queryLogID
        String sql = String.format("SELECT US.user_name FROM %s.query_log QL inner join %s.users US ON QL.user_id = US.user_id where QL.query_log_id = %d", haystackSchema, haystackSchema, queryLogId);

        try {
            ResultSet rs = dbConnect.execQuery(sql);
            rs.next();
            userName = rs.getString("user_name");
        } catch (SQLException e) {
            log.error("Unable to fetch user_id, Exception:" + e.toString());
        }
        try {
            extTableName = createExternalTableForQueries(queryLogDirectory, queryLogId, userName);
        } catch (Exception e) {
            log.error("Unable to create external table for queries, Exception:" + e.toString());
        }
        try {
            loadQueries(extTableName, queryLogId, userName);
            return true;
        } catch (Exception e) {
            log.error("Unable to populate queries, Exception:" + e.toString());
            return false;
        }

    }

    public boolean executeGPSD(int gpsdId, String userName, String fileName) {
        // TODO: This should come from the config file
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
                log.debug("DATABASE:" + gpsdDBName + " already exists!\n");
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
                            sqlToExec = String.format("UPDATE %s.gpsd SET gpsd_db='%s', gpsd_date='%s', gpsd_params='%s', gpsd_version='%s' WHERE gpsd_id = %d;",
                                    searchPath, gpsdDBName, gpsd_date, gpsd_params, gpsd_version, gpsdId);
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
                            String[] strSplit = line.split(";");
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
                                        log.debug(sbBatchQueries.toString());
                                    } catch (Exception e) {
                                        hadErrors = true;
                                        log.debug(e.getMessage());
                                        // do nothing
                                    }
                                    currBatchSize = 0;
                                    sbBatchQueries.setLength(0);
                                }
                                currBatchSize++;
                            } else { // Not an insert query execute it;
                                try {
                                    dbConnGPSD.execNoResultSet(currQuery);
                                    log.debug(currQuery);
                                } catch (Exception e) {
                                    hadErrors = true;
                                    log.debug(e.getMessage());
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
            dbConnGPSD.close();
            //======================================================
            // Update  GPSD  with the Header Information for the new Database
            //dbConnect.execNoResultSet("update  haystack.gpsd set noOflines = " + lineNo + " where dbname ='" + gpsdDBName + "';");
            sqlToExec = String.format("UPDATE %s.gpsd SET nooflines=%d, gpsd_db='%s' WHERE gpsd_id=%d;", searchPath, lineNo, gpsdDBName, gpsdId);
            dbConnect.execNoResultSet(sqlToExec);
            dbConnect.close();

        } catch (Exception e) {
            hadErrors = true;
            log.error(e.toString());
        }

        return hadErrors;
    }

    private void loadQueries(String extTableName, Integer QueryId, String userId) throws SQLException {

        String strRunId = String.format("%05d", QueryId);
        String queryLogTableName = userId + ".qry" + strRunId;
        String schemaName = userId;
        ResultSet rs = null;

        String sql = "SELECT " + haystackSchema + ".load_querylog('" + haystackSchema + "','" + schemaName + "','Queries','" + extTableName + "'," + QueryId + ");";
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
                "FORMAT 'CSV' (delimiter ',' null '' escape '\"' quote '\"');";

        createSchema(userid);
        dbConnect.execNoResultSet("DROP EXTERNAL TABLE IF EXISTS " + userid + "." + extTableName + ";");
        dbConnect.execNoResultSet(sql);

        return extTableName;
    }

    public void createUser(String userId, String password, String org) {
        try {

            String sql = "insert into " + haystackSchema + ".users(userid, password, organization, createddate) values('" + userId +
                    "','" + password + "','" + org + "', now());";
            dbConnect.execNoResultSet(sql);
        } catch (Exception e) {
            log.error(e.toString());
        }
    }

    public Integer getRunId(String userId) {
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

    public void validateSchema() {
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
