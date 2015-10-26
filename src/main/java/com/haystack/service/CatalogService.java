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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/** CatalogService will deal with all functions related to storing and retreving Model in haystack internal
 *  Postgres database
 */
public  class CatalogService {
    private ConfigProperties configProperties;
    private Credentials credentials;
    private String sqlPath;
    private static Logger log = LoggerFactory.getLogger(CatalogService.class.getName());
    private DBConnectService dbConnect;

    public CatalogService(ConfigProperties configProperties) {
        this.configProperties = configProperties;
        this.credentials = configProperties.getHaystackDBCredentials();
        this.sqlPath = configProperties.properties.getProperty("haystack.sqlDirectory");
        dbConnect = new DBConnectService(DBConnectService.DBTYPE.POSTGRES,this.sqlPath);
        try {
            dbConnect.connect(this.credentials);
        }catch(Exception e){
            log.error("Exception while connecting to Catalog Database" + e.toString());
        }
        validateSchema();
    }

    // LoadQueryLog Method, reads the gzipped query log file from the Upload Directory
    // Pass the UserID, which will determine which schema the QueryLog table will be created
    // Pass the QueryId, against which the QueryLogDates table will be populated
    // Pass the QueryLogDirectory, where the csv log files have been uncompressed


    public boolean processQueryLog(String userId, Integer queryID, String queryLogDirectory) {

        //Integer runID = getRunId(userId);

        String extTableNAme = createExternalTableForQueries(queryLogDirectory, queryID, userId);

        Boolean loadResult = loadQueries(extTableNAme, queryID, userId);

        if (loadResult == true) {
            return true;
        } else {
            return false;
        }

    }

    public void process(String runId, String userId) {
        // json
    }


    public void executeGPSD(String userId, String filename) {
        int lineNo = 0;
        try {
            Integer batchSize = Integer.parseInt(configProperties.properties.getProperty("gpsd.batchsize"));
            StringBuilder sbBatchQueries = new StringBuilder();


            ArrayList<String> fileQueries = new ArrayList<String>();

            String gpsdDBName = "";
            String seqkey = "";
            DBConnectService dbConnGPSD = new DBConnectService(DBConnectService.DBTYPE.GREENPLUM, this.sqlPath);

            // Generate a new Database for this User
            ResultSet rsMaxdbId = dbConnect.execQuery("select lpad(((coalesce(max(seqkey),0)+1)::text), 4, '0')\n" +
                    "from haystack.gpsd\n" +
                    "where userid = '" + userId + "'");
            while (rsMaxdbId.next()) {
                seqkey = rsMaxdbId.getString(1);
                gpsdDBName = userId + seqkey;
            }
            rsMaxdbId.close();

            // Create a database
            ConfigProperties tmpConfig = new ConfigProperties();
            tmpConfig.loadProperties();
            Credentials tmpCred = tmpConfig.getGPSDCredentials();
            tmpCred.setDatabase("postgres");

            DBConnectService tmpdbConn = new DBConnectService(DBConnectService.DBTYPE.GREENPLUM, this.sqlPath);
            tmpdbConn.connect(tmpCred);

            tmpdbConn.execNoResultSet("CREATE DATABASE " + gpsdDBName + ";");
            tmpdbConn.close();

            //======================================================
            // Add a line in GPSD for the new Database
            dbConnect.execNoResultSet("insert into haystack.gpsd(userid, dbname, seqkey) "
                    + " values('" + userId + "','" + gpsdDBName + "'," + Integer.parseInt(seqkey) + ");");


            // Get GPSD Credentials
            ConfigProperties gpsdConfig = new ConfigProperties();
            gpsdConfig.loadProperties();

            Credentials gpsdCred = gpsdConfig.getGPSDCredentials();
            gpsdCred.setDatabase(gpsdDBName);

            // Create a new connection object
            dbConnGPSD.connect(gpsdCred);

            String currQuery = "";
            String nextQuery = "";

            BufferedReader reader = new BufferedReader(new FileReader(filename));
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
                            dbConnect.execNoResultSet("update haystack.gpsd set gpsd_db = '" + gpsd_DB + "', gpsd_date = '" + gpsd_date + "' , gpsd_params='"
                                    + gpsd_params + "', gpsd_version = '" + gpsd_version + "', filename ='" + filename + "' where dbname ='" + gpsdDBName + "';");
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
                                    } catch (Exception e) {
                                        // do nothing
                                    }
                                    currBatchSize = 0;
                                    sbBatchQueries.setLength(0);
                                }
                                currBatchSize++;
                            } else { // Not an insert query execute it;
                                try {
                                    dbConnGPSD.execNoResultSet(currQuery);
                                } catch (Exception e) {
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
            dbConnect.execNoResultSet("update  haystack.gpsd set noOflines = " + lineNo + " where dbname ='" + gpsdDBName + "';");
            dbConnect.close();

        } catch (Exception e) {
            log.error(e.toString());
        }
    }


    private Boolean loadQueries(String extTableName, Integer QueryId, String userId) {

        String strRunId = String.format("%05d", QueryId);
        String queryLogTableName = userId + ".qry" + strRunId;
        String schemaName = userId;
        ResultSet rs = null;

        try {

            String sql = "SELECT haystack.load_querylog('" + schemaName + "','QueryLog','" + extTableName + "');";

            dbConnect.execNoResultSet(sql);

            return true;
        } catch (Exception e) {
            log.error(e.toString());
        }
        return true;

    }

    private String createExternalTableForQueries(String queryLogDirectory, Integer queryId, String userid) {
        String extTableName = userid + ".ext_" + queryId;

        try {

            //String qryFileDir = configProperties.properties.getProperty("qry.upload.directory");

            String sql = "\n" +
                    "CREATE EXTERNAL WEB TABLE " + extTableName + "\n" +
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
                    "EXECUTE E'cat " + queryLogDirectory + "/*.csv' ON MASTER\n" +
                    "FORMAT 'CSV' (DELIMITER AS ',' NULL AS '' QUOTE AS '\"');";


            createSchema(userid);
            dbConnect.execNoResultSet("DROP EXTERNAL  TABLE IF EXISTS " + extTableName + ";");
            dbConnect.execNoResultSet(sql);

        } catch (Exception e) {
            log.error(e.toString());
        }
        return extTableName;
    }
    public void createUser(String userId, String password, String org) {
        try {

            String sql = "insert into haystack.users(userid, password, organization, createddate) values('" + userId +
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
                    "FROM haystack.run_log;\n";
            //ResultSet run_id = dbConnect.execQuery(dbConnect.getSQLfromFile("getMaxRunId"));
            ResultSet run_id = dbConnect.execQuery(sql);
            run_id.next();

            l_RunId = run_id.getInt("max_run_id");

            log.info("RUNLOG=" + l_RunId);
            //String sql = "insert into haystack.run_log(run_id, , run_db, run_date, run_user) values(" + l_RunId +
            //        ",'" + credentials.getDatabase() + "', now(),'" + userId + "' );";
            //dbConnect.execNoResultSet(sql);
        }catch(Exception e){
            log.error(e.toString());
        }
        return l_RunId;
    }


    private void createSchema(String schemaName) throws SQLException, IOException {

        try {
            // Create Schema HAYSTACK
            String qry = "select exists (select * from pg_catalog.pg_namespace where nspname = '" + schemaName + "')as result;";
            ResultSet rs = dbConnect.execQuery(qry);
            rs.next();
            if (rs.getBoolean("result") == false) {

                String sqlQry = "create schema " + schemaName + ";";
                int result = dbConnect.execNoResultSet(sqlQry);
                // Create Model Tables
                if (schemaName == "haystack") {
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
            String sqlQry = "select exists (select * from pg_catalog.pg_namespace where nspname = 'haystack');";
            ResultSet rs = dbConnect.execQuery(sqlQry);

            while (rs.next()) {
                String res = rs.getString(1);
                if (res.contains("f")) {
                    log.info("Haystack schema doesnt exist in postgres, creating it now");
                    createSchema("haystack");
                    exists = false;

                } else {
                    exists = true;
                }
            }
        }catch (Exception e){
            log.error(e.toString());
        }
    }
    public void saveQueries(ArrayList<Query> querylist){
        // Save all queries in the Haystack Schema
    }



}
