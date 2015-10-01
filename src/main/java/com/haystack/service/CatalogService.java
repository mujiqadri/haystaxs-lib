package com.haystack.service;

import com.haystack.domain.Query;
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
    public int runId;

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

    public void executeGPSD(String userId, String filename) {
        try {
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
            //======================================================
            // Add a line in GPSD for the new Database
            dbConnect.execNoResultSet("insert into haystack.gpsd(userid, dbname, seqkey) values('" + userId + "','" + gpsdDBName + "'," + Integer.parseInt(seqkey) + ");");

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

            while ((line = reader.readLine()) != null) {
                if (line.trim().length() == 0) {
                    continue;
                }
                // If line has comments ignore the comments line, extract any characters before the comments line
                int x = line.indexOf("--");
                if (x >= 0) {
                    if (x > 0) {
                        String[] strSplit = line.split("--");
                        currQuery = currQuery + " " + strSplit[0];
                    }
                } else {
                    int z = line.indexOf("\\connect");
                    if (z < 0) {


                        int y = line.indexOf(";");
                        if (y >= 0) {
                            String[] strSplit = line.split(";");
                            if (strSplit.length > 0) {
                                currQuery = currQuery + " " + strSplit[0] + ";";
                            } else {
                                currQuery = currQuery + " " + ";";
                            }
                            fileQueries.add(currQuery);
                            try {
                                dbConnGPSD.execNoResultSet(currQuery);
                            } catch (Exception e) {
                                // do nothing
                            }
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
            // All queries parsed now execute them
            for (int k = 0; k < fileQueries.size(); k++) {
                String qry = fileQueries.get(k);
                dbConnGPSD.execNoResultSet(qry);
            }
        } catch (Exception e) {
            log.error(e.toString());
        }
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

    public void genRunId(String userId) {
        try {
            validateSchema();
            // Insert a row in runlog, so that next time, only new queries get inserted in log
            ResultSet run_id = dbConnect.execQuery(dbConnect.getSQLfromFile("getMaxRunId"));
            run_id.next();

            runId = run_id.getInt("max_run_id");

            log.info("RUNLOG=" + run_id);
            String sql = "insert into haystack.run_log(run_id, run_db, run_date, run_user,  run_time) values(" + runId +
                    ",'" + credentials.getDatabase() + "', now());";
            dbConnect.execNoResultSet(sql);
        }catch(Exception e){
            log.error(e.toString());
        }
    }



    private void createSchema() throws SQLException, IOException {

        // Create Schema HAYSTACK
        String sqlQry = "create schema haystack;";
        int result = dbConnect.execNoResultSet(sqlQry);
        // Create Model Tables
        dbConnect.execScript("createTables");

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
                    createSchema();
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

    private void getStatistics(){

    }
    // This function will be called for UI refresh of the model, will
    public void loadTables(String runId){

    }
}
