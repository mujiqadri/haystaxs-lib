package com.haystack.service;

import com.haystack.domain.Query;
import com.haystack.util.ConfigProperties;
import com.haystack.util.Credentials;
import com.haystack.util.DBConnectService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        this.credentials = configProperties.getCredentials();
        this.sqlPath = configProperties.properties.getProperty("haystack.sqlDirectory");
        dbConnect = new DBConnectService(DBConnectService.DBTYPE.POSTGRES,this.sqlPath);
        try {
            dbConnect.connect(this.credentials);
        }catch(Exception e){
            log.error("Exception while connecting to Catalog Database" + e.toString());
        }
        validateSchema();
    }
    public void genRunId () {
        try {
            validateSchema();
            // Insert a row in runlog, so that next time, only new queries get inserted in log
            ResultSet run_id = dbConnect.execQuery(dbConnect.getSQLfromFile("getMaxRunId"));
            run_id.next();

            runId = run_id.getInt("max_run_id");

            log.info("RUNLOG=" + run_id);
            String sql = "insert into haystack.run_log(run_id, _database, run_time) values(" + runId +
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
