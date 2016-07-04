package com.haystack.service;

import com.haystack.domain.Tables;
import com.haystack.service.database.Cluster;
import com.haystack.service.database.Greenplum;
import com.haystack.service.database.Netezza;
import com.haystack.service.database.Redshift;
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
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Properties;

/**
 * Created by qadrim on 15-04-23.
 */
/* CatalogService class reads the User Data from Greenplum/HAWQ Database
        * Gets the List of Tables
        * Annotates it with the Statistics
        * Gets Table Type Columnar/Row, compression and partitions
*/
 public class ClusterService {


    private String sqlPath;
    private String haystackSchema;
    private Credentials credentials;
    private Properties properties;
    private Credentials gpsd_credentials;

    static Logger log = LoggerFactory.getLogger(ModelService.class.getName());
    private DBConnectService dbConnect ;

    //public ArrayList < Query > querylist = new ArrayList<Query>();
    //public ArrayList<TablesNamesFinder> tblNamesFinderlist = new ArrayList<TablesNamesFinder>();

    public ClusterService(ConfigProperties config) throws Exception {
        try {
            this.properties = config.properties;
            this.sqlPath = this.properties.getProperty("cluster.sqlDirectory");
            this.haystackSchema = this.properties.getProperty("main.schema");
            dbConnect = new DBConnectService(DBConnectService.DBTYPE.POSTGRES, sqlPath);
            this.credentials = config.getHaystackDBCredentials();
            this.gpsd_credentials = config.getGPSDCredentials();
            dbConnect.connect(credentials);
        } catch (Exception e) {
            log.error(e.toString());
            throw e;
        }

    }

    /*
        public ClusterService(ConfigProperties config, String dbName) throws Exception {
            try {
                this.properties = config.properties;
                this.sqlPath = this.properties.getProperty("cluster.sqlDirectory");
                this.haystackSchema = this.properties.getProperty("main.schema");
                dbConnect = new DBConnectService(DBConnectService.DBTYPE.GREENPLUM, sqlPath);
                this.credentials = config.getHaystackDBCredentials();

                // Commented out on 11Feb2016 ReFactoring Code
                //this.credentials.setDatabase(dbName);
                //dbConnect.connect(credentials);
            } catch (Exception e) {
                log.error(e.toString());
                throw e;
            }

        }
    */
    public Tables getTablesfromCluster() {
        return new Tables();
    }

    /**
     * This method is used by the UI to try to connect to a cluster before registering it in the haystaxs DB.
     */
    public boolean tryConnect(String host, String dbName, String userName, String password, int port, String clusterType) throws Exception {
        boolean result = true;
        Cluster cluster = null;
        Credentials clusterCred = new Credentials();
        clusterCred.setCredentials(host, Integer.toString(port), dbName, userName, password);

        // Instantiate Cluster Object based on type
        switch (clusterType) {
            case "GREENPLUM":
                cluster = new Greenplum();
                break;
            case "REDSHIFT":
                cluster = new Redshift();
                break;
            case "NETEZZA":
                cluster = new Netezza();
                break;
            default:
                throw new Exception("Cluster Type Invalid");
        }

        return (cluster.connect(clusterCred));
    }

    // Refresh method checks the
    public boolean refreshSchemaAndQueryLogs(Integer clusterId) {

        String queryRefreshSchedule = "", schemaRefreshSchedule = "";
        String clusterType;
        Cluster cluster;
        Credentials clusterCred; // Credentials for the Cluster we need to connect to

        try {

            //Get current refresh status of this cluster
            String sql = "SELECT is_refreshing FROM " +haystackSchema +".cluster WHERE cluster_id = " +clusterId;
            ResultSet resultSet = dbConnect.execQuery(sql);

            resultSet.next();
            Boolean isAlreadyRefreshing = resultSet.getBoolean("is_refreshing");

            //If cluster is refreshing already then don't execute further just return.
            if(isAlreadyRefreshing) {
                return false;
            }

            // Fetch cluster details from HayStack Schema
            sql = "select cluster_id, cluster_db, host , dbname ,password , username ,port, coalesce(last_queries_refreshed_on, '1900-01-01') as last_queries_refreshed_on, " +
                    " coalesce(last_schema_refreshed_on,'1900-01-01') as last_schema_refreshed_on ,db_type as cluster_type, now() as current_time  from " + haystackSchema +
                    ".cluster where host is not null and is_active = true and cluster_id = " + clusterId + ";";
            ResultSet rs = dbConnect.execQuery(sql);
            while (rs.next()) {

                //Integer clusterId = rs.getInt("cluster_id");
                Timestamp lastQueryRefreshTime = rs.getTimestamp("last_queries_refreshed_on");
                Timestamp lastSchemaRefreshTime = rs.getTimestamp("last_schema_refreshed_on");
                Timestamp currentTime = rs.getTimestamp("current_time");
                clusterType = rs.getString("cluster_type");

                String hostName = rs.getString("host");
                Boolean isGPSD = false;
                if (hostName == null || hostName.length() == 0) { // load from stats
                    isGPSD = true;
                }

                if (isGPSD) { // load from stats
                    clusterCred = this.gpsd_credentials;
                    clusterCred.setDatabase(rs.getString("cluster_db"));
                } else {
                    clusterCred = getCredentials(rs);
                }

                // Instantiate Cluster Object based on type
                switch (clusterType) {
                    case "GREENPLUM":
                        cluster = new Greenplum();
                        break;
                    case "REDSHIFT":
                        cluster = new Redshift();
                        break;
                    case "NETEZZA":
                        cluster = new Netezza();
                        break;
                    default:
                        throw new Exception("Cluster Type Invalid");
                }

                try {
                    Boolean connectSuccess = cluster.connect(clusterCred);
                    if (!connectSuccess) { // Set IsActive = false for this gpsd_id, so that we donot try to connect it each time
                        sql = "UPDATE " + haystackSchema + ".cluster set is_active = false where cluster_id = " + clusterId + ";";
                        //TODO Add Notification for User - UserInbox
                        //TODO  Comment out for now, enable when scheduled refresh it done
                        //dbConnect.execNoResultSet(sql);
                        continue;
                    }

                    //Set is_refreshing to TRUE for this cluster so that 2nd refresh do not start until current refresh finishes.
                    sql = "UPDATE " +haystackSchema +".cluster SET is_refreshing = true WHERE cluster_id = " + clusterId;
                    dbConnect.execNoResultSet(sql);

                    Integer queryRefreshIntervalHours = Integer.parseInt(this.properties.getProperty("query.refresh.interval.hours"));
                    // Add Refresh Interval to Last refresh time and then check
                    Timestamp newQueryRefreshTime = new Timestamp(lastQueryRefreshTime.getTime() + (queryRefreshIntervalHours * 60 * 60 * 1000));
                    if (newQueryRefreshTime.compareTo(currentTime) < 0) { // if this time is less than current_time, if yes then refresh queries
                        cluster.loadQueries(clusterId, lastQueryRefreshTime);
                    }

                    Integer schemaRefreshIntervalHours = Integer.parseInt(this.properties.getProperty("schema.refresh.interval.hours"));
                    lastSchemaRefreshTime.setTime(lastSchemaRefreshTime.getTime() + (schemaRefreshIntervalHours * 60 * 60 * 1000));
                    if (lastSchemaRefreshTime.compareTo(currentTime) < 0) {// if this time is less than current_time, if yes then refresh schema
                        Tables tables = cluster.loadTables(clusterCred, isGPSD, clusterId);
                    }

                    //Set is_refreshing to FALSE for this cluster so that user can perform refresh again.
                    sql = "UPDATE " + haystackSchema +".cluster SET is_refreshing = false WHERE cluster_id = " + clusterId;
                    dbConnect.execNoResultSet(sql);

                } catch (Exception e) {
                    log.error("Error in refreshing cluster cluster_id= " + clusterId + " Exception=" + e.toString());
                }
            }
            rs.close();

        } catch (Exception e) {
            log.error("Error in fetching cluster from " + haystackSchema + " schema.");
            return false;
        }

        return true;
    }

    private Credentials getCredentials(ResultSet rs) {
        Credentials cred = new Credentials();
        try {
            String sHost = rs.getString("host");
            String dbname = rs.getString("dbname");
            String username = rs.getString("username");
            String password = rs.getString("password");
            Integer port = rs.getInt("port");

            if (sHost.length() == 0 || dbname.length() == 0 || username.length() == 0 || password.length() == 0) {
                Exception e = new Exception();
                throw e;
            }
            cred.setCredentials(sHost, port.toString(), dbname, username, password);
        } catch (Exception e) {
            log.error("Error in getting credentials for cluster: " + e.toString());
            return null;
        }
        return cred;
    }

    // Read File with multiple SQL statements, spread across multiple lines, and includes comments
    public void readSQLFile(String filename) {
        try {
            ArrayList<String> fileQueries = new ArrayList<String>();

            String currQuery = "";
            String nextQuery = "";

            BufferedReader reader = new BufferedReader(new FileReader(filename));
            String line = null;

            String ls = System.getProperty("line.separator");

            while ((line = reader.readLine()) != null) {
                if (line.trim().length()==0){
                    continue;
                }
                // If line has comments ignore the comments line, extract any characters before the comments line
                int x = line.indexOf("--");
                if (x >= 0) {
                    if (x > 0) {
                        String[] strSplit = line.split("--");
                        currQuery = currQuery + " " + strSplit[0];
                    }
                }
                else {
                    int y = line.indexOf(";");
                    if (y >= 0) {
                        String[] strSplit = line.split(";");
                        if (strSplit.length >0) {
                            currQuery = currQuery + " " +  strSplit[0] + ";";
                        }
                        else
                        {
                            currQuery = currQuery + " " + ";";
                        }
                        fileQueries.add(currQuery);
                        currQuery = "";
                        if (strSplit.length > 1) {
                            currQuery = strSplit[1];
                        }
                    }
                    else {  // No comment or end of line character append to CurrQuery
                        currQuery = currQuery + " " + line;
                    }
                }
            }
        }catch(Exception e) {
            log.error(e.toString());
        }
    }

    // This method gets the table details from statistics loaded from GPSD file
    public Tables getTables(Integer cluster_id) {
        Tables tablelist = new Tables();
        String clusterType = "";
        Cluster cluster;
        Credentials clusterCred = new Credentials();
        try {

            String sql = "select cluster_id, cluster_db, host , dbname ,password , username ,port, coalesce(last_queries_refreshed_on, '1900-01-01') as last_queries_refreshed_on, " +
                    " coalesce(last_schema_refreshed_on,'1900-01-01') as last_schema_refreshed_on ,db_type as cluster_type, now() as current_time  from " + haystackSchema +
                    ".cluster where host is not null and is_active = true and cluster_id = " + cluster_id;
            ResultSet rs = dbConnect.execQuery(sql);

            while (rs.next()) {

                Integer clusterId = rs.getInt("cluster_id");
                Timestamp lastQueryRefreshTime = rs.getTimestamp("last_queries_refreshed_on");
                Timestamp lastSchemaRefreshTime = rs.getTimestamp("last_schema_refreshed_on");
                Timestamp currentTime = rs.getTimestamp("current_time");
                clusterType = rs.getString("cluster_type");

                String hostName = rs.getString("host");
                Boolean isGPSD = false;
                if (hostName == null || hostName.length() == 0) { // load from stats
                    isGPSD = true;
                }

                if (isGPSD) { // load from stats
                    clusterCred = this.gpsd_credentials;
                    clusterCred.setDatabase(rs.getString("cluster_db"));
                } else {
                    clusterCred = getCredentials(rs);
                }

                // Instantiate Cluster Object based on type
                switch (clusterType) {
                    case "GREENPLUM":
                        cluster = new Greenplum();
                        break;
                    case "REDSHIFT":
                        cluster = new Redshift();
                        break;
                    case "NETEZZA":
                        cluster = new Netezza();
                        break;
                    default:
                        throw new Exception("Cluster Type Invalid");
                }
                tablelist = cluster.loadTables(clusterCred, isGPSD, clusterId);
                // cluster.saveGpsdStats(gpsd_id, tablelist);
            }

        } catch (Exception e) {
            log.error("Error while fetching table details from CLUSTER:" + e.toString());
        }
        return tablelist;
    }



}
