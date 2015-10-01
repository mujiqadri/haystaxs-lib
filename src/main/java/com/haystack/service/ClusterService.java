package com.haystack.service;

import com.haystack.domain.Column;
import com.haystack.domain.Query;
import com.haystack.domain.Table;
import com.haystack.domain.Tables;
import com.haystack.parser.util.TablesNamesFinder;
import com.haystack.util.ConfigProperties;
import com.haystack.util.Credentials;
import com.haystack.util.DBConnectService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.Date;

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
    private Credentials credentials;
    private Properties properties;

    public ArrayList<String> fileQueries = new ArrayList<String>();

    static Logger log = LoggerFactory.getLogger(ModelService.class.getName());
    private DBConnectService dbConnect ;

    public ArrayList < Query > querylist = new ArrayList<Query>();
    public ArrayList<TablesNamesFinder> tblNamesFinderlist = new ArrayList<TablesNamesFinder>();



    public ClusterService(ConfigProperties config) throws Exception{
        try {
            this.properties = config.properties;
            this.sqlPath = this.properties.getProperty("cluster.sqlDirectory");
            dbConnect = new DBConnectService(DBConnectService.DBTYPE.GREENPLUM,sqlPath);
            this.credentials = config.getUserDataCredentials();
            dbConnect.connect(credentials);
        }catch (Exception e){
            log.error(e.toString());
            throw e;
        }
    }

    // Read File with multiple SQL statements, spread across multiple lines, and includes comments
    public void readSQLFile(String filename) {
        try {

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
    // Get Tables from Cluster along with thier Structure and Stats
    public Tables getTablesfromCluster() {
        Tables tablelist = new Tables();

        log.info("Populating table properties for Schemas where threshold has passed");

        Integer schemaThreshold = Integer.parseInt(this.properties.getProperty("schema.change.threshold.days"));

        try {
            // Check if schema.change.threshold has passed from the last run_log timestamp

            String sqlSchemas = "select a.schema_name, B.max_run_date, now()\n" +
                    "from information_schema.schemata A\n" +
                    "\n" +
                    "left outer join \n" +
                    "\t(\n" +
                    "\tselect run_schema, max(run_date) as max_run_date\n" +
                    "\tfrom haystack.run_log\n" +
                    "\tgroup by run_schema\n" +
                    "\t) AS B\n" +
                    "on A.schema_name = B.run_schema\n" +
                    "where A.schema_name not in ('pg_toast','pg_bitmapindex','madlib','pg_aoseg','pg_catalog'\n" +
                    ",'gp_toolkit','information_schema','haystack')\n" +
                    "and now() > COALESCE(B.max_run_date, '1900-01-01')::DATE  + INTERVAL '" + schemaThreshold.toString() + " days' ";
            ResultSet rsSchemas = dbConnect.execQuery(sqlSchemas);

            while (rsSchemas.next()){

                String sRunID = "";
                String schemaName = rsSchemas.getString("schema_name");
                Date runDate = rsSchemas.getDate("max_run_date");

                log.info("Processing tables in Schema:" + schemaName);

                String sqlRunStats = "select haystack.capturetabledetailsgpdb('" + schemaName + "','" + this.properties.getProperty("haystack.ds_cluster.username") + "')";
                ResultSet rsRes = dbConnect.execQuery(sqlRunStats);
                while (rsRes.next()) {
                    sRunID = rsRes.getString(1);
                }
                rsRes.close();

                // Now Update SizeOnDisk for all tables in schema
                String sqlSize = "\t\tselect sotailtablename as tableName, \n" +
                        "\t\tto_char((sotailtablesizeuncompressed :: FLOAT8 / 1024 / 1024 / 1024),\n" +
                        "\t\t\t'FM9999999999.0000') as SizeOnDiskU,\n" +
                        "\t\tto_char((sotailtablesizedisk :: FLOAT8 / 1024 / 1024 / 1024),\n" +
                        "\t\t\t'FM9999999999.0000') as SizeOnDisk,\n" +
                        "\t\tto_char((sotailtablesizeuncompressed - sotailtablesizedisk) \n" +
                        "\t\t\t* 100 / sotailtablesizeuncompressed, 'FM999') as CompressRatio\n" +
                        "\t\t\t\n" +
                        "\t\tfrom gp_toolkit.gp_size_of_table_and_indexes_licensing \n" +
                        "\t\twhere sotailschemaname = '" + schemaName + "'";

                ResultSet rsSize = dbConnect.execQuery(sqlSize);
                while (rsSize.next()){
                    String sTableName = rsSize.getString("tableName");
                    Float sizeOnDiskU = rsSize.getFloat("SizeOnDiskU");
                    Float sizeOnDisk = rsSize.getFloat("SizeOnDisk");
                    Float compressRatio = rsSize.getFloat("CompressRatio");

                    String sqlUpdTbl = "update haystack.tables set sizeInGB = " + sizeOnDisk +
                            ", sizeInGBU = " + sizeOnDiskU + ", compressRatio = " + compressRatio +
                            " where schema_name = '" + schemaName + "' and table_name = '" + sTableName + "'" ;
                    dbConnect.execNoResultSet(sqlUpdTbl);
                }
                rsSize.close();
            }
            log.info("Schema Processing Complete");
            rsSchemas.close();

            // Load Tables into Memory
            tablelist.load(dbConnect);

        }catch(Exception e){
            log.error("Error while fetching table details from cluster:" + e.toString());
        }
        return tablelist;
    }

    //public void
    public void getQueriesfromDB() {
        log.info("Getting Queries from Cluster Log Tables");

        // Load queries into HayStack.gp_queries_log for all queries not in last runlog
        //dbConnect.execScript("loadQueriesIntoLogTable");
        try {

            // Load All Queries from HayStack.gp_queries_log into memory and return List<Query> back to processor
            long slidingWindowDays = Integer.parseInt(properties.getProperty("query.window.days"));


            // Fetch all tables from within the current database and schema
            String sqlQry = dbConnect.getSQLfromFile("getAllQueries");

            PreparedStatement ps = dbConnect.prepareStatement(sqlQry);

            final long ONE_DAY_MILLISCONDS = 25 * 60 * 60 * 1000;
            Timestamp tenDaysAgo = new Timestamp(System.currentTimeMillis() - ( slidingWindowDays * ONE_DAY_MILLISCONDS));

            ps.setTimestamp(1, tenDaysAgo);
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                Query query = new Query();
                query.add(rs.getDate("logtime"), rs.getString("loguser"), rs.getString("logdatabase"), rs.getString("logpid"),
                        rs.getString("logthread"), rs.getString("loghost"), rs.getString("logsegment"), rs.getString("SQLText"));

                querylist.add(query);
            }
            log.info("Done Getting Queries from Cluster Log Tables");
        } catch (Exception e) {
            log.error("Error getting queries from Cluster Log" + e.toString());


        }
    }
    public List<Query> getQueriesfromFile(){
        return null;
    }
}
