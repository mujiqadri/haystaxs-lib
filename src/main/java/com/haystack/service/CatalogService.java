package com.haystack.service;

import com.haystack.domain.*;
import com.haystack.parser.expression.StringValue;
import com.haystack.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.PreparedStatement;
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


    public String getClusterJson(int cluster_id) {

        Integer user_id = null;
        String json = "";
        try {
            String sql = "select A.cluster_db, C.user_id, C.user_name\n" +
                    "from " + haystackSchema + ".cluster A , " + haystackSchema + ".users C \n" +
                    "where C.user_id = A.user_id AND A.cluster_id = " + cluster_id;

            ResultSet rs = dbConnect.execQuery(sql);

            rs.next();

            String gpsd_db = rs.getString("cluster_db");
            user_id = rs.getInt("user_id");
            //Date startDate = rs.getDate("start_date");
            //Date endDate = rs.getDate("end_date");
            String schemaName = rs.getString("user_name");

            rs.close();

            ClusterService clusterService = new ClusterService(this.configProperties);
            Tables tablelist = clusterService.getTables(cluster_id);

            saveClusterStats(cluster_id, tablelist);
            json = tablelist.getJSON();

        } catch (Exception e) {
            log.error("Error in getting Json (cluster might not exist) from CLUSTER:" + cluster_id + " Exception:" + e.toString());
            HSException hsException = new HSException("CatalogService.getClusterJson()", "Error in getting Json for CLUSTER", e.toString(), "cluster_id=" + cluster_id, user_id);
        }
        return json;
    }

    private void saveClusterStats(int clusterId, Tables tables) {
        for (Table table : tables.tableHashMap.values()) {
            HashMap<String, Object> mapValues = new HashMap<>();

            mapValues.put("cluster_id", clusterId);
            mapValues.put("schema_name", table.schema);
            mapValues.put("table_name", table.tableName);
            mapValues.put("size_in_mb", table.stats.sizeOnDisk * 1024);
            mapValues.put("no_of_rows", table.stats.noOfRows);

            try {
                dbConnect.insert(haystackSchema + ".cluster_stats", mapValues);
            } catch (Exception ex) {
                log.error("Error inserting cluster stats in cluster_stats table for cluster_id = " + clusterId + " and for table = " + table.tableName + " ;Exception:" + ex.toString());
                //HSException hsException = new HSException("CatalogService.getGPSDJson()", "Error in getting Json for GPSD", e.toString(), "gpsd_id=" + gpsd_id, user_id);
            }
        }
    }
    // ProcessWorkload Method, will take 1 CLUSTER DB and a date range and run Analysis on these queries
    // to generate a JSON model against the workload

    public void processWorkload(Integer workloadId) {
        Integer user_id = null;
        GeoLocation geoLocation = new GeoLocation();

        try {

            //For AuditTrail
            String queryAuditTrailSQL = "INSERT INTO " +haystackSchema +".query_audit_trail(query_id, log_user, database_name, schema_name, table_name, column_name, usage_type, usage_location, ip_address, geo_location, access_time)VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
            PreparedStatement preparedStatement = dbConnect.prepareStatement(queryAuditTrailSQL);

            // Fetch Cluster Credentials from CLUSTER against the WorkloadID
            // Load this in the TableList


            // For Queries select the AdminUser and fetch all the queries against the user
            // Based on StartDate and EndDate of the Cluster

            // Get Database Name against CLUSTERID,-> DBName
            String sql = "select A.workload_id, A.cluster_id, A.start_date, A.end_date, B.cluster_db, B.dbname, C.user_id, C.user_name\n" +
                    "from " + haystackSchema + ".workloads A, " + haystackSchema + ".cluster B , " + haystackSchema + ".users C \n" +
                    "where A.cluster_id = B.cluster_id and C.user_id = A.user_id AND A.workload_id = " + workloadId;

            ResultSet rs = dbConnect.execQuery(sql);

            rs.next();

            String cluster_db = rs.getString("cluster_db");
            String dbname = rs.getString("dbname");

            Integer cluster_id = rs.getInt("cluster_id");
            user_id = rs.getInt("user_id");
            Date startDate = rs.getDate("start_date");
            Date endDate = rs.getDate("end_date");
            String schemaName = rs.getString("user_name");

            ClusterService clusterService = new ClusterService(this.configProperties);
            Tables tablelist = clusterService.getTables(cluster_id);

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
            // Update: 11Jan2016= Fetch only queries related to the cluster_db linked to this workload
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
                    " and logdatabase = '" + dbname + "'";        // Get queries related to the Cluster database to minimze repeat processing of queries
            //" and lower(sql) like 'set%search_path%'" +  // Added this to test search_path functionality
            String orderbySQL = " order by logsessiontime;";

            // Count # of Rows so that Workload Processing Percentage can be updated

            sql = "select count(1) as NoOfQueries FROM " + whereSQL;
            ResultSet rsCnt = dbConnect.execQuery(sql);

            rsCnt.next();
            int totalQueries = rsCnt.getInt("NoOfQueries");
            int percentProcessed = 0;
            int currQryCounter = 0;

            sql = "select cluster_id, loguser, logdatabase, id as queryId, sql, EXTRACT(EPOCH FROM logduration) as duration_Seconds, logduration, logsessiontime, loghost from " + whereSQL + orderbySQL;
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
                    String clusterUser = rsQry.getString("loguser");
                    String clusterDatabaseName = rsQry.getString("logdatabase");
                    String currQry = rsQry.getString("sql");
                    Integer queryId = rsQry.getInt("queryId");
                    currQry = currQry.toLowerCase(); // convert sql to lower case for ease of processing

                    Timestamp duration = rsQry.getTimestamp("logduration");
                    Integer durationSeconds = rsQry.getInt("duration_seconds");
                    Timestamp logsessiontime = rsQry.getTimestamp("logsessiontime");
                    String logHost = rsQry.getString("loghost");

                    String[] arrQry = currQry.split(";");

                    for (int i = 0; i < arrQry.length; i++) {
                        String sQry = arrQry[i];

                        // Check if the SQL statement is set search_path statement, if yes then store this in the search_path variable
                        if (sQry.trim().toLowerCase().startsWith("set search_path") == true) {
                            String[] s1 = sQry.split("=");
                            current_search_path = s1[1].toString().replaceAll("\\s+", "");
                            continue;
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
                            ms.processSQL(queryId, nQry, clusterUser, clusterDatabaseName, durationSeconds, user_id, current_search_path);
//                            ms.processSQL(queryId, nQry, durationSeconds, user_id, current_search_path);

                            //For AuditTrail
                            String queryType  = ms.getQueryType();

                            HashMap<String, HashMap<String, Set<Column>>> columnsForAuditTrail = ms.getColumnsForAuditTrail();
                            Iterator<Map.Entry<String, HashMap<String, Set<Column>>>> usageLocationIterator = columnsForAuditTrail.entrySet().iterator();

                            while(usageLocationIterator.hasNext()){
                                Map.Entry<String, HashMap<String, Set<Column>>> usageLocationEntry = usageLocationIterator.next();
                                String usageLocation = usageLocationEntry.getKey();

                                HashMap<String, Set<Column>> tablesHashMap = usageLocationEntry.getValue();
                                Iterator<Map.Entry<String, Set<Column>>> tablesIterator = tablesHashMap.entrySet().iterator();

                                while(tablesIterator.hasNext()){
                                    Map.Entry<String, Set<Column>> currentTable = tablesIterator.next();
                                    Set<Column> columns = currentTable.getValue();

                                    Iterator<Column> columnIterator = columns.iterator();
                                    while (columnIterator.hasNext()){
                                        Column column = columnIterator.next();
                                        String columnName = column.column_name;
                                        String tableName = column.getResolvedTableName();
                                        String tableSchemaName = column.getResolvedSchemaName();

                                        String geoLocationLocation = "";
                                        if(logHost != null && !logHost.isEmpty()){
                                            geoLocation.setIP(logHost);
                                            geoLocationLocation = geoLocation.getLatitude() +"," +geoLocation.getLongitude();
                                        }

                                        preparedStatement.setInt(1, queryId);
                                        preparedStatement.setString(2, clusterUser);
                                        preparedStatement.setString(3, clusterDatabaseName);
                                        preparedStatement.setString(4, tableSchemaName);
                                        preparedStatement.setString(5, tableName);
                                        preparedStatement.setString(6, columnName);
                                        preparedStatement.setString(7, queryType);
                                        preparedStatement.setString(8, usageLocation);
                                        preparedStatement.setString(9, logHost);
                                        preparedStatement.setString(10,geoLocationLocation);
                                        preparedStatement.setTimestamp(11, logsessiontime);
                                        preparedStatement.addBatch();
                                    }
                                }
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

            preparedStatement.executeBatch(); //Persist AuditTrail Data in Database

            ms.scoreModel();
            ms.generateRecommendations(cluster_id);
            String model_json = ms.getModelJSON();

            //Persist the tables states and everything in database
            persistInDatabase(workloadId, ms.getTableList());

            // Create a UserInbox Message for Completed Processing
            Date date3 = new Date();
            saveUserInboxMsg(user_id, "WORKLOAD_STATUS", "COMPLETED", "Workload Processing Completed @DateTime=" + dateFormat.format(date3) + " For Workload: Id:" + workloadId + " dbname:" + dbname + " Start:" + startDate + " End:" + endDate
                    , "WORKLOAD PROCESSING COMPLETED", "CatalogService.processWorkload");

            //sql = "update " + haystackSchema + ".workloads set model_json ='" + model_json + "' where workload_id =" + workloadId + ";";
            //dbConnect.execNoResultSet(sql);

            //Save generated json into the workloads_json table so that it can be accessed from anywhere
            sql = "INSERT INTO " +haystackSchema +".workloads_json (workload_id, workload_json) VALUES(" +workloadId +",'" +model_json +"')";
            int r = dbConnect.execNoResultSet(sql);

        } catch (Exception e) {
            // Create a UserInbox Message for Error in Processing
            Date date = new Date();
            saveUserInboxMsg(user_id, "WORKLOAD_STATUS", "ERROR", "Workload Processing Error @DateTime=" + dateFormat.format(date) + " For Workload: Id:" + workloadId + " Exception :" + e.toString()
                    , "WORKLOAD PROCESSING ERROR", "CatalogService.processWorkload");
            log.error("Error in Processing WorkloadId:" + workloadId + " Exception:" + e.toString());
            HSException hsException = new HSException("CatalogService.processWorkload()", "Error in processing workload", e.toString(), "WorkloadId=" + workloadId, user_id);
        }
    }

    public void persistInDatabase(int workloadId, Tables tablesList) {
        String tableInsertSQL = "INSERT INTO " +haystackSchema +".wl_table(wl_table_id, workload_id, oid, table_name, database, schema, dkarray, skew, iscolumnar, iscompressed, storage_mode, compress_level, noofrows, size_on_disk, size_uncompressed, compression_ratio, no_of_columns, ispartitioned, relpage, size_for_display_compressed, size_for_display_uncompressed, model_score, usage_frequency, execution_time, workload_score)VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
        String columnInsertSQL = "INSERT INTO " +haystackSchema +".wl_table_column(wl_table_id, wl_table_column_id, column_name, ordinal_position, data_type, isdk, character_max_length, numeric_precision, numeric_precision_radix, numeric_scale, usage_frequency, where_usage, ispartitioned, partition_level, position_in_partition_key)VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
        String tableDKInsertSQL = "INSERT INTO " +haystackSchema +".wl_table_dk(wl_table_id, wl_table_column_id)VALUES (?, ?);";
        String tablePartitionColumnInsertSQL = "INSERT INTO " +haystackSchema +".wl_table_partitioncolumn(wl_table_id, wl_table_column_id)VALUES (?, ?);";
        String tableJoinInsertSQL = "INSERT INTO " +haystackSchema +".wl_table_join(wl_table_id, wl_join_id, left_schema, left_table, right_schema, right_table, level, support_count, confidence)VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);";
        String tableJoinTupleInsertSQL = "INSERT INTO " +haystackSchema +".wl_table_join_jointuple(wl_join_id, left_column, right_column) VALUES (?, ?, ?);\n";
        String tablePartitionInsertSQL = "INSERT INTO " +haystackSchema +".wl_table_partition(wl_table_id, wl_table_partition_id, table_name, partition_name, level, type, rank, \"position\", list_values, range_start, range_start_inclusive, range_end, range_end_inclusive, every_clause, isdefault, boundary, reltuples, relpages)VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
        String childPartitionInsertSQL = "INSERT INTO " +haystackSchema +".wl_table_child_partition(wl_table_id, wl_table_partition_id, table_name, partition_name, level, type, rank, \"position\", list_values, range_start, range_start_inclusive, range_end, range_end_inclusive, every_clause, isdefault, boundary, reltuples, relpages)VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";;
        String tableRecommandationInsertSQL = "INSERT INTO " +haystackSchema +".wl_table_recommendation(wl_table_id, oid, schema, table_name, type, description, anamoly) VALUES (?, ?, ?, ?, ?, ?, ?);";

        try {
            PreparedStatement tableInsertStatement = dbConnect.prepareStatement(tableInsertSQL);
            PreparedStatement columnInsertStatement = dbConnect.prepareStatement(columnInsertSQL);
            PreparedStatement tableDKInsertStatement = dbConnect.prepareStatement(tableDKInsertSQL);
            PreparedStatement tablePartitionColumnInsertStatement = dbConnect.prepareStatement(tablePartitionColumnInsertSQL);
            PreparedStatement tableJoinInsertStatement = dbConnect.prepareStatement(tableJoinInsertSQL);
            PreparedStatement tableJoinTupleInsertStatement = dbConnect.prepareStatement(tableJoinTupleInsertSQL);
            PreparedStatement tablePartitionInsertStatement = dbConnect.prepareStatement(tablePartitionInsertSQL);
            PreparedStatement childPartitionInsertStatement = dbConnect.prepareStatement(childPartitionInsertSQL);
            PreparedStatement tableRecommandationInsertStatement = dbConnect.prepareStatement(tableRecommandationInsertSQL);

            Iterator<Map.Entry<String, Table>> tablesIterator = tablesList.tableHashMap.entrySet().iterator();

            while (tablesIterator.hasNext()) {
                Table currTable = tablesIterator.next().getValue();

                log.debug("Persisting Table: " +currTable.schema+"." +currTable.tableName);

                //1. Get new table id from sequence
                ResultSet resultSet = dbConnect.execQuery("SELECT nextval('" + haystackSchema + ".seq_wl_table') as new_table_id");
                resultSet.next();
                int tableId = resultSet.getInt("new_table_id");
                resultSet.close();

                //2. Persist table info in database
                //wl_table_id, workload_id, oid, table_name, database, schema, dkarray, skew, iscolumnar, iscompressed, storage_mode,
                // compress_level, noofrows, size_on_disk, size_uncompressed, compression_ratio, no_of_columns, ispartitioned,
                // relpage, size_for_display_compressed, size_for_display_uncompressed, model_score, usage_frequency, execution_time, workload_score
                tableInsertStatement.setInt(1, tableId);
                tableInsertStatement.setInt(2, workloadId);
                tableInsertStatement.setString(3, currTable.oid);
                tableInsertStatement.setString(4, currTable.tableName);
                tableInsertStatement.setString(5, currTable.database);
                tableInsertStatement.setString(6, currTable.schema);
                tableInsertStatement.setString(7, currTable.dkArray);
                tableInsertStatement.setFloat(8, currTable.stats.skew);
                tableInsertStatement.setBoolean(9, currTable.stats.isColumnar);
                tableInsertStatement.setBoolean(10, currTable.stats.isCompressed);
                tableInsertStatement.setString(11, currTable.stats.storageMode);
                tableInsertStatement.setInt(12, currTable.stats.compressLevel);
                tableInsertStatement.setDouble(13, currTable.stats.noOfRows);
                tableInsertStatement.setFloat(14, currTable.stats.sizeOnDisk);
                tableInsertStatement.setFloat(15, currTable.stats.sizeUnCompressed);
                tableInsertStatement.setFloat(16, currTable.stats.compressionRatio);
                tableInsertStatement.setInt(17, currTable.stats.noOfColumns);
                tableInsertStatement.setBoolean(18, currTable.stats.isPartitioned);
                tableInsertStatement.setInt(19, currTable.stats.relPages);
                tableInsertStatement.setString(20, currTable.stats.sizeForDisplayCompressed);
                tableInsertStatement.setString(21, currTable.stats.sizeForDisplayUnCompressed);
                tableInsertStatement.setFloat(22, currTable.stats.getModelScore());
                tableInsertStatement.setInt(23, currTable.stats.getTableUsageFrequency());
                tableInsertStatement.setDouble(24, currTable.stats.getExecutionTime());
                tableInsertStatement.setFloat(25, currTable.stats.getWorkloadScore());

                Iterator<Map.Entry<String, Column>> columnsIterator = currTable.columns.entrySet().iterator();
                while (columnsIterator.hasNext()) {
                    Column currTableColumn = columnsIterator.next().getValue();

                    //3.Get new column id from sequence
                    resultSet = dbConnect.execQuery("SELECT nextval('" + haystackSchema + ".seq_wl_table_column') as new_column_id");
                    resultSet.next();
                    int columnId = resultSet.getInt("new_column_id");
                    resultSet.close();

                    //4. Persist table column info in database
                    //wl_table_id, wl_table_column_id, column_name, ordinal_position, data_type, isdk, character_max_length,
                    // numeric_precision, numeric_precision_radix, numeric_scale, usage_frequency, where_usage, ispartitioned,
                    // partition_level, position_in_partition_key
                    columnInsertStatement.setInt(1, tableId);
                    columnInsertStatement.setInt(2, columnId);
                    columnInsertStatement.setString(3, currTableColumn.column_name);
                    columnInsertStatement.setInt(4, currTableColumn.ordinal_position);
                    columnInsertStatement.setString(5, currTableColumn.data_type);
                    columnInsertStatement.setBoolean(6, currTableColumn.isDK);
                    columnInsertStatement.setInt(7, currTableColumn.character_maximum_length);
                    columnInsertStatement.setInt(8, currTableColumn.numeric_precision);
                    columnInsertStatement.setInt(9, currTableColumn.numeric_precision_radix);
                    columnInsertStatement.setInt(10, currTableColumn.numeric_scale);
                    columnInsertStatement.setInt(11, currTableColumn.getUsageScore());
                    columnInsertStatement.setInt(12, currTableColumn.getWhereUsage());
                    columnInsertStatement.setBoolean(13, currTableColumn.isPartitioned);
                    columnInsertStatement.setInt(14, currTableColumn.partitionLevel);
                    columnInsertStatement.setInt(15, currTableColumn.positionInPartitionKey);

                    //5. if Current Column is a distribution key of current table then persist it in database
                    if (currTable.dk.containsKey(currTableColumn.column_name)) {
                        tableDKInsertStatement.setInt(1, tableId);
                        tableDKInsertStatement.setInt(2, columnId);
                        tableDKInsertStatement.addBatch();
                    }

                    //5. if Current Column is a partition column of current table then persist it in database
                    if (currTable.partitionColumn.containsKey(currTableColumn.column_name)) {
                        tablePartitionColumnInsertStatement.setInt(1, tableId);
                        tablePartitionColumnInsertStatement.setInt(2, columnId);
                        tablePartitionColumnInsertStatement.addBatch();
                    }

                    columnInsertStatement.addBatch();
                }

                Iterator<Map.Entry<String, Join>> joinsIterator = currTable.joins.entrySet().iterator();
                while (joinsIterator.hasNext()) {
                    Join join = joinsIterator.next().getValue();

                    // Get new join id from sequence
                    resultSet = dbConnect.execQuery("SELECT nextval('" + haystackSchema + ".seq_wl_table_join') as new_join_id ");
                    resultSet.next();
                    int joinId = resultSet.getInt("new_join_id");

                    //Persist Join in database
                    //wl_table_id, wl_join_id, left_schema, left_table, right_schema, right_table, level, support_count, confidence
                    tableJoinInsertStatement.setInt(1, tableId);
                    tableJoinInsertStatement.setInt(2, joinId);
                    tableJoinInsertStatement.setString(3, join.leftSchema);
                    tableJoinInsertStatement.setString(4, join.leftTable);
                    tableJoinInsertStatement.setString(5, join.rightSchema);
                    tableJoinInsertStatement.setString(6, join.rightTable);
//                    tableJoinInsertStatement.setInt(7, Integer.parseInt(join.level));
                    tableJoinInsertStatement.setInt(7, 0);
                    tableJoinInsertStatement.setInt(8, join.getSupportCount());
                    tableJoinInsertStatement.setFloat(9, join.getConfidence());

                    tableJoinInsertStatement.addBatch();

                    Iterator<Map.Entry<String, JoinTuple>> joinTupleIterator = join.joinTuples.entrySet().iterator();
                    while (joinTupleIterator.hasNext()) {
                        JoinTuple joinTuple = joinTupleIterator.next().getValue();

                        //Persist JionTuple Information in database
                        //wl_join_id, left_column, right_column
                        tableJoinTupleInsertStatement.setInt(1, joinId);
                        tableJoinTupleInsertStatement.setString(2, joinTuple.leftcolumn);
                        tableJoinTupleInsertStatement.setString(3, joinTuple.rightcolumn);

                        tableJoinTupleInsertStatement.addBatch();
                    }
                }

                Iterator<Map.Entry<String, Partition>> partitionIterator = currTable.partitions.entrySet().iterator();
                while (partitionIterator.hasNext()) {
                    Partition partition = partitionIterator.next().getValue();

                    // Get new partition id from sequence
                    resultSet = dbConnect.execQuery("SELECT nextval('" + haystackSchema + ".seq_wl_table_partition') as new_partition_id ");
                    resultSet.next();
                    int partitionId = resultSet.getInt("new_partition_id");

                    //persist partition information in database
                    //wl_table_id, wl_table_partition_id, table_name, partition_name, level, type, rank, "position", list_values,
                    // range_start, range_start_inclusive, range_end, range_end_inclusive, every_clause, isdefault, boundary, reltuples, relpages
                    tablePartitionInsertStatement.setInt(1, tableId);
                    tablePartitionInsertStatement.setInt(2, partitionId);
                    tablePartitionInsertStatement.setString(3, partition.tableName);
                    tablePartitionInsertStatement.setString(4, partition.partitionName);
                    tablePartitionInsertStatement.setInt(5, partition.level);
                    tablePartitionInsertStatement.setString(6, partition.type);
                    tablePartitionInsertStatement.setInt(7, partition.rank);
                    tablePartitionInsertStatement.setInt(8, partition.position);
                    tablePartitionInsertStatement.setString(9, partition.listValues);
                    tablePartitionInsertStatement.setString(10, partition.rangeStart);
                    tablePartitionInsertStatement.setBoolean(11, partition.rangeStartInclusive);
                    tablePartitionInsertStatement.setString(12, partition.rangeEnd);
                    tablePartitionInsertStatement.setBoolean(13, partition.rangeEndInclusive);
                    tablePartitionInsertStatement.setString(14, partition.everyClause);
                    tablePartitionInsertStatement.setBoolean(15, partition.isDefault);
                    tablePartitionInsertStatement.setString(16, partition.boundary);
                    tablePartitionInsertStatement.setInt(17, partition.relTuples);
                    tablePartitionInsertStatement.setInt(18, partition.relPages);

                    tablePartitionInsertStatement.addBatch();

                    Iterator<Map.Entry<String, Partition>> childPartitionIterator = partition.childPartitions.entrySet().iterator();
                    while (childPartitionIterator.hasNext()) {
                        Partition childPartition = childPartitionIterator.next().getValue();
                        //persist childPartition infromatino in database
                        childPartitionInsertStatement.setInt(1, tableId);
                        childPartitionInsertStatement.setInt(2, partitionId);
                        childPartitionInsertStatement.setString(3, partition.tableName);
                        childPartitionInsertStatement.setString(4, partition.partitionName);
                        childPartitionInsertStatement.setInt(5, partition.level);
                        childPartitionInsertStatement.setString(6, partition.type);
                        childPartitionInsertStatement.setInt(7, partition.rank);
                        childPartitionInsertStatement.setInt(8, partition.position);
                        childPartitionInsertStatement.setString(9, partition.listValues);
                        childPartitionInsertStatement.setString(10, partition.rangeStart);
                        childPartitionInsertStatement.setBoolean(11, partition.rangeStartInclusive);
                        childPartitionInsertStatement.setString(12, partition.rangeEnd);
                        childPartitionInsertStatement.setBoolean(13, partition.rangeEndInclusive);
                        childPartitionInsertStatement.setString(14, partition.everyClause);
                        childPartitionInsertStatement.setBoolean(15, partition.isDefault);
                        childPartitionInsertStatement.setString(16, partition.boundary);
                        childPartitionInsertStatement.setInt(17, partition.relTuples);
                        childPartitionInsertStatement.setInt(18, partition.relPages);

                        childPartitionInsertStatement.addBatch();
                    }
                }

                //Check for recommendations for this table, if any then persist it in database

                Iterator<Map.Entry<String, Recommendation>> recommandationsIterator = tablesList.recommendations.entrySet().iterator();

                while (recommandationsIterator.hasNext()) {
                    Recommendation recommendation = recommandationsIterator.next().getValue();
                    if (currTable.tableName.equals(recommendation.tableName) && currTable.schema.equals(recommendation.schema)) {
                        //persist recommandation in database
                        //wl_table_id, oid, schema, table_name, type, description, anamoly
                        tableRecommandationInsertStatement.setInt(1, tableId);
                        tableRecommandationInsertStatement.setString(2, recommendation.oid);
                        tableRecommandationInsertStatement.setString(3, recommendation.schema);
                        tableRecommandationInsertStatement.setString(4, recommendation.tableName);
                        tableRecommandationInsertStatement.setString(5, recommendation.type.toString());
                        tableRecommandationInsertStatement.setString(6, recommendation.description);
                        tableRecommandationInsertStatement.setString(7, recommendation.anamoly);

                        tableRecommandationInsertStatement.addBatch();
                    }
                }

                tableInsertStatement.addBatch();
            }

            tableInsertStatement.executeBatch();
            columnInsertStatement.executeBatch();
            tableDKInsertStatement.executeBatch();
            tablePartitionColumnInsertStatement.executeBatch();
            tableJoinInsertStatement.executeBatch();
            tableJoinTupleInsertStatement.executeBatch();
            tablePartitionInsertStatement.executeBatch();
            tableRecommandationInsertStatement.executeBatch();
        }catch(Exception ex){
            log.error("Exception in persistInDatabase: " +ex.toString());
        }
    }

    public String getWorkloadJSON(int workloadId){
        String workloadJSON = "";

        String sql = "SELECT workload_json FROM " +haystackSchema+".workloads_json WHERE workload_id = " + workloadId;
        try {
            ResultSet result = dbConnect.execQuery(sql);

            if(result.next()){
                workloadJSON = result.getString("workload_json");
                return workloadJSON;
            }
            result.close();

        } catch (SQLException e) {
            log.error("Error wile getting workload json for workload: " +workloadId + " Exception:" +e.toString());
        }

        return  null;
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

    public boolean processQueryLog(int queryLogId, int cluster_id, String queryLogDirectory) {

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

                loadQueries(extTableName, queryLogId, userName, cluster_id);
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

    private void loadQueries(String extTableName, Integer QueryId, String userId, Integer cluster_id) throws SQLException {

        String strRunId = String.format("%05d", QueryId);
        String queryLogTableName = userId + ".qry" + strRunId;
        String schemaName = userId;
        ResultSet rs = null;

        String sql = "SELECT " + haystackSchema + ".load_querylog('" + haystackSchema + "','" + schemaName + "','" + queryTblName + "','" + extTableName + "'," + QueryId + "," + cluster_id + ");";
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


    public String executeGPSD(int clusterId, String userName, String fileName) {
        String searchPath = this.haystackSchema;
        int lineNo = 0;
        boolean hadErrors = false;
        String sqlToExec;
        try {
            // NOTE: What's this for ?
            Integer batchSize = Integer.parseInt(configProperties.properties.getProperty("gpsd.batchsize"));
            StringBuilder sbBatchQueries = new StringBuilder();

            ArrayList<String> fileQueries = new ArrayList<String>();

            String gpsdDBName = userName.toLowerCase() + String.format("%04d", clusterId);
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
                        "CLUSTERID=" + clusterId + ", FileName=" + fileName, userName);

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
            String cluster_DB = "", cluster_date = "", cluster_params = "", cluster_version = "";


            while ((line = reader.readLine()) != null) {
                lineNo++;
                if (line.trim().length() == 0) {
                    continue;
                }
                // If line has comments ignore the comments line, extract any characters before the comments line
                int x = line.indexOf("--");
                if (lineNo == 20) {
                    if (cluster_date.length() == 0 && cluster_version.length() == 0) {
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
                            cluster_DB = line.substring(13);
                        }
                        i = line.indexOf("-- Date:");
                        if (i >= 0) {
                            cluster_date = line.substring(8);
                        }
                        i = line.indexOf("-- CmdLine:");
                        if (i >= 0) {
                            cluster_params = line.substring(11);
                        }
                        i = line.indexOf("-- Version:");
                        if (i >= 0) {
                            cluster_version = line.substring(11);
                            // Update  GPSD  with the Header Information for the new Database
                            /*dbConnect.execNoResultSet(String.format("update haystack.gpsd set gpsd_db = '" + gpsd_DB + "', gpsd_date = '" + gpsd_date + "' , gpsd_params='"
                                    + gpsd_params + "', gpsd_version = '" + gpsd_version + "', filename ='" + fileName + "' where dbname ='" + gpsdDBName + "';");*/
                            sqlToExec = String.format("UPDATE %s.cluster SET dbname='%s', cluster_date='%s', cluster_params='%s', cluster_version='%s' WHERE cluster_id = %d;",
                                    searchPath, cluster_DB, cluster_date, cluster_params, cluster_version, clusterId);
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
                                                "GPSDId=" + clusterId + ", FileName=" + fileName + " ,SQL=" + sbBatchQueries.toString(), userName);
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
                                    HSException hsException = new HSException("CatalogService.executeGPSD()", "Error in executing Cluster Query", e.toString(),
                                            "ClusterId=" + clusterId + ", FileName=" + fileName + " ,SQL=" + currQuery, userName);
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
            Tables tablelist = clusterService.getTables(clusterId);
            String json_res = tablelist.getJSON();
            dbConnGPSD.close();
            //======================================================
            // Update  CLUSTER  with the Header Information for the new Database
            //dbConnect.execNoResultSet("update  haystack.cluster set noOflines = " + lineNo + " where dbname ='" + gpsdDBName + "';");
            sqlToExec = String.format("UPDATE %s.cluster SET nooflines=%d, cluster_db='%s' WHERE cluster_id=%d;", haystackSchema, lineNo, gpsdDBName, clusterId);
            dbConnect.execNoResultSet(sqlToExec);

            sqlToExec = "select user_id from " + haystackSchema + ".users where user_name = '" + userName + "';";
            ResultSet rsUser = dbConnect.execQuery(sqlToExec);
            rsUser.next();

            //TODO  Check if cluster entry exists for the same dbname,
            sqlToExec = "INSERT INTO " + haystackSchema + ".cluster(  cluster_id ,cluster_name ,host ,dbname ,port ,password ,username ,\n" +
                    "  query_refresh_schedule ,created_on ,cluster_type ,user_id ) VALUES (999" + clusterId + ",'GPSD-" + clusterId + "-" + gpsdDBName +
                    "','" + gpsdCred.getHostName() + "','" + gpsdDBName + "'," + gpsdCred.getPort() + ",'" + gpsdCred.getPassword() +
                    "','" + gpsdCred.getUserName() + "','24hour',now(),'GREENPLUM'," + rsUser.getInt("user_id");

            dbConnect.execNoResultSet(sqlToExec);
            dbConnect.close();
            return json_res;

        } catch (Exception e) {
            hadErrors = true;
            log.error(e.toString());
            HSException hsException = new HSException("CatalogService.executeGPSD()", "Error in parsing GPSD File or updating GPSD table", e.toString(),
                    "CLUSTERId=" + clusterId + ", FileName=" + fileName, userName);
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
