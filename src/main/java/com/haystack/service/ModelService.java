package com.haystack.service;


import com.google.gson.*;
import com.haystack.domain.*;


//import com.haystack.parser.parser.TokenMgrError;
//import com.haystack.parser.statement.update.Update;
//import com.haystack.parser.util.parserDOM;
//import com.haystack.parser.parser.CCJSqlParserUtil;
//import com.haystack.parser.statement.Statement;
//import com.haystack.parser.statement.select.Select;
//import com.haystack.parser.util.parserDOM;

import com.haystack.domain.Query;
import com.haystack.visitor.*;
import net.sf.jsqlparser.parser.TokenMgrError;
import net.sf.jsqlparser.statement.update.Update;
import com.haystack.parser.util.parserDOM;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import com.haystack.parser.util.parserDOM;

import com.haystack.parser.util.ASTGenerator;

import com.haystack.util.ConfigProperties;
import com.haystack.util.Credentials;
import com.haystack.util.DBConnectService;
import com.haystack.util.HSException;

//import net.sf.jsqlparser.parser.CCJSqlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



import java.io.IOException;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by qadrim on 15-03-04.
 *
 */
public class ModelService {
    private ConfigProperties configProperties;

    static Logger log = LoggerFactory.getLogger(ModelService.class.getName());
    static Tables tablelist;
    private Date wl_start_date;
    private Date wl_end_date;
    private Date model_creation_date;
    private Integer userId = null;

    private  DBConnectService dbConnectService;

    public ModelService(){
        tablelist = new Tables();
        configProperties = new ConfigProperties();
        dbConnectService = new DBConnectService(DBConnectService.DBTYPE.POSTGRES);

        try {
            configProperties.loadProperties();
        } catch (Exception e) {
            //throw new Exception("Unable to read config.properties files");
        }
    }

    public static void main(String args[]) {

    }
    public void setTableList(Tables tbllist){
        this.tablelist = tbllist;
    }

    private int getDistinctNoOfRows(String schema, String tableName, String columnName, Credentials credentials) throws SQLException, IOException, ClassNotFoundException {

        dbConnectService.setCredentials(credentials);

        int totalNoDistinctValues = 0;

        String sql = "SELECT count (DISTINCT " +tableName +"." +columnName +") as no_of_distinct_value FROM " +schema +"." +tableName;

        dbConnectService.connect(credentials);

        ResultSet result = dbConnectService.execQuery(sql);

        result.next();
        totalNoDistinctValues = result.getInt("no_of_distinct_value");

        return totalNoDistinctValues;
    }

    private Credentials getCredentials(int gpsd_id) throws IOException, SQLException, ClassNotFoundException {
        ConfigProperties configProperties = new ConfigProperties();
        configProperties.loadProperties();

        String haystackSchema = configProperties.properties.getProperty("main.schema");

        Credentials gpsd_credentials  = configProperties.getGPSDCredentials();

        String sql = "select gpsd_id, gpsd_db, host , dbname ,password , username ,port, coalesce(last_queries_refreshed_on, '1900-01-01') as last_queries_refreshed_on, " +
                " coalesce(last_schema_refreshed_on,'1900-01-01') as last_schema_refreshed_on ,db_type as cluster_type, now() as current_time  from " + haystackSchema +
                ".gpsd where host is not null and is_active = true and gpsd_id = " + gpsd_id;

        Credentials clusterCred = new Credentials();

        dbConnectService.connect(configProperties.getHaystackDBCredentials());
        ResultSet rs = dbConnectService.execQuery(sql);

        while (rs.next()) {

            Integer clusterId = rs.getInt("gpsd_id");

            String hostName = rs.getString("host");
            Boolean isGPSD = false;
            if (hostName == null || hostName.length() == 0) { // load from stats
                isGPSD = true;
            }

            if (isGPSD) { // load from stats
                clusterCred = gpsd_credentials;
                clusterCred.setDatabase(rs.getString("gpsd_db"));
            } else {

                String sHost = rs.getString("host");
                String dbname = rs.getString("dbname");
                String username = rs.getString("username");
                String password = rs.getString("password");
                Integer port = rs.getInt("port");

                clusterCred.setCredentials(sHost, ""+port, dbname, username, password);
            }
        }

        return clusterCred;
    }

    private Recommendation createNewRecommendation(Table currTable, Recommendation.RecommendationType type) {
        Recommendation recommendation = new Recommendation();
        recommendation.schema = currTable.schema;
        recommendation.tableName = currTable.tableName;
        recommendation.oid = currTable.oid;
        recommendation.type = type;
        return recommendation;
    }
    public void generateRecommendations(int gpsd_id){

        try {

            Credentials credentials = getCredentials(gpsd_id);

            // Fetch Recommendation Engine settings from config.properties file
            Double columnarThresholdPercent = Double.valueOf(configProperties.properties.getProperty("re.columnarThresholdPercent"));
            Double topNPercent = Double.valueOf(configProperties.properties.getProperty("re.topNPercent"));
            Double bottomNPercent = Double.valueOf(configProperties.properties.getProperty("re.bottomNPercent"));

            Integer recId = 1;

            Iterator<Map.Entry<String, Table>> entries = tablelist.tableHashMap.entrySet().iterator();
            while (entries.hasNext()) {
                Map.Entry<String, Table> entry = entries.next();
                String currKey = entry.getKey();
                Table currTable = entry.getValue();

                if (currTable.joins.size() > 0) { // If there are no joins then ignore this Table
                    // A) Distribution Key:
                    //     Check if the current distibution key is being used in most of the join,
                    //     if not then recommend DK which is used in joins (higher workload score) for larger tables
                    Iterator<Map.Entry<String, Join>> joins = currTable.joins.entrySet().iterator();
                    float maxConfidence = -1;
//                    ArrayList<String> maxConfidenceCandidateDK = new ArrayList<String>();
                    HashMap<String, Float> maxConfidenceCandidateDK = new HashMap<String, Float>(); //Key = Column Name, Value = Confidence of Column

                    while (joins.hasNext()) {

                        Map.Entry<String, Join> join = joins.next();
                        String joinKey = join.getKey();
                        Join currJoin = join.getValue();

                        if (currJoin.getConfidence() > maxConfidence) {  // Current Join Usage Score is greater makes this the max one
                            for (Map.Entry<String, JoinTuple> entryJT : currJoin.joinTuples.entrySet()) {
                                if (currTable.tableName.equals(currJoin.leftTable)) {  // Add left column
//                                    maxConfidenceCandidateDK.add(entryJT.getValue().leftcolumn);
                                    maxConfidenceCandidateDK.put(entryJT.getValue().leftcolumn, currJoin.getConfidence());
                                } else { // Add Right Column
//                                    maxConfidenceCandidateDK.add(entryJT.getValue().rightcolumn);
                                    maxConfidenceCandidateDK.put(entryJT.getValue().rightcolumn, currJoin.getConfidence());
                                }
                            }
                            maxConfidence = currJoin.getConfidence();
                        }
                        // A.2) Check if attribute data types match for this join, if not add this recommendation
                        for (Map.Entry<String, JoinTuple> entryJT : currJoin.joinTuples.entrySet()) {
                            JoinTuple currJT = entryJT.getValue();
                            Column leftColumn = tablelist.findColumn(currJoin.leftSchema, currJoin.leftTable, currJT.leftcolumn, "");
                            Column rightColumn = tablelist.findColumn(currJoin.rightSchema, currJoin.rightTable, currJT.rightcolumn, "");

                            Boolean mismatch = false;
                            String desc = "Datatype mismatch for join between " + currJoin.leftSchema + "." + currJoin.leftTable + "." + currJT.leftcolumn +
                                    " and " + currJoin.rightSchema + "." + currJoin.rightTable + "." + currJT.rightcolumn;
                            String anamoly = leftColumn.isMatchDataType(rightColumn);

                            if (anamoly.length() > 0) {
                                Recommendation recommendation = createNewRecommendation(currTable, Recommendation.RecommendationType.DATATYPE);
                                recommendation.description = desc;
                                recommendation.anamoly = anamoly;
                                tablelist.recommendations.put(recId.toString(), recommendation);
                                recId++;
                            }
                        }
                    }
                    // A.1) Get Distribution Key, if it matches the candidateDK then ignore else add Recommendation with the CandidateDK

                    //By Ghaffar: 21/6/2016
                    //I have commented-out the old code because it have some bugs.
                    //TODO: remove after conformation from mujtaba

                    /*String recKey = "";
                    for (String currCol : maxConfidenceCandidateDK) {
                        if (recKey.length() == 0) {
                            recKey = currCol;
                        } else {
                            recKey += "," + currCol;
                        }
                    }*/

                    /*for (String candidateColumn : maxConfidenceCandidateDK) {
                        if (currTable.dk.containsKey(candidateColumn) == false) {
                            // DK and Candidate Key Mismatch, add recommendation for Candidate Key
                            Recommendation recommendation = createNewRecommendation(currTable, Recommendation.RecommendationType.DK);
                            recommendation.description = "Distribution key should be set to " + recKey;
                            recommendation.anamoly = "Confidence for new key:" + maxConfidence;
                            tablelist.recommendations.put(recId.toString(), recommendation);
                            recId++;
                        }
                    }*/

                    Iterator<Map.Entry<String, Float>> maxConfidenceCandidateDKIterator = maxConfidenceCandidateDK.entrySet().iterator();

                    while(maxConfidenceCandidateDKIterator.hasNext()){
                        Map.Entry<String, Float> columnAndConfidenceValue = maxConfidenceCandidateDKIterator.next();
                        String candidateColumn = columnAndConfidenceValue.getKey();
                        Float confidenceOfColumn = columnAndConfidenceValue.getValue();

                        if (currTable.dk.containsKey(candidateColumn) == false) {
                            // DK and Candidate Key Mismatch, add recommendation for Candidate Key
                            Recommendation recommendation = createNewRecommendation(currTable, Recommendation.RecommendationType.DK);
                            recommendation.description = "Distribution key should be set to " + candidateColumn;
                            recommendation.anamoly = "Confidence for new key:" + confidenceOfColumn;
                            tablelist.recommendations.put(recId.toString(), recommendation);
                            recId++;
                        }
                    }
                }
                // Objective Measure of Interestingness, Recommendation should be generated for only TopNPercent of tables
                // B) Columnar & Compression Rule:
                //     Check to see if less than 30% (rs.ColumnarThresholdPercent) of attributes are used,
                //     if yes then check if the table is in TopNPercent in rows (re.topNPercent) threshold
                //     if yes then
                //             recommend columnar, if row storage
                //             recommend compression, if uncompressed
                Double avgColUsagePercent = currTable.stats.avgColUsage * 100 / currTable.stats.noOfColumns;

                if (avgColUsagePercent <= columnarThresholdPercent) {  // ColAvgUsage is less than ColumnThreshold
                    if ((100 - currTable.stats.percentile) < topNPercent) { // If Table in TopNPercent then
                        if (!currTable.stats.isColumnar) { // If Table is heap then recommend columnar
                            Recommendation recommendation = createNewRecommendation(currTable, Recommendation.RecommendationType.STORAGE);
                            recommendation.description = "Table " + currTable.schema + "." + currTable.tableName + " should be changed to columnar.";
                            recommendation.anamoly = "Table Percentile:" + currTable.stats.percentile + " and average column usage in queries:" + avgColUsagePercent + " Threshold: AvgColUsage="
                                    + columnarThresholdPercent + " topNPercent=" + topNPercent;
                            tablelist.recommendations.put(recId.toString(), recommendation);
                            recId++;
                        }
                        if (!currTable.stats.isCompressed) { // Recommend compressing the table if Cluster CPU Usage is less than 70% most of the time
                            Recommendation recommendation = createNewRecommendation(currTable, Recommendation.RecommendationType.COMPRESSION);
                            recommendation.description = "Table " + currTable.schema + "." + currTable.tableName + " should be compressed if cluster CPU is less than 70% threshold";
                            recommendation.anamoly = "Table Percentile:" + currTable.stats.percentile + " and average column usage in queries:" + avgColUsagePercent + " Threshold: AvgColUsage="
                                    + columnarThresholdPercent + " topNPercent=" + topNPercent;
                            tablelist.recommendations.put(recId.toString(), recommendation);
                            recId++;
                        }
                    }

                } else {
                    // B.2) If average column usage is greater than the threshold  (rs.ColumnarThresholdPercent) then
                    //      check if the storage type is columnar, then recommend heap storage
                    if (currTable.stats.isColumnar) {
                        Recommendation recommendation = createNewRecommendation(currTable, Recommendation.RecommendationType.STORAGE);
                        recommendation.description = "Table " + currTable.schema + "." + currTable.tableName + " should be changed to heap storage.";
                        recommendation.anamoly = "Table Percentile:" + currTable.stats.percentile + " and average column usage in queries:" + avgColUsagePercent + " Threshold: AvgColUsage="
                                + columnarThresholdPercent + " topNPercent=" + topNPercent;
                        tablelist.recommendations.put(recId.toString(), recommendation);
                        recId++;
                    }
                }

                // B.3) If the table is in bottomNPercent and if its columnar, then recommend heap
                //      storage and uncompressed
                if (currTable.stats.percentile <= bottomNPercent) {
                    if (currTable.stats.isColumnar) {
                        Recommendation recommendation = createNewRecommendation(currTable, Recommendation.RecommendationType.STORAGE);
                        recommendation.description = "Table " + currTable.schema + "." + currTable.tableName + " should be changed to heap storage.";
                        recommendation.anamoly = "Table Percentile:" + currTable.stats.percentile + " is in the BottomN% smallest tables in the Database. Threshold: bottomNPercent=" + bottomNPercent;
                        tablelist.recommendations.put(recId.toString(), recommendation);
                        recId++;
                    }
                }

                //Step1: Find That Table Is In TopNPercent Using Perito Principle

                HashMap<String, Column> columnsForSuggestion = new HashMap<String, Column>();

                if((100-topNPercent) <= currTable.stats.percentile){

                    //Step2: Get Those Columns Which Are Used In Where Clause And Add Them In 'columnsForSuggestion' List.

                    Iterator<Column> columnsIterator = currTable.columns.values().iterator();

                    while(columnsIterator.hasNext()){
                        Column column = columnsIterator.next();
                        int whereUsage = column.getWhereUsage();

                        if(whereUsage > 0){
                            columnsForSuggestion.put( column.column_name ,column);
                        }
                    }
                }

                //Step3: Find The Column With Max Cardenality For Recommendation.

                Column recommendedColumn = null;
                double maxCardenality = 0;
                int recommendedColumnDistinctNoOfRows = 0;

                Collection<Column> columns = columnsForSuggestion.values();

                for(Column column : columns){
                    int distinctNoOfRows = getDistinctNoOfRows(currTable.schema, currTable.tableName, column.column_name, credentials);

                    double cardenality = currTable.stats.noOfRows / distinctNoOfRows; //Finding Cardenality using formula : total no of rows in table / total no of distinct rows in column

                    if(cardenality > maxCardenality){
                        maxCardenality = cardenality;
                        recommendedColumn = column;
                        recommendedColumnDistinctNoOfRows = distinctNoOfRows;
                    }
                }

                // C) Partitions:
                //     Check to see if the table is partititioned
                if (currTable.stats.isPartitioned) {
                    //    if YES
                    //         then check if the partitioned attribute is used in most of the where clauses
                    //         give bias to date attribute, recommend two or three possible options for partition columns

                    //Recommandtaion type= Partition
                    //Desc: why we select the column, where uses = , cardinality = , distinct no of value = , rank # .

                    if(recommendedColumn != null) {

                        //If Table Is Partitioned On Any Column
                        if(!currTable.partitionColumn.isEmpty()) {
                            if(!currTable.partitionColumn.containsKey(recommendedColumn.column_name)){
                                Recommendation recommendation = createNewRecommendation(currTable, Recommendation.RecommendationType.PARTITION);
                                recommendation.description = String.format("Why we selected the column %s.%s, Where usage : %d, Cardinality : %.2f, Distinct no of values : %d", recommendedColumn.getResolvedTableName(), recommendedColumn.column_name, recommendedColumn.getWhereUsage(), maxCardenality, recommendedColumnDistinctNoOfRows);
                                tablelist.recommendations.put(recId.toString(), recommendation);
                                recId++;
                            }
                        }
                    }

                } else {
                    //     if NO
                    //         then check if table is in re.TopNPercent tables by rows
                    //         if YES
                    //             then identify the attribute which is used most frequently in where clauses

                    //Check if table is in topNPercent tables by row
                    if(recommendedColumn != null) {
                        if((100-topNPercent) <= currTable.stats.percentile) {
                            Recommendation recommendation = createNewRecommendation(currTable, Recommendation.RecommendationType.PARTITION);
                            recommendation.description = String.format("Why we selected the column %s.%s, Where usage : %d, Cardinality : %.2f, Distinct no of values : %d", recommendedColumn.getResolvedTableName(), recommendedColumn.column_name, recommendedColumn.getWhereUsage(), maxCardenality, recommendedColumnDistinctNoOfRows);
                            tablelist.recommendations.put(recId.toString(), recommendation);
                            recId++;
                        }
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void scoreModel(){
        try {
            float totalWorkloadScore = 0;
            Double totalSizeofTables = 0.0;
            // Calculate total workload score
            Iterator<Map.Entry<String, Table>> entriesForTotal = tablelist.tableHashMap.entrySet().iterator();
            while(entriesForTotal.hasNext()) {
                Map.Entry<String, Table> entry = entriesForTotal.next();
                String currKey = entry.getKey();
                Table currTable = entry.getValue();
                totalSizeofTables += Double.valueOf(currTable.stats.sizeUnCompressed);
                float currentWorkload = currTable.stats.getWorkloadScore();
                totalWorkloadScore += currentWorkload;
            }


            // Based on the Total Workload Score, Rationalize on a scale of 0-100
            Iterator<Map.Entry<String, Table>> entries = tablelist.tableHashMap.entrySet().iterator();
            while(entries.hasNext()) {
                Map.Entry<String, Table> entry = entries.next();
                String currKey = entry.getKey();
                Table currTable = entry.getValue();
                float currentWorkload = currTable.stats.getWorkloadScore();

                float workloadScore = 0;

                if (currentWorkload > 0) {
                    workloadScore = currentWorkload / totalWorkloadScore;
                }
                currTable.stats.setModelScore(workloadScore);

                //TODO: Ghaffar: Is Average Calculation right?
                // Calculate Average Column Usage
                try {
                    // Calculate confidence for all joins, using Confidence = Support x (LeftTableWeight + RightTableWeight)
                    Double totalColumnUsage = 0.0;
                    Integer columnCount = currTable.columns.size();
                    Double avgColUsage = 0.0;
                    for (Map.Entry<String, Column> entryColumn : currTable.columns.entrySet()) {
                        Column currColumn = entryColumn.getValue();
                        totalColumnUsage += currColumn.getUsageScore();
                        columnCount++;
                    }
                    avgColUsage = totalColumnUsage / columnCount;
                    currTable.stats.avgColUsage = avgColUsage;
                } catch (Exception e) {
                    log.error("Error in calculating join confidence for Table:" + currTable.schema + "." + currTable.tableName);
                    currTable.stats.avgColUsage = 0.0;
                }

                // Calculate confidence for all joins, using Confidence = Support x (LeftTableWeight + RightTableWeight)
                try {
                    for (Map.Entry<String, Join> entryJoin : currTable.joins.entrySet()) {
                        Join currJoin = entryJoin.getValue();
                        Table leftTable = tablelist.findTable(currJoin.leftSchema, currJoin.leftTable);
                        Table rightTable = tablelist.findTable(currJoin.rightSchema, currJoin.rightTable);

                        Double leftTableWeight = 0.0, rightTableWeight = 0.0;
                        if (leftTable.stats.isColumnar == true) { // If table is columnar then
                            leftTableWeight = leftTable.stats.sizeUnCompressed * leftTable.stats.avgColUsage;
                        } else {
                            leftTableWeight = (double) leftTable.stats.sizeUnCompressed;
                        }
                        if (rightTable.stats.isColumnar == true) { // If table is columnar then
                            rightTableWeight = rightTable.stats.sizeUnCompressed * rightTable.stats.avgColUsage;
                        } else {
                            rightTableWeight = (double) rightTable.stats.sizeUnCompressed;
                        }
                        float confidence = (float) (currJoin.getSupportCount() * (leftTableWeight + rightTableWeight));
                        currJoin.setConfidence(confidence);
                    }
                } catch (Exception e) {
                    log.error("Error in calculating join confidence for Table:" + currTable.schema + "." + currTable.tableName);
                }
                tablelist.tableHashMap.put(currKey, currTable);

            }
            // Calculate Percentile and Rank for tables in the Hashmap
            List<Table> tablesSortedBySize = new ArrayList<Table>(tablelist.tableHashMap.values());

            Collections.sort(tablesSortedBySize, new Comparator<Table>() {
                @Override
                public int compare(Table o1, Table o2) {
                    Double o1_weight = Double.valueOf(o1.stats.sizeUnCompressed);
                    Double o2_weight = Double.valueOf(o2.stats.sizeUnCompressed);

                    Integer ret = Double.compare(o1_weight, o2_weight);

                    return ret;
                }
            });

            int rank = 0;
            Double cumPercentage = 0.0;
            for (Table table : tablesSortedBySize) {
                rank++;
                Double tableSizePercentage = table.stats.sizeUnCompressed * 100 / totalSizeofTables;
                cumPercentage += tableSizePercentage;
                table.stats.rank = rank;
                table.stats.percentile = cumPercentage;
            }

        } catch (Exception e) {
            log.error(e.toString());
        }
    }
    public String getModelJSON(){
        return tablelist.getJSON();
    }

    public void processSQL(Integer queryId, Query query, double executionTime, Integer userId, String current_search_path) throws Exception {
        processSQL(queryId, query.getQueryText(), executionTime, userId, current_search_path);
    }

    public void processSQL(Integer queryId, String query, double executionTime, Integer userId, String current_search_path) throws Exception {
        parserDOM currtablesNF = new parserDOM();

        String jsonAST = "";
        try {
            // TODO: Parse Statement and annotate the input Query object with details
            // 1.   Note: Some queries will be executed with the search_path set, so table name might not have schema and will
            //      have to manually resolve this (if there are collisions then errors have to be logged

            this.userId = userId;
            log.info("Starting ModelService.processSQL");
            log.debug("Processing SQL:\n" + query);

            String sqls = query;
            Statement statement = null;
            try {
                statement = CCJSqlParserUtil.parse(sqls);

                //statement = (Statement)net.sf.jsqlparser.parser.CCJSqlParserUtil.parse(sqls);


            } catch (TokenMgrError e) {
                // Statement is not a select/update or supported by Parser, store as is;
                jsonAST = sqls;
                log.error("ModelService.processSQL() : Error in parsing SQL=" + query.toString());
                return;
            }
            Select selectStatement = null;
            Update updateStatement = null;
            String stmtType = "";

            if (statement == null) {
                throw new Exception("Parsing Error");
            }
            try {
                selectStatement = (Select) statement;
                stmtType = "SELECT";


            } catch (Exception e) {
                log.debug("Not a Select Statement :" + e.toString());
            }

            try {
                updateStatement = (Update) statement;
                stmtType = "UPDATE";
            } catch (Exception e) {
                log.debug("Not an Update Statement :" + e.toString());
            }

            List<String> strTableList;

            if (stmtType == "SELECT") {
                currtablesNF.getSemantics(selectStatement);
            } else if (stmtType == "UPDATE") {
                currtablesNF.getSemantics(updateStatement);
            } else {
                log.error("Statement Not Supported :" + statement.toString());
                //HSException hsException = new HSException("ModelService.processSQL()", "Statement Not Supported", null, "SQL=" + query, userId);
                throw new Exception("Statement Not Supported :" + statement.toString());
            }

            resolve(currtablesNF.queryLevelObj, executionTime, current_search_path);

            log.debug("=============== CONDITIONS EXTRACTED =================");

            log.info("ModelService.processSQL Complete");
            return;
        }
        catch(Exception e){
            // Log Parsing Error
            log.error("SQL:" + query);
            log.error("PARSING ERROR:" + e.toString());
            throw e;
        }
    }

    public boolean resolve(com.haystack.visitor.Query queryLevelObj, double executionTime, String current_search_path){

        //Resolving tables
        for (int i = 0; i < queryLevelObj.tables.size(); i++) {
            String schemaName = "";
            if (queryLevelObj.tables.get(i).schema == null) {
                schemaName = tablelist.findSchema(queryLevelObj.tables.get(i).tablename, current_search_path);
                queryLevelObj.tables.get(i).schema = schemaName;
            }
            Table tbl = tablelist.findTable(queryLevelObj.tables.get(i).schema, queryLevelObj.tables.get(i).tablename);
            if (tbl != null){ // Increment table Usage, if its not a derived table
                tbl.stats.incrementUsageFrequency();
            }
            log.info("Table Extracted:" + schemaName + "." + queryLevelObj.tables.get(i).tablename) ;
        }

        //Resolving Columns
        for (int i = 0; i < queryLevelObj.columns.size(); i++) {
            //TODO: Muji
            Attribute currCol = queryLevelObj.columns.get(i);
            if (currCol.name.equals("*")) {
                // Check if all columns for a particular table selected or all for all tables
                for (int j = 0; j < queryLevelObj.tables.size(); j++) {
                    QryTable currTable = queryLevelObj.tables.get(i);
                    if ((currCol.tableName == null) || (currCol.tableName.equals(currTable.tablename))) {
                        // Add all Columns for this table;
                        Table tbl = tablelist.findTable(currTable.schema, currTable.tablename);
                        Iterator tblColIterator = tbl.columns.entrySet().iterator();

                        for (Map.Entry<String, Column> entry : tbl.columns.entrySet()) {
                            String key = entry.getKey();
                            Column tableColumn = entry.getValue();
                            // Create new temp Attribute Object
                            Attribute tmpAttr = new Attribute();
                            tmpAttr.name = tableColumn.column_name;
                            tmpAttr.tableName = tableColumn.getResolvedTableName();
                            tmpAttr.schema = tableColumn.getResolvedSchemaName();
                            tmpAttr.nameFQN = tableColumn.toString();
                            processProjectedColumn(tmpAttr, queryLevelObj.tables, current_search_path);
                        }

                    }
                }

            } else {
                processProjectedColumn(queryLevelObj.columns.get(i), queryLevelObj.tables, current_search_path);
            }
        }

        //Resolving Conditions
        processConditions(queryLevelObj, current_search_path);

        //TODO: Ek weekend tayl karna hay
        //Date: 18/6/2016
        divideTimeAmongstTables(queryLevelObj, executionTime);

        Iterator<com.haystack.visitor.Query> subQueriesIterator = queryLevelObj.subQueries.iterator();

        while(subQueriesIterator.hasNext()){
            com.haystack.visitor.Query currentSubQuery = subQueriesIterator.next();
            resolve(currentSubQuery, executionTime, current_search_path);
        }

        return false;
    }

    private void divideTimeAmongstTables(com.haystack.visitor.Query queryLevelObj, double executionTime) {

        // If Table is columnar then goto A else goto B
        // A) Calculate the number of columns used for each table in the query = NoOfColsUsed
        // Factor = NoOfColsUsed / TotalColumnsInTable
        // Multiple this with the TableSizeUncompressed = to get the table workload , skip to C0
        // B) Take TableSizeUncompressed = table workload
        // C) Do this for all tables in the query

        // D) Calculate Total by adding up all workload scores, and put a % value for each table
        // E) Divide executionTime against each table % value and assign that time slice to the table
        try {
            float totalWorkloadScore = 0;
            ArrayList<QryTable> uniqueTblArray = new ArrayList<QryTable>(); // Array of Table to keep Unique Tables done
            // This to ensure that we don't recalculate workload for tables more than one time.

            for (int i = 0; i < queryLevelObj.tables.size(); i++) {
                Boolean isTableProcessed = false;
                QryTable qryTbl = queryLevelObj.tables.get(i);

                // Check if table has already been calculated once
                for (int k = 0; k < uniqueTblArray.size(); k++) {
                    if (uniqueTblArray.get(k).tablename.equals(qryTbl.tablename)) {
                        if (uniqueTblArray.get(k).schema.equals(qryTbl.schema)) {
                            // Workload already calculated for this table skip it
                            isTableProcessed = true;
                            break;
                        }
                    }
                }
                if (isTableProcessed == true) {
                    continue;
                }
                Table currTbl = tablelist.findTable(qryTbl.schema, qryTbl.tablename);

                if (currTbl != null) {  // Table is a derived table
                    if (currTbl.stats.isColumnar) {
                        Integer tableColUsage = 0;

                        for (int j = 0; j < queryLevelObj.columns.size(); j++) {
                            Attribute attribute = queryLevelObj.columns.get(j);
                            if (attribute.schema == null || attribute.tableName == null) {
                                continue; // derived column, move to next one
                            }
                            if ((attribute.schema.equals(qryTbl.schema)) && (attribute.tableName.equals(qryTbl.tablename))) {
                                tableColUsage++;
                            }
                        }
                        if (tableColUsage <= 0) {
                            // There is something wrong with the code, debug
                            throw new Exception();
                        } else {
                            qryTbl.workloadScore = ((currTbl.stats.sizeUnCompressed * tableColUsage) / currTbl.stats.noOfColumns);
                            qryTbl.colUsage = tableColUsage;
                            uniqueTblArray.add(qryTbl);
                        }
                    } // Table is row based
                    else {
                        qryTbl.workloadScore = currTbl.stats.sizeUnCompressed;
                    }
                    totalWorkloadScore += qryTbl.workloadScore;
                }
            }


            // Step E)

            for (int i = 0; i < uniqueTblArray.size(); i++) {
                QryTable qryTbl = uniqueTblArray.get(i);
                qryTbl.workloadPercentage = (qryTbl.workloadScore * 100) / totalWorkloadScore;

                Table currTbl = tablelist.findTable(qryTbl.schema, qryTbl.tablename);
                Double timeSlice = (executionTime *  qryTbl.workloadPercentage) /100;
                currTbl.stats.addExecutionTime(timeSlice);
                currTbl.stats.addWorkloadScore(qryTbl.workloadScore);
            }
        } catch (Exception e) {
            log.error("Error divideTimeAmongstTable: " + e.toString());
            HSException hsException = new HSException("ModelService.divideTimeAmongstTables()", "Exception in dividing time among the tables ",
                    e.toString(), "No Context Info", userId);

        }
    }

    private void processConditions(com.haystack.visitor.Query queryLevelObj, String current_search_path) {
        // === Extract Conditions
        // === If where clause then increment UsageScore for the column
        // === If join condition then connect the two tables together and increment join usage for left and right column
        HashMap<String, Join> localJoinHashmap = new HashMap<String, Join>();
        for (Condition condition : queryLevelObj.conditions) {
            try {
                // Condition can be where clause or a join condition, separate them and load them into local cache
                if (condition.isJoin) { // Join Condition

                    Attribute leftAttr = new Attribute();  // Resolve Aliases or Empty Table Names in the Join Conditions
                    leftAttr.tableName = condition.leftTable;
                    leftAttr.name = condition.leftColumn;
//                    leftAttr.level = condition.level;
                    Column leftColumn = resolveColumnForJoin(leftAttr, queryLevelObj, current_search_path);

                    Attribute rightAttr = new Attribute();
                    rightAttr.tableName = condition.rightTable;
                    rightAttr.name = condition.rightColumn;
//                    rightAttr.level = condition.level;
                    Column rightColumn = resolveColumnForJoin(rightAttr, queryLevelObj, current_search_path);

                    if (leftColumn == null || rightColumn == null ){  // If derived columns are involved skip the join condition
                        continue;
                    }
                    // Set the resolved column to condition in currTablesNF
                    condition.leftTable = leftAttr.tableName;
                    condition.rightTable = rightAttr.tableName;
                    // Sanitize Left and Right TableNames for ßKey Gen
                    String keyLeftTbl = null;
                    String keyRightTbl = null;

                    if (condition.leftTable.length()<=0){
                        keyLeftTbl = leftAttr.tableName;
                    } else {
                        keyLeftTbl = condition.leftTable;
                    }

                    if(condition.rightTable.length() <= 0){
                        keyRightTbl = rightAttr.tableName;
                    } else {
                        keyRightTbl = condition.rightTable;
                    }

                    String key = keyLeftTbl + "~" + keyRightTbl + "~" + queryLevelObj.getLevel();
                    // Check if the table pair already has a join condition in the cache
                    Join join = localJoinHashmap.get(key);
                    if (join == null ) { // try reversing left and right table
                        key = keyRightTbl + "~" + keyLeftTbl + "~" + queryLevelObj.getLevel();
                        join = localJoinHashmap.get(key);
                    }

                    if (join == null) {
                        // New Join Row set Tables and add Columns
                        join = new Join();
                        join.leftSchema = leftAttr.schema;
                        join.leftTable = leftAttr.tableName;
                        join.rightSchema = rightAttr.schema;
                        join.rightTable = rightAttr.tableName;
                        join.level = queryLevelObj.getLevel();
                        JoinTuple joinTuple = new JoinTuple();
                        joinTuple.leftcolumn = leftColumn.column_name;
                        joinTuple.rightcolumn = rightColumn.column_name;
                        String jtKey = joinTuple.leftcolumn + "~" + joinTuple.rightcolumn;
                        join.joinTuples.put(jtKey, joinTuple);
                        localJoinHashmap.put(key,join); // Put the new Join Object in the local cache
                    } else {

                        JoinTuple joinTuple = new JoinTuple();
                        joinTuple.leftcolumn = leftColumn.column_name;
                        joinTuple.rightcolumn = rightColumn.column_name;
                        String jtKey = joinTuple.leftcolumn + "~" + joinTuple.rightcolumn;

                        // Now check if join condition is repeated at this level
                        // if yes then add a separate Join record, else add a JoinTuple

                        boolean foundJT = false;
                        if(join.joinTuples.get(jtKey) == null) {
                            // Now try reversing the columns and search
                            jtKey = joinTuple.rightcolumn + "~" + joinTuple.leftcolumn;
                            if (join.joinTuples.get(jtKey) == null) {
                                join.joinTuples.put(jtKey, joinTuple);
                            }
                            else {  // Found same columns and same tables in join condition definitely a separate join
                                Join joinNew = new Join();
                                joinNew.leftSchema = leftAttr.schema;
                                joinNew.leftTable = leftAttr.tableName;
                                joinNew.rightSchema = rightAttr.schema;
                                joinNew.rightTable = rightAttr.tableName;
                                joinNew.level = queryLevelObj.getLevel();
                                joinNew.joinTuples.put(jtKey, joinTuple);
                                localJoinHashmap.put(key,joinNew); // Put the new Join Object in the local cache
                            }
                        } else { // key already exists add a new Join row
                            Join joinNew = new Join();
                            joinNew.leftSchema = leftAttr.schema;
                            joinNew.leftTable = leftAttr.tableName;
                            joinNew.rightSchema = rightAttr.schema;
                            joinNew.rightTable = rightAttr.tableName;
                            joinNew.level = queryLevelObj.getLevel();
                            joinNew.joinTuples.put(jtKey, joinTuple);
                            localJoinHashmap.put(key,joinNew); // Put the new Join Object in the local cache
                        }
                    }
                } else {
                    //  Where Clause Conditions
                    Attribute attribute = new Attribute();
                    attribute.tableName = condition.leftTable;
                    attribute.name = condition.leftColumn;
//                    attribute.level = condition.level;
                    Column col = resolveColumnForJoin(attribute, queryLevelObj, current_search_path); // Get Column Object

                    col.whereConditionValue.put(condition.rightValue, condition.fullExpression);

                    Object doesKeyExist = col.whereConditionFreq.get(condition.rightValue);

                    if (doesKeyExist == null ){
                        col.whereConditionFreq.put(condition.rightValue, 1);
                    } else {
                        Integer currFreq = 0;
                        currFreq = col.whereConditionFreq.get(condition.rightValue).intValue();
                        col.whereConditionFreq.put(condition.rightValue, currFreq+1);
                    }

                    col.incrementWhereUsageScore();
                }

            } catch (Exception e) {
                log.error("Error processCondition: " + condition.fullExpression);
                // HSException hsException = new HSException("ModelService.processConditions()", "Exception in processing condition.",
                //         e.toString(), "condition=" + condition.fullExpression, userId);

            }
        }
        // Now merge the local Join Cache with the Global Join Cache
        //tablelist.joinHashMap  -> Global Join Cache
        //localJoinHashmap -> Local Join Cache

        Iterator<Map.Entry<String, Join>> entries = localJoinHashmap.entrySet().iterator();

        while(entries.hasNext()){
            Map.Entry<String, Join> entry = entries.next();
            Join join = entry.getValue();
            boolean isSelfJoin = false;

            // If its a self-join then add join to only one table
            if(join.leftSchema.equals(join.rightSchema) && join.leftTable.equals(join.rightTable)){
                isSelfJoin = true;
            }
            // Find left Table in tablelist, add join condition
            tablelist.findTable(join.leftSchema, join.leftTable).addJoin(join);
            // Find right Table in tablelist, and add join condition
            if ( isSelfJoin==false) {
                tablelist.findTable(join.rightSchema, join.rightTable).addJoin(join);
            }
        }

    }


    private Column resolveColumnForJoin(Attribute column, com.haystack.visitor.Query queryLevelObj, String current_search_path) {
        String tableName = column.tableName;
        String columnName = column.name;
        String schemaName = column.schema;
        Boolean found = false;
        Column col = null;
        try {
            if (tableName == null || tableName.length() == 0) { // - TableName is not specified hence we will have to find the column
                // - in the tables on the same level in the Query
                for (int i = 0; i < queryLevelObj.tables.size(); i++) {
                    QryTable qryTable = queryLevelObj.tables.get(i);
                    col = tablelist.findColumn(qryTable.schema, qryTable.tablename, column.name, current_search_path);
                    if (col != null) {
                        column.schema = qryTable.schema;
                        column.tableName = qryTable.tablename;
                        found = true;
                        break;
                    }
                }
            } else {
                for (int i = 0; i < queryLevelObj.tables.size(); i++) {
                    QryTable qryTable = queryLevelObj.tables.get(i);
                    if (tableName.equals(qryTable.tablename)) {
                        // Table Name matches, now check if schema matches
                        if (schemaName == null) {
                            // set schema for the columns for future use
                            col = tablelist.findColumn(schemaName, tableName, columnName, current_search_path);
                            if (col == null) {
                                found = false; // Column does'nt exist in Table, move on to next table
                            } else {
                                column.schema = qryTable.schema;
                                found = true;
                                break;
                            }
                        } else {
                            if (schemaName.equals(qryTable.schema)) {
                                col = tablelist.findColumn(schemaName, tableName, columnName, current_search_path);
                                if (col == null) {
                                    found = false; // Column does'nt exist in Table, move on to next table
                                } else {
                                    found = true;
                                    break;
                                }
                            }
                        }
                    } else { // Mismatch now check if alias matches
                        if (tableName.equals(qryTable.alias)) {
                            col = tablelist.findColumn(qryTable.schema, qryTable.tablename, columnName, current_search_path);
                            if (col == null) {
                                found = false; // Column does'nt exist in Table, move on to next table
                            } else {
                                column.schema = qryTable.schema;
                                column.tableName = qryTable.tablename;
                                found = true;
                                break;
                            }
                        }
                    }
                }
                if(col == null) {
                    log.debug("I give up cannot resolve column:" + column.toString());
                }
            }
        } catch( Exception e){
            log.error("Error Resolving Column:"+ column.nameFQN + " " + e.toString());
            HSException hsException = new HSException("ModelService.resolveColumnForJoin()", "Exception in resolving column for join.",
                    e.toString(), "column=" + column.nameFQN, userId);
        }
        return col;
    }

    private void processProjectedColumn(Attribute attribute, ArrayList<QryTable> tables, String current_search_path) {
        try
        {
            // For Debugging
            if (attribute.name.equals("*")) {
                // Add all columns
                attribute = attribute;
            }
            Column resolvedColumn = resolveColumn(attribute, tables, current_search_path);
            // Check if TableName can be resolved with the Tables extracted by Parser in the Query


            // Increment Column Usage in the Table Cache
            if (resolvedColumn != null ) {
                if (resolvedColumn.getResolvedTableName() != null) {
                    resolvedColumn.incrementUsageScore();
                    attribute.schema = resolvedColumn.getResolvedSchemaName();
                    attribute.tableName = resolvedColumn.getResolvedTableName();
                    log.info("processColumn Complete:" + attribute.getFQDN());
                }
            }
        }
        catch (Exception e){
            log.error("Error processColumn: " + attribute.nameFQN);
            HSException hsException = new HSException("ModelService.processProjectedColumn()", "Exception in processing column.",
                    e.toString(), "column=" + attribute.nameFQN, userId);
        }
    }

    private Column resolveColumn(Attribute column, ArrayList<QryTable> tables , String current_search_path) {
        String tableName = column.tableName;
        String columnName = column.name;
        String schemaName = column.schema;
        Boolean found = false;

        Column col = null;
            try {
                if (tableName == null || tableName.length() == 0) { // - TableName is not specified hence we will have to find the column
                    // - in the tables on the same level in the Query
                    for (int i = 0; i < tables.size(); i++) {
                        QryTable qryTable = tables.get(i);
                        col = tablelist.findColumn(qryTable.schema, qryTable.tablename, column.name, current_search_path);
                        if (col != null) {
                            col.setResolvedNames(qryTable.schema,qryTable.tablename);
                            found = true;
                            break;
                        }
                    }
                } else {
                    for (int i = 0; i < tables.size(); i++) {
                        QryTable qryTable = tables.get(i);

                        if (tableName.equals(qryTable.tablename)) {
                            // Table Name matches, now check if schema matches
                            if (schemaName == null) {
                                // set schema for the columns for future use
                                String tmpSchema = tablelist.findSchema(tableName, current_search_path);
                                col = tablelist.findColumn(tmpSchema, tableName, columnName, current_search_path);
                                if (col == null) {
                                    found = false; // Column does'nt exist in Table, move on to next table
                                } else {
                                    col.setResolvedNames(qryTable.schema,qryTable.tablename);
                                    found = true;
                                    break;
                                }
                            } else {
                                if (schemaName.equals(qryTable.schema)) {
                                    col = tablelist.findColumn(schemaName, tableName, columnName, current_search_path);
                                    if (col == null) {
                                        found = false; // Column does'nt exist in Table, move on to next table
                                    } else {
                                        found = true;
                                        col.setResolvedNames(qryTable.schema,qryTable.tablename);
                                        break;
                                    }
                                }
                            }
                        } else { // Mismatch now check if alias matches
                            if (tableName.equals(qryTable.alias)) {
                                col = tablelist.findColumn(qryTable.schema, qryTable.tablename, columnName, current_search_path);
                                if (col == null) {
                                    found = false; // Column does'nt exist in Table, move on to next table
                                } else {
                                    col.setResolvedNames(qryTable.schema,qryTable.tablename);
                                    found = true;
                                    break;
                                }
                            }
                        }
                    }
                }
            } catch( Exception e){
                log.error("Error Resolving Column:"+ column.nameFQN + " " + e.toString());
                HSException hsException = new HSException("ModelService.resolveColumn()", "Exception in resolving column.",
                        e.toString(), "column=" + column.nameFQN, userId);
            }
        return col;
    }

    // TODO No need for this function since used Hashmap key to search
    private Join searchJoinHashMap(String leftSchema, String leftTable, String rightSchema, String rightTable, HashMap<String, Join> joinHashMap){
        Join resJoin = null;

        for(int i=0; i<joinHashMap.size(); i++){
            resJoin = joinHashMap.get(i);
            if (leftSchema.equals(resJoin.leftSchema) && leftTable.equals(resJoin.leftTable) &&
                    rightSchema.equals(resJoin.rightSchema) && rightTable.equals(resJoin.rightTable)){
                return resJoin;
            }
            if (leftSchema.equals(resJoin.rightSchema) && leftTable.equals(resJoin.rightTable) &&
                    rightSchema.equals(resJoin.leftSchema) && rightTable.equals(resJoin.leftTable)){
                return resJoin;
            }
        }
        return resJoin;
    }

    // TODO No need for this function since the child attribute will always increment usage
    //Date 18/6/2016
    /*private Column resolveColAliasFromSubQuery(Attribute column, parserDOM currTablesNF) {
        Column retColumn = null;
        int intLevel = column.level.lastIndexOf(".");
        if (intLevel == -1) { // Root level
            intLevel = 0;
        }

        for (int i=0; i< currTablesNF.columns.size(); i++){
            Attribute currCol = currTablesNF.columns.get(i);
            int l1 = currCol.level.lastIndexOf(".");
            if (l1 > intLevel) { // child attribute
                // TODO match column name with alias of child column
            }
        }
        return retColumn;
    }*/

}
