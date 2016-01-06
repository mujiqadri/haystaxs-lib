package com.haystack.service;


import com.haystack.domain.*;

import com.haystack.parser.JSQLParserException;
import com.haystack.parser.statement.update.Update;
import com.haystack.util.HSException;
import com.sun.org.apache.xpath.internal.operations.Bool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.haystack.parser.parser.CCJSqlParserUtil;
import com.haystack.parser.statement.Statement;
import com.haystack.parser.statement.select.Select;
import com.haystack.parser.util.TablesNamesFinder;

import java.util.*;

/**
 * Created by qadrim on 15-03-04.
 *
 */
public class ModelService {


    static Logger log = LoggerFactory.getLogger(ModelService.class.getName());
    static Tables tablelist;
    private Integer userId = null;

    public ModelService(){
        tablelist = new Tables();
    }
    public void setTableList(Tables tbllist){
        this.tablelist = tbllist;
    }

    public void scoreModel(){
        try {
            float totalWorkloadScore = 0;
            // Calculate total workload score
            Iterator<Map.Entry<String, Table>> entriesForTotal = tablelist.tableHashMap.entrySet().iterator();
            while(entriesForTotal.hasNext()) {
                Map.Entry<String, Table> entry = entriesForTotal.next();
                String currKey = entry.getKey();
                Table currTable = entry.getValue();
                float currentWorkload = currTable.stats.getWorkloadScore();
                totalWorkloadScore += currentWorkload;
            }


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
                tablelist.tableHashMap.put(currKey, currTable);
            }
        } catch (Exception e) {
            log.error(e.toString());
        }
    }
    public String getModelJSON(){
        return tablelist.getJSON();
    }

    public void processSQL(Query query, double executionTime, Integer userId) throws Exception {
        processSQL(query.getQueryText(), executionTime, userId);
    }

    public boolean processSQL(String query, double executionTime, Integer userId) {
        TablesNamesFinder currtablesNF = new TablesNamesFinder();
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
            } catch (Exception e) {
                log.error("ModelService.processSQL() : Error in parsing SQL=" + query.toString());
                // HSException hsException = new HSException("ModelService.processSQL()", "Error in parsing SQL", e.toString(), "SQL=" + query, userId);
            }
            Select selectStatement = null;
            Update updateStatement = null;
            String stmtType = "";

            if (statement == null) {
                return false;
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
                strTableList = currtablesNF.getSemantics(selectStatement, "1");
            } else if (stmtType == "UPDATE") {
                strTableList = currtablesNF.getSemantics(updateStatement, "1");
                if (strTableList.size() > 0) {
                    QryTable q = new QryTable();
                    q.tablename = strTableList.get(0).toString();
                    currtablesNF.tables.add(q);
                }
            } else {
                log.error("Statement Not Supported :" + statement.toString());
                //HSException hsException = new HSException("ModelService.processSQL()", "Statement Not Supported", null, "SQL=" + query, userId);
                return false;
            }

            // === Resolve table schema name if missing
            for (int i = 0; i < currtablesNF.tables.size(); i++) {
                String schemaName = "";
                if (currtablesNF.tables.get(i).schema == null) {
                    schemaName = tablelist.findSchema(currtablesNF.tables.get(i).tablename);
                    currtablesNF.tables.get(i).schema = schemaName;
                }
                Table tbl = tablelist.findTable(currtablesNF.tables.get(i).schema, currtablesNF.tables.get(i).tablename);
                if (tbl != null){ // Increment table Usage, if its not a derived table
                    tbl.stats.incrementUsageFrequency();
                }
                log.info("Table Extracted:" + schemaName + "." + currtablesNF.tables.get(i).tablename) ;
            }
            // === Resolve column names to original table, where aliases were used or no TableName/Alias specified
            // === Increment column usage
            for (int i = 0; i < currtablesNF.columns.size(); i++) {
                processProjectedColumn(currtablesNF.columns.get(i), currtablesNF);
            }
            processConditions(currtablesNF);

            divideTimeAmongstTables(currtablesNF, executionTime);

            log.debug("=============== CONDITIONS EXTRACTED =================");

            log.info("ModelService.processSQL Complete");
            return true;
        }
        catch(Exception e){
            // Log Parsing Error
            log.error("SQL:" + query);
            log.error("PARSING ERROR:" + e.toString());
            HSException hsException = new HSException("ModelService.processSQL()", "Unable to Process Query", e.toString(), "SQL=" + query, userId);
            return false;
        }
    }

    private void divideTimeAmongstTables(TablesNamesFinder currtablesNF, double executionTime) {

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

            for (int i = 0; i < currtablesNF.tables.size(); i++) {
                Boolean isTableProcessed = false;
                QryTable qryTbl = currtablesNF.tables.get(i);

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

                        for (int j = 0; j < currtablesNF.columns.size(); j++) {
                            Attribute attribute = currtablesNF.columns.get(j);
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

    private void processConditions(TablesNamesFinder currtablesNF) {
        // === Extract Conditions
        // === If where clause then increment UsageScore for the column
        // === If join condition then connect the two tables together and increment join usage for left and right column
        HashMap<String, Join> localJoinHashmap = new HashMap<String, Join>();
        for (Condition condition : currtablesNF.conditions) {
            try {
                // Condition can be where clause or a join condition, separate them and load them into local cache
                if (condition.isJoin) { // Join Condition

                    Attribute leftAttr = new Attribute();  // Resolve Aliases or Empty Table Names in the Join Conditions
                    leftAttr.tableName = condition.leftTable;
                    leftAttr.name = condition.leftColumn;
                    leftAttr.level = condition.level;
                    Column leftColumn = resolveColumnForJoin(leftAttr, currtablesNF);

                    Attribute rightAttr = new Attribute();
                    rightAttr.tableName = condition.rightTable;
                    rightAttr.name = condition.rightColumn;
                    rightAttr.level = condition.level;
                    Column rightColumn = resolveColumnForJoin(rightAttr, currtablesNF);

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

                    String key = keyLeftTbl + "~" + keyRightTbl + "~" + condition.level;
                    // Check if the table pair already has a join condition in the cache
                    Join join = localJoinHashmap.get(key);
                    if (join == null ) { // try reversing left and right table
                        key = keyRightTbl + "~" +  keyLeftTbl+ "~" + condition.level;
                        join = localJoinHashmap.get(key);
                    }

                    if (join == null) {
                        // New Join Row set Tables and add Columns
                        join = new Join();
                        join.leftSchema = leftAttr.schema;
                        join.leftTable = leftAttr.tableName;
                        join.rightSchema = rightAttr.schema;
                        join.rightTable = rightAttr.tableName;
                        join.level = condition.level;
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
                                joinNew.level = condition.level;
                                joinNew.joinTuples.put(jtKey, joinTuple);
                                localJoinHashmap.put(key,joinNew); // Put the new Join Object in the local cache
                            }
                        } else { // key already exists add a new Join row
                            Join joinNew = new Join();
                            joinNew.leftSchema = leftAttr.schema;
                            joinNew.leftTable = leftAttr.tableName;
                            joinNew.rightSchema = rightAttr.schema;
                            joinNew.rightTable = rightAttr.tableName;
                            joinNew.level = condition.level;
                            joinNew.joinTuples.put(jtKey, joinTuple);
                            localJoinHashmap.put(key,joinNew); // Put the new Join Object in the local cache
                        }
                    }
                } else {
                    //  Where Clause Conditions
                    Attribute attribute = new Attribute();
                    attribute.tableName = condition.leftTable;
                    attribute.name = condition.leftColumn;
                    attribute.level = condition.level;
                    Column col = resolveColumnForJoin(attribute, currtablesNF); // Get Column Object

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


    private Column resolveColumnForJoin(Attribute column, TablesNamesFinder currTablesNF){
        String tableName = column.tableName;
        String columnName = column.name;
        String schemaName = column.schema;
        Boolean found = false;
        Column col = null;
        try {
            if (tableName == null || tableName.length() == 0) { // - TableName is not specified hence we will have to find the column
                // - in the tables on the same level in the Query
                for (int i = 0; i < currTablesNF.tables.size(); i++) {
                    QryTable qryTable = currTablesNF.tables.get(i);
                    if (column.level.equals(qryTable.level)) {  // filter only tables which are on the same level
                        col = tablelist.findColumn(qryTable.schema, qryTable.tablename, column.name);
                        if (col != null) {
                            column.schema = qryTable.schema;
                            column.tableName = qryTable.tablename;
                            found = true;
                            break;
                        }
                    }
                }
            } else {
                for (int i = 0; i < currTablesNF.tables.size(); i++) {
                    QryTable qryTable = currTablesNF.tables.get(i);

                    if (column.level.equals(qryTable.level)) {
                        if (tableName.equals(qryTable.tablename)) {
                            // Table Name matches, now check if schema matches
                            if (schemaName == null) {
                                // set schema for the columns for future use
                                col = tablelist.findColumn(schemaName, tableName, columnName);
                                if (col == null) {
                                    found = false; // Column does'nt exist in Table, move on to next table
                                } else {
                                    column.schema = qryTable.schema;
                                    found = true;
                                    break;
                                }
                            } else {
                                if (schemaName.equals(qryTable.schema)) {
                                    col = tablelist.findColumn(schemaName, tableName, columnName);
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
                                col = tablelist.findColumn(qryTable.schema, qryTable.tablename, columnName);
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
                }
                if (col == null){ // Went through all the tables and could not find the column, probably an alias from derived table
                    for (int i=0; i<currTablesNF.columns.size(); i++){
                        Attribute currAttr = currTablesNF.columns.get(i);

                        if (currAttr.getDepth() >= column.getDepth()) {
                            if (column.name.equals(currAttr.alias)) {
                                col = tablelist.findColumn(currAttr.schema, currAttr.tableName, currAttr.name);
                                if(col == null) {
                                    continue;
                                }else {
                                    column.schema = currAttr.schema;
                                    column.tableName = currAttr.tableName;
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
            }
        } catch( Exception e){
            log.error("Error Resolving Column:"+ column.nameFQN + " " + e.toString());
            HSException hsException = new HSException("ModelService.resolveColumnForJoin()", "Exception in resolving column for join.",
                    e.toString(), "column=" + column.nameFQN, userId);
        }
        return col;
    }

    private void processProjectedColumn(Attribute attribute, TablesNamesFinder currTablesNF) {
        try
        {
            // For Debugging
            if (attribute.name.equals("sales_cnt")){
                attribute = attribute;
            }
            Column resolvedColumn = resolveColumn(attribute, currTablesNF);
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

    private Column resolveColumn(Attribute column, TablesNamesFinder currTablesNF) {
        String tableName = column.tableName;
        String columnName = column.name;
        String schemaName = column.schema;
        Boolean found = false;

        Column col = null;
            try {
                if (tableName == null || tableName.length() == 0) { // - TableName is not specified hence we will have to find the column
                    // - in the tables on the same level in the Query
                    for (int i = 0; i < currTablesNF.tables.size(); i++) {
                        QryTable qryTable = currTablesNF.tables.get(i);
                        if (column.level.equals(qryTable.level)) {  // filter only tables which are on the same level
                            col = tablelist.findColumn(qryTable.schema, qryTable.tablename, column.name);
                            if (col != null) {
                                col.setResolvedNames(qryTable.schema,qryTable.tablename);
                                found = true;
                                break;
                            }
                        }
                    }

                    if (col == null) { // didn't find the table for the Attribute on the same level,
                        // try whole SQL text and return the first table matched
                        for (int i = 0; i < currTablesNF.tables.size(); i++) {
                            QryTable qryTable = currTablesNF.tables.get(i);
                            col = tablelist.findColumn(qryTable.schema, qryTable.tablename, column.name);
                            if (col != null) {
                                col.setResolvedNames(qryTable.schema,qryTable.tablename);
                                found = true;
                                break;
                            }
                        }
                    }
                } else {
                    for (int i = 0; i < currTablesNF.tables.size(); i++) {
                        QryTable qryTable = currTablesNF.tables.get(i);

                        if (column.level.equals(qryTable.level)) {
                            if (tableName.equals(qryTable.tablename)) {
                                // Table Name matches, now check if schema matches
                                if (schemaName == null) {
                                    // set schema for the columns for future use
                                    String tmpSchema = tablelist.findSchema(tableName);
                                    col = tablelist.findColumn(tmpSchema, tableName, columnName);
                                    if (col == null) {
                                        found = false; // Column does'nt exist in Table, move on to next table
                                    } else {
                                        col.setResolvedNames(qryTable.schema,qryTable.tablename);
                                        found = true;
                                        break;
                                    }
                                } else {
                                    if (schemaName.equals(qryTable.schema)) {
                                        col = tablelist.findColumn(schemaName, tableName, columnName);
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
                                    col = tablelist.findColumn(qryTable.schema, qryTable.tablename, columnName);
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
                    /***  -- No Need to resolve derived columns, leave them alone
                    if (col == null){ // Went through all the tables and could not find the column, probably an alias from derived table
                        for (int i=0; i<currTablesNF.columns.size(); i++){
                            Attribute currAttr = currTablesNF.columns.get(i);

                            if(currAttr.getDepth() > column.getDepth()) {
                                if (column.name.equals(currAttr.alias)) {
                                    col = tablelist.findColumn(currAttr.schema, currAttr.tableName, currAttr.name);
                                    if(col == null) {
                                        continue;
                                    }else {
                                        column.schema = currAttr.schema;
                                        column.tableName = currAttr.tableName;
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
                    */
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
    private Column resolveColAliasFromSubQuery(Attribute column, TablesNamesFinder currTablesNF) {
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
    }
}
