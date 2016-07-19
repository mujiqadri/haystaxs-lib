/*
 * #%L
 * JSQLParser library
 * %%
 * Copyright (C) 2004 - 2013 JSQLParser
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as 
 * published by the Free Software Foundation, either version 2.1 of the 
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Lesser Public License for more details.
 * 
 * You should have received a copy of the GNU General Lesser Public 
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/lgpl-2.1.html>.
 * #L%
 */
package com.haystack.parser.util;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.update.Update;

import java.net.Inet4Address;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Stack;

import com.haystack.domain.Attribute;
import com.haystack.visitor.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.haystack.domain.QryTable;
import com.haystack.domain.Condition;
/**
 * Find all used tables within an select statement.
 */
public class parserDOM implements SelectVisitor, FromItemVisitor, ExpressionVisitor, ItemsListVisitor, SelectItemVisitor, OrderByVisitor {
    static Logger log = LoggerFactory.getLogger(parserDOM.class.getName());
//    public ArrayList<QryTable> tables;
//    public ArrayList<Attribute> columns;
//    public ArrayList<Condition> conditions;
    private HashMap<String, Integer> levelsHashMap;
    private HashMap<Integer, String> queryHashLevels;
    private List<String> otherItemNames;

    public Query queryLevelObj;
    private String currLevel;

    private Stack<String> currLocation;

    public String getCurrLevel() {
        return currLevel;
    }
    public void setCurrLevel(String currLevel) {
        this.currLevel = currLevel;
    }

    private void init() {
        otherItemNames = new ArrayList<String>();
//        tables = new ArrayList<QryTable>();
//        columns = new ArrayList<Attribute>();
//        conditions = new ArrayList<Condition>();
        levelsHashMap = new HashMap<String, Integer>();
        queryHashLevels = new HashMap<Integer, String>();
        queryLevelObj = new Query(null);
        setCurrLevel("1");

        //To keep track of current usage location of column
        currLocation = new Stack<String>();
    }

   /* public ArrayList<Attribute> getAttributesForTable(String tablename, String schemaName) {
        ArrayList<Attribute> result = new ArrayList<Attribute>();

        for (int i = 0; i < columns.size(); i++) {
            Attribute attr = columns.get(i);
            if (attr.schema != null && attr.tableName != null) {
                if (attr.schema.equals(schemaName)) {
                    if (attr.tableName.equals(tablename)) {
                        result.add(attr);
                    }
                }
            }
        }
        return result;
    }*/

    private String getNextSubLevel(String level) {

        String splitArr[] = level.split("\\.");
        Integer intLevel = splitArr.length;
        Integer maxSubCnt = 0;

        maxSubCnt = levelsHashMap.get(level);
        if (maxSubCnt == null) { // No key in hashmap
            maxSubCnt = 1;
        } else {
            maxSubCnt++;
        }

        // Now create the new level key for usage
        String retNewLevel = level + "." + maxSubCnt.toString();
        levelsHashMap.put(level, maxSubCnt);

        return retNewLevel;
    }

    private String getNextSiblingLevel(String currLevel) {
        // TODO
        return "null";
    }


    @Override
    public void visit(OracleHint hint) {
    }

    /**
     * Main entry for this Tool class. A list of found tables is returned.
     *
     * @param delete
     * @return
     */
    public List<String> getTableList(Delete delete) {
        init();
        otherItemNames.add(delete.getTable().getName());
        if (delete.getWhere() != null) {
            delete.getWhere().accept(this);
        }

        return otherItemNames;
    }

    /**
     * Main entry for this Tool class. A list of found tables is returned.
     *
     * @param insert
     * @return
     */
    public List<String> getTableList(Insert insert) {
        init();
        otherItemNames.add(insert.getTable().getName());
        if (insert.getItemsList() != null) {
            insert.getItemsList().accept(this);
        }

        return otherItemNames;
    }

    /**
     * Main entry for this Tool class. A list of found tables is returned.
     *
     * @param replace
     * @return
     */
    public List<String> getTableList(Replace replace) {
        init();
        otherItemNames.add(replace.getTable().getName());
        if (replace.getExpressions() != null) {
            for (Expression expression : replace.getExpressions()) {
                expression.accept(this);
            }
        }
        if (replace.getItemsList() != null) {
            replace.getItemsList().accept(this);
        }

        return otherItemNames;
    }

    /**
     * Main entry for this Tool class. A list of found tables is returned.
     *
     * @param select
     * @return
     */
    public void getSemantics(Select select) {
        init();
        levelsHashMap.put("0", 1); // Add root element for first level
        queryHashLevels.put(select.toString().hashCode(), "0");

        if (select.getWithItemsList() != null) {
            for (WithItem withItem : select.getWithItemsList()) {
                withItem.accept(this);
            }
        }
        select.getSelectBody().accept(this);
    }

    /**
     * Main entry for this Tool class. A list of found tables is returned.
     *
     * @param update
     * @return
     */
    public void getSemantics(Update update) {
        init();
        levelsHashMap.put("0", 1); // Add root element for first level
        queryHashLevels.put(update.toString().hashCode(), "0");

        for (Table table : update.getTables()) {
            otherItemNames.add(table.getName());


            QryTable qryTable = new QryTable();

            try {
                qryTable.tablename = table.getName();
                qryTable.alias = table.getAlias().getName();
                qryTable.schema = table.getSchemaName();
            }catch(Exception ex){}

            queryLevelObj.addTable(queryLevelObj, qryTable, currLevel);
        }
        if (update.getExpressions() != null) {
            for (Expression expression : update.getExpressions()) {
                expression.accept(this);
            }
        }

        if (update.getFromItem() != null) {
            update.getFromItem().accept(this);
        }

        if (update.getJoins() != null) {
            for (Join join : update.getJoins()) {
                join.getRightItem().accept(this);
            }
        }

        if (update.getWhere() != null) {
            update.getWhere().accept(this);
        }
    }

    @Override
    public void visit(WithItem withItem) {
        otherItemNames.add(withItem.getName().toLowerCase());
        withItem.getSelectBody().accept(this);
    }

    @Override
    public void visit(PlainSelect plainSelect) {
        //TODO: Populate structure in this method
        queryHashLevels.put(plainSelect.toString().hashCode(), currLevel);

        if (plainSelect.getSelectItems() != null) {
            for (SelectItem item : plainSelect.getSelectItems()) {
                currLocation.push("in_select");
                item.accept(this);
                currLocation.pop();
            }
        }

        if (plainSelect.getGroupByColumnReferences() != null) {
            List<Expression> groupByCols = plainSelect.getGroupByColumnReferences();
            for (int i = 0; i < groupByCols.size(); i++) {
                currLocation.push("in_groupby");
                groupByCols.get(i).accept(this);
                currLocation.pop();

                //by: muji
                //We are not extracting groupby columns because it dose not add another operation usage for the query
                // Date:18/6/2016
                /*
                Column grpByRefs = (Column)groupByCols.get(i);
                Attribute column = new Attribute();
                column.name = grpByRefs.getColumnName();
                column.level = level;
                try {
                    //column.alias = grpByRefs.g
                    column.tableName = grpByRefs.getTable().getName();
                    column.schema = grpByRefs.getTable().getSchemaName();
                    column.nameFQN = grpByRefs.getFullyQualifiedName();
                } catch (Exception e){

                }
                columns.add(column);
                */
            }
        }

        if (plainSelect.getHaving() != null) {
            currLocation.push("in_having");
            plainSelect.getHaving().accept(this);
            currLocation.pop();
        }

        if(plainSelect.getFromItem() != null) {
            plainSelect.getFromItem().accept(this);
        }

        if (plainSelect.getJoins() != null) {
            for (Join join : plainSelect.getJoins()) {
                join.getRightItem().accept(this);
                if (join.getOnExpression() != null) {
                    join.getOnExpression().accept(this);
                }
            }
        }



        if (plainSelect.getWhere() != null) {
            currLocation.push("in_where");
            plainSelect.getWhere().accept(this);
            currLocation.pop();
        }

        if(plainSelect.getOrderByElements() != null){
            for(OrderByElement orderByElement : plainSelect.getOrderByElements()){
                currLocation.push("in_orderby");
                orderByElement.accept(this);
                currLocation.pop();
            }
        }


        if (plainSelect.getOracleHierarchical() != null) {
            plainSelect.getOracleHierarchical().accept(this);
        }

    }

    @Override
    public void visit(Table tableName) {
        try {
            //String tableWholeName = tableName.getFullyQualifiedName() + ':' + tableName.getAlias().toString().trim();
            QryTable table = new QryTable();
            table.database = tableName.getDatabase().toString();
            table.schema = tableName.getSchemaName();
            table.tablename = tableName.getName();
            if (tableName.getAlias() != null) {
                table.alias = tableName.getAlias().toString().trim();
            }
//            table.level = level;
//            tables.add(table);
            queryLevelObj.addTable(queryLevelObj, table, currLevel);

        } catch (Exception e) {
            log.error("Error in adding Table in NF:" + tableName.toString() + ": Error Msg:" + e.toString());
        }

        /*if (!otherItemNames.contains(tableWholeName.toLowerCase())) {
            otherItemNames.add(tableWholeName.toLowerCase());
            tables.add(table);
        }
        */
    }

    @Override
    public void visit(Column tableColumn) {
        try {
            Attribute column = new Attribute();
            column.name = tableColumn.getColumnName();
            column.tableName = tableColumn.getTable().getName();
            column.schema = tableColumn.getTable().getSchemaName();
            column.nameFQN = tableColumn.getFullyQualifiedName();

            queryLevelObj.addColumn(queryLevelObj, column, currLevel);

            //For audittrail
            String currUsageLocation = currLocation.pop();
            column.usageLocation = currUsageLocation;
            currLocation.push(currUsageLocation);

        } catch (Exception e) {
            log.error("Error in extracting alias for Column:" + tableColumn.getFullyQualifiedName());
        }
    }

    @Override
    public void visit(AllColumns allColumns) {
        Attribute column = new Attribute();
        column.name = "*";
        column.tableName = null;
        column.schema = null;
        column.nameFQN = "*";
        queryLevelObj.addColumn(queryLevelObj, column, currLevel);
    }

    @Override
    public void visit(AllTableColumns allTableColumns) {
        Attribute column = new Attribute();
        column.name = "*";
        column.tableName = allTableColumns.getTable().toString();
        column.schema = allTableColumns.getTable().getSchemaName();
        column.nameFQN = allTableColumns.toString();
        queryLevelObj.addColumn(queryLevelObj, column, currLevel);
    }
    @Override
    public void visit(Function function) {
        // 17-June-2015 Muji - Extract columns from the function
        try {
            ExpressionList parameters = function.getParameters();
            if (parameters != null) {
                for (int i = 0; i < parameters.getExpressions().size(); i++) {
                    Expression param = parameters.getExpressions().get(i);

                    // Check if expression is BinaryExpress (i.e. has left and right expression
                    try {
                        BinaryExpression bExp = (BinaryExpression) param;
                        if ((bExp.getLeftExpression() != null) && (bExp.getRightExpression() != null)) {

                            bExp.getLeftExpression().accept(this);
                            bExp.getRightExpression().accept(this);
                            continue;
                        }
                    } catch (Exception e) {

                    }
                    // Check for nested function inside the parameter if found then call visit nested
                    try {
                        Function nestedFunc = (Function) param;
                        if (nestedFunc.getParameters() != null) {
                            visit(nestedFunc);
                            continue;
                        }
                    } catch (Exception e) {
                    }

                    Attribute column = new Attribute();
                    try {
                        column.name = ((Column) param).getColumnName();
                        try {
                            column.tableName = ((Column) param).getTable().getName();
                            column.nameFQN = ((Column) param).getTable().getFullyQualifiedName();
                        } catch (Exception e) {
                        }
                        queryLevelObj.addColumn(queryLevelObj, column, currLevel);
                    } catch (Exception e) {

                    }
                }
            }
        } catch (Exception e) {
            log.error(e.toString());
        }
    }


    public void visitBinaryExpression(BinaryExpression binaryExpression) {
        Condition leftCondition = null, rightCondition = null;
        try {
            Condition condition = extractCondition(binaryExpression);
        } catch (Exception e) {
            log.debug("Do nothing, not a leaf expression" + binaryExpression.toString() + e.toString());
        }
        try {
            leftCondition = extractCondition(binaryExpression.getLeftExpression());
        } catch (Exception e) {
            log.debug("left extractCondition expression:" + this.toString() + " exception:" + e.toString());
        }
        try {
            rightCondition = extractCondition(binaryExpression.getRightExpression());
        } catch (Exception e) {
            log.debug("right extractCondition expression:" + this.toString() + " exception:" + e.toString());
        }
        // Now visit the rest of the tree
        //if (leftCondition == null ){
        binaryExpression.getLeftExpression().accept(this);
        //}
        //if (rightCondition == null) {
        binaryExpression.getRightExpression().accept(this);
        //}
    }

    private Condition extractCondition(Expression currExpression) {

        try {
            String expressionClass = currExpression.getClass().toString();

            if (expressionClass.endsWith(".Column") || expressionClass.endsWith(".Function") || expressionClass.endsWith("Value")
                    || expressionClass.endsWith(".SubSelect")) {
                return null;
            }
            Condition condition = new Condition();

            BinaryExpression binaryExpression = null;
            try {
                binaryExpression = (BinaryExpression) currExpression;
            } catch (Exception e) {
                // Not a Binary Expression
            }
            if (binaryExpression != null) {

                binaryExpression = (BinaryExpression) currExpression;
                condition.fullExpression = binaryExpression.toString();
                condition.leftExpression = binaryExpression.getLeftExpression().toString();
                condition.rightExpression = binaryExpression.getRightExpression().toString();
                condition.operator = binaryExpression.getStringExpression();
                try {
                    if (((Column) (binaryExpression).getLeftExpression()).getColumnName() != null) {
                        condition.leftTable = ((Column) binaryExpression.getLeftExpression()).getTable().toString();
                        condition.leftColumn = ((Column) binaryExpression.getLeftExpression()).getColumnName();
                        //((Column) binaryExpression.getLeftExpression()).accept(this);

                    }
                } catch (Exception e) {
                    condition.isJoin = false;
                    condition.leftValue = ((LongValue) binaryExpression.getLeftExpression()).getStringValue();
                }

                try {
                    if (((Column) (binaryExpression).getRightExpression()).getColumnName() != null) {
                        condition.rightTable = ((Column) binaryExpression.getRightExpression()).getTable().toString();
                        condition.rightColumn = ((Column) binaryExpression.getRightExpression()).getColumnName();
                        // ((Column) binaryExpression.getRightExpression()).accept(this);
                    }
                } catch (Exception e) {
                    condition.isJoin = false;
                    condition.rightValue = binaryExpression.getRightExpression().toString();
                }
                if (condition.operator.equals("-")) { // This is projection expression donot add it to conditions
                    condition.operator = condition.operator;
                } else{
                    queryLevelObj.addCondition(queryLevelObj, condition, currLevel);
                }
            } else {
                if (expressionClass.contains(".arithmetic") || expressionClass.contains(".Between") || expressionClass.contains(".EqualsTo")) {
                    log.error("Dangling condition:" + currExpression.toString());
                    condition.fullExpression = ((BinaryExpression) currExpression).getStringExpression();
                } else {
                    if (expressionClass.contains(".IsNullExpression")) {
                        condition.fullExpression = currExpression.toString();
                        condition.leftExpression = ((IsNullExpression) currExpression).getLeftExpression().toString();
                        condition.leftTable = ((Column) ((IsNullExpression) currExpression).getLeftExpression()).getTable().getName();
                        condition.leftColumn = ((Column) ((IsNullExpression) currExpression).getLeftExpression()).getColumnName().toString();
                        condition.isJoin = false;
                        if (((IsNullExpression) currExpression).isNot() == true) {
                            condition.operator = "ISNOTNULL";
                        } else {
                            condition.operator = "ISNULL";
                        }
                        queryLevelObj.addCondition(queryLevelObj, condition, currLevel);
                    } else if (expressionClass.endsWith("relational.InExpression")) {
                        InExpression inExpression = (InExpression) currExpression;
                        condition.fullExpression = currExpression.toString();
                        condition.leftExpression = inExpression.getLeftExpression().toString();
                        condition.rightExpression = inExpression.getRightItemsList().toString();
                        condition.leftTable = ((Column) inExpression.getLeftExpression()).getTable().getName();
                        condition.leftColumn = ((Column) inExpression.getLeftExpression()).getColumnName().toString();
                        condition.isJoin = false;
                        condition.operator = "IN";

                    } else {
                        // TODO: CaseExpression evaluate if we need to convert this into a where condition
                        condition = condition;
                        log.error("Dangling condition:" + currExpression.toString());
                    }
                }
            }
            return condition;
        } catch (Exception e) {
            // Not a leaf level expression return null
            return null;
        }
    }

    @Override
    public void visit(SelectExpressionItem item) {
        item.getExpression().accept(this);
    }

    @Override
    public void visit(Addition addition) {
        visitBinaryExpression(addition);
    }

    @Override
    public void visit(AndExpression andExpression) {
        visitBinaryExpression(andExpression);
    }

    @Override
    public void visit(Between between) {
        between.getLeftExpression().accept(this);
        between.getBetweenExpressionStart().accept(this);
        between.getBetweenExpressionEnd().accept(this);
    }

    @Override
    public void visit(SubSelect subSelect) {
        String nextLevelkey = getNextSubLevel(currLevel);
        queryHashLevels.put(subSelect.toString().hashCode(), currLevel);
        currLevel = nextLevelkey;

        subSelect.getSelectBody().accept(this);
        currLevel = queryHashLevels.get(subSelect.toString().hashCode());
    }


    @Override
    public void visit(Division division) {
        visitBinaryExpression(division);
    }

    @Override
    public void visit(DoubleValue doubleValue) {
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        visitBinaryExpression(equalsTo);
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        visitBinaryExpression(greaterThan);
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        visitBinaryExpression(greaterThanEquals);
    }

    @Override
    public void visit(InExpression inExpression) {
        inExpression.getLeftExpression().accept(this);
        inExpression.getRightItemsList().accept(this);
    }

    @Override
    public void visit(SignedExpression signedExpression) {
        signedExpression.getExpression().accept(this);
    }

    @Override
    public void visit(IsNullExpression isNullExpression) {
    }

    @Override
    public void visit(JdbcParameter jdbcParameter) {
    }

    @Override
    public void visit(LikeExpression likeExpression) {
        visitBinaryExpression(likeExpression);
    }

    @Override
    public void visit(ExistsExpression existsExpression) {
        existsExpression.getRightExpression().accept(this);
    }

    @Override
    public void visit(LongValue longValue) {
    }

    @Override
    public void visit(HexValue hexValue) {

    }

    @Override
    public void visit(MinorThan minorThan) {
        visitBinaryExpression(minorThan);
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        visitBinaryExpression(minorThanEquals);
    }

    @Override
    public void visit(Multiplication multiplication) {
        visitBinaryExpression(multiplication);
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        visitBinaryExpression(notEqualsTo);
    }

    @Override
    public void visit(NullValue nullValue) {
    }

    @Override
    public void visit(OrExpression orExpression) {
        visitBinaryExpression(orExpression);
    }

    @Override
    public void visit(Parenthesis parenthesis) {
        parenthesis.getExpression().accept(this);
    }

    @Override
    public void visit(StringValue stringValue) {
    }

    @Override
    public void visit(Subtraction subtraction) {
        visitBinaryExpression(subtraction);
    }

    @Override
    public void visit(ExpressionList expressionList) {
        for (Expression expression : expressionList.getExpressions()) {
            expression.accept(this);
        }

    }

    @Override
    public void visit(DateValue dateValue) {
    }

    @Override
    public void visit(TimestampValue timestampValue) {
    }

    @Override
    public void visit(TimeValue timeValue) {
    }

    /*
     * (non-Javadoc)
     *
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.CaseExpression)
     */
    @Override
    public void visit(CaseExpression caseExpression) {
    }

    @Override
    public void visit(WhenClause whenClause) {
    }

    @Override
    public void visit(AllComparisonExpression allComparisonExpression) {
        allComparisonExpression.getSubSelect().getSelectBody().accept(this);
    }

    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression) {
        anyComparisonExpression.getSubSelect().getSelectBody().accept(this);
    }

    @Override
    public void visit(SubJoin subjoin) {
        subjoin.getLeft().accept(this);
        subjoin.getJoin().getRightItem().accept(this);
    }

    @Override
    public void visit(Concat concat) {
        visitBinaryExpression(concat);
    }

    @Override
    public void visit(Matches matches) {
        visitBinaryExpression(matches);
    }

    @Override
    public void visit(BitwiseAnd bitwiseAnd) {
        visitBinaryExpression(bitwiseAnd);
    }

    @Override
    public void visit(BitwiseOr bitwiseOr) {
        visitBinaryExpression(bitwiseOr);
    }

    @Override
    public void visit(BitwiseXor bitwiseXor) {
        visitBinaryExpression(bitwiseXor);
    }

    @Override
    public void visit(CastExpression cast) {
        cast.getLeftExpression().accept(this);
    }

    @Override
    public void visit(Modulo modulo) {
        visitBinaryExpression(modulo);
    }

    @Override
    public void visit(AnalyticExpression analytic) {
    }

    @Override
    public void visit(SetOperationList list) {
        for (SelectBody plainSelect : list.getSelects()) {
            plainSelect.accept(this);
        }
    }

    @Override
    public void visit(ExtractExpression eexpr) {
    }

    @Override
    public void visit(LateralSubSelect lateralSubSelect) {
        lateralSubSelect.getSubSelect().getSelectBody().accept(this);
    }

    @Override
    public void visit(MultiExpressionList multiExprList) {
        for (ExpressionList exprList : multiExprList.getExprList()) {
            exprList.accept(this);
        }
    }

    @Override
    public void visit(ValuesList valuesList) {
    }

    @Override
    public void visit(TableFunction tableFunction) {

    }


    @Override
    public void visit(IntervalExpression iexpr) {
    }

    @Override
    public void visit(JdbcNamedParameter jdbcNamedParameter) {
    }

    @Override
    public void visit(OracleHierarchicalExpression oexpr) {
        if (oexpr.getStartExpression() != null) {
            oexpr.getStartExpression().accept(this);
        }

        if (oexpr.getConnectExpression() != null) {
            oexpr.getConnectExpression().accept(this);
        }
    }

    @Override
    public void visit(RegExpMatchOperator rexpr) {
        visitBinaryExpression(rexpr);
    }

    @Override
    public void visit(RegExpMySQLOperator rexpr) {
        visitBinaryExpression(rexpr);
    }

    @Override
    public void visit(JsonExpression jsonExpr) {
    }


    @Override
    public void visit(WithinGroupExpression wgexpr) {
    }

    @Override
    public void visit(UserVariable var) {
    }

    @Override
    public void visit(NumericBind numericBind) {

    }

    @Override
    public void visit(KeepExpression keepExpression) {

    }

    @Override
    public void visit(MySQLGroupConcat mySQLGroupConcat) {

    }

    @Override
    public void visit(RowConstructor rowConstructor) {

    }

    @Override
    public void visit(OrderByElement orderByElement) {
        orderByElement.getExpression().accept(this);
    }
}
