package com.haystack.parser.util;

import com.haystack.domain.Attribute;
import com.haystack.domain.Condition;
import com.haystack.domain.QryTable;
import com.haystack.parser.schema.Column;
import com.haystack.parser.schema.Table;

/*
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.replace.*;
*/


import com.haystack.parser.statement.delete.Delete;
import com.haystack.parser.statement.insert.Insert;
import com.haystack.parser.statement.replace.Replace;
import com.haystack.parser.statement.select.*;
import com.haystack.parser.statement.update.Update;


import com.haystack.parser.expression.*;
import com.haystack.parser.expression.arithmetic.*;
import com.haystack.parser.expression.conditional.AndExpression;
import com.haystack.parser.expression.conditional.OrExpression;
import com.haystack.parser.expression.relational.*;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by qadrim on 16-01-14.
 */


public class ASTGenerator implements SelectVisitor, FromItemVisitor, ExpressionVisitor, ItemsListVisitor, SelectItemVisitor {
    static Logger log = LoggerFactory.getLogger(ASTGenerator.class.getName());

    public StringBuilder json = new StringBuilder();
    public ArrayList<QryTable> tables;
    public ArrayList<Attribute> columns;
    public ArrayList<Condition> conditions;
    private HashMap<String, Integer> levels;
    /**
     * There are special names, that are not table names but are parsed as
     * tables. These names are collected here and are not included in the tables
     * - names anymore.
     */
    private List<String> otherItemNames;

    private void init() {
        otherItemNames = new ArrayList<String>();
        tables = new ArrayList<QryTable>();
        columns = new ArrayList<Attribute>();
        conditions = new ArrayList<Condition>();
        levels = new HashMap<String, Integer>();
    }

    public ArrayList<Attribute> getAttributesForTable(String tablename, String schemaName) {
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
    }

    private String getNextSubLevel(String currLevel) {

        String splitArr[] = currLevel.split("\\.");
        Integer intLevel = splitArr.length;
        Integer maxSubCnt = 0;

        maxSubCnt = levels.get(currLevel);
        if (maxSubCnt == null) { // No key in hashmap
            maxSubCnt = 1;
        } else {
            maxSubCnt++;
        }

        // Now create the new level key for usage
        String retNewLevel = currLevel + "." + maxSubCnt.toString();
        levels.put(currLevel, maxSubCnt);

        return retNewLevel;
    }

    private String getNextSiblingLevel(String currLevel) {
        // TODO
        return "null";
    }

    /**
     * Main entry for this Tool class. A list of found tables is returned.
     *
     * @param delete
     * @return
     */
    public List<String> getTableList(Delete delete, String level) {
        init();
        otherItemNames.add(delete.getTable().getName());
        if (delete.getWhere() != null) {
            delete.getWhere().accept(this, level);
        }

        return otherItemNames;
    }

    /**
     * Main entry for this Tool class. A list of found tables is returned.
     *
     * @param insert
     * @return
     */
    public List<String> getTableList(Insert insert, String level) {
        init();
        otherItemNames.add(insert.getTable().getName());
        if (insert.getItemsList() != null) {
            insert.getItemsList().accept(this, level);
        }

        return otherItemNames;
    }

    /**
     * Main entry for this Tool class. A list of found tables is returned.
     *
     * @param replace
     * @return
     */
    public List<String> getTableList(Replace replace, String level) {
        init();
        otherItemNames.add(replace.getTable().getName());
        if (replace.getExpressions() != null) {
            for (Expression expression : replace.getExpressions()) {
                expression.accept(this, level);
            }
        }
        if (replace.getItemsList() != null) {
            replace.getItemsList().accept(this, level);
        }

        return otherItemNames;
    }

    /**
     * Main entry for this Tool class. A list of found tables is returned.
     *
     * @param select
     * @return
     */
    public void removeWhereExpressions(Select select, String level) {
        init();
        levels.put("0", 1); // Add root element for first level
        if (select.getWithItemsList() != null) {
            for (WithItem withItem : select.getWithItemsList()) {
                withItem.accept(this, level);
            }
        }
        select.getSelectBody().accept(this, level);
    }

    public void removeWhereExpressions(Insert insert, String level) {
        try {
            init();
            levels.put("0", 1); // Add root element for first level
            if (insert.getSelect().getWithItemsList() != null) {
                for (WithItem withItem : insert.getSelect().getWithItemsList()) {
                    withItem.accept(this, level);
                }
            }
            insert.getSelect().getSelectBody().accept(this, level);
        } catch (Exception e) {
            e = e;
        }
    }
    /**
     * Main entry for this Tool class. A list of found tables is returned.
     *
     * @param update
     * @return
     */
    public List<String> getSemantics(Update update, String level) {
        init();
        for (Table table : update.getTables()) {
            otherItemNames.add(table.getName());
        }
        if (update.getExpressions() != null) {
            for (Expression expression : update.getExpressions()) {
                expression.accept(this, level);
            }
        }

        if (update.getFromItem() != null) {
            update.getFromItem().accept(this, level);
        }

        if (update.getJoins() != null) {
            for (Join join : update.getJoins()) {
                join.getRightItem().accept(this, level);
            }
        }

        if (update.getWhere() != null) {
            update.getWhere().accept(this, level);
        }

        return otherItemNames;
    }

    @Override
    public void visit(SelectExpressionItem item, String level) {
        String alias = null;
        if (item.getAlias() != null) {
            alias = item.getAlias().getName();
        }
        // Not a good habit but I have to send the Alias down to the Column level
        // Concat with level with a tilde ~
        if (alias != null) {
            item.getExpression().accept(this, level + "~" + alias);
        } else {
            item.getExpression().accept(this, level);
        }
    }

    @Override
    public void visit(WithItem withItem, String level) {
        otherItemNames.add(withItem.getName().toLowerCase());
        withItem.getSelectBody().accept(this, level);
    }

    @Override
    public void visit(PlainSelect plainSelect, String level) {
        if (plainSelect.getSelectItems() != null) {
            for (SelectItem item : plainSelect.getSelectItems()) {
                item.accept(this, level);
            }
        }

        plainSelect.getFromItem().accept(this, level);

        if (plainSelect.getJoins() != null) {
            for (Join join : plainSelect.getJoins()) {
                join.getRightItem().accept(this, level);
                if (join.getOnExpression() != null) {
                    join.getOnExpression().accept(this, level);
                }
            }
        }
        if (plainSelect.getWhere() != null) {
            plainSelect.getWhere().accept(this, level);
        }
        if (plainSelect.getOracleHierarchical() != null) {
            plainSelect.getOracleHierarchical().accept(this, level);
        }
        if (plainSelect.getGroupByColumnReferences() != null) {
            List<Expression> groupByCols = plainSelect.getGroupByColumnReferences();
            for (int i = 0; i < groupByCols.size(); i++) {
                groupByCols.get(i).accept(this, level);
            }
        }
        if (plainSelect.getHaving() != null) {
            plainSelect.getHaving().accept(this, level);
        }
    }

    @Override
    public void visit(Table tableName, String level) {
        try {
            //String tableWholeName = tableName.getFullyQualifiedName() + ':' + tableName.getAlias().toString().trim();
            QryTable table = new QryTable();
            table.database = tableName.getDatabase().toString();
            table.schema = tableName.getSchemaName();
            table.tablename = tableName.getName();
            if (tableName.getAlias() != null) {
                table.alias = tableName.getAlias().toString().trim();
            }
            table.level = level;
            tables.add(table);
        } catch (Exception e) {
            log.error("Error in adding Table in NF:" + tableName.toString() + ": Error Msg:" + e.toString());
        }
    }


    @Override
    public void visit(Addition addition, String level) {
        visitBinaryExpression(addition, level);
    }

    @Override
    public void visit(AndExpression andExpression, String level) {
        visitBinaryExpression(andExpression, level);
    }

    @Override
    public void visit(Between between, String level) {
        between.getLeftExpression().accept(this, level);
        between.getBetweenExpressionStart().accept(this, level);
        between.getBetweenExpressionEnd().accept(this, level);
    }

    @Override
    public void visit(SubSelect subSelect, String level) {
        String nextLevelkey = getNextSubLevel(level);
        subSelect.getSelectBody().accept(this, nextLevelkey);
    }

    @Override
    public void visit(Column tableColumn, String level) {
        try {
            String splitArr[] = null;
            String extractedLevel = level;
            String alias = null;
            if (level.contains("~")) {
                splitArr = level.split("~");
                extractedLevel = splitArr[0];
                alias = splitArr[1];
            }
            // Add the column in the
            Attribute column = new Attribute();
            column.name = tableColumn.getColumnName();
            column.tableName = tableColumn.getTable().getName();
            column.schema = tableColumn.getTable().getSchemaName();
            column.nameFQN = tableColumn.getFullyQualifiedName();
//            column.level = extractedLevel;
            column.alias = alias;
            columns.add(column);
        } catch (Exception e) {
            log.error("Error in extracting alias for Column:" + tableColumn.getFullyQualifiedName());
        }
    }

    public void visitBinaryExpression(BinaryExpression binaryExpression, String level) {
        Condition leftCondition = null, rightCondition = null;
        try {
            Condition condition = extractCondition(binaryExpression, level);
        } catch (Exception e) {
            log.debug("Do nothing, not a leaf expression" + binaryExpression.toString() + e.toString());
        }
        try {
            leftCondition = extractCondition(binaryExpression.getLeftExpression(), level);
        } catch (Exception e) {
            log.debug("left extractCondition expression:" + this.toString() + " exception:" + e.toString());
        }
        try {
            rightCondition = extractCondition(binaryExpression.getRightExpression(), level);
        } catch (Exception e) {
            log.debug("right extractCondition expression:" + this.toString() + " exception:" + e.toString());
        }
        // Now visit the rest of the tree
        //if (leftCondition == null ){
        binaryExpression.getLeftExpression().accept(this, level);
        //}
        //if (rightCondition == null) {
        binaryExpression.getRightExpression().accept(this, level);
        //}
    }

    private Condition extractCondition(Expression currExpression, String level) {

        try {
            String expressionClass = currExpression.getClass().toString();

            if (expressionClass.endsWith(".Column") || expressionClass.endsWith(".Function") || expressionClass.endsWith("Value")
                    || expressionClass.endsWith(".SubSelect")) {
                return null;
            }
            Condition condition = new Condition();
            condition.level = level;

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
                        //((Column) binaryExpression.getLeftExpression()).accept(this, level);

                    }
                } catch (Exception e) {
                    condition.isJoin = false;
                    //condition.leftValue = binaryExpression.getLeftExpression().toString();
                    // Try to extract the value of condition to see if its a complex expression or LeafLevelValue
                    condition.leftValue = ((LongValue) binaryExpression.getLeftExpression()).getStringValue();

                    // Remove the Left Value and set to empty String to create the Abstract Syntax Tree
                    StringValue sv = new StringValue();
                    sv.setValue("");
                    binaryExpression.setLeftExpression((Expression) sv);
                }

                try {
                    if (((Column) (binaryExpression).getRightExpression()).getColumnName() != null) {
                        condition.rightTable = ((Column) binaryExpression.getRightExpression()).getTable().toString();
                        condition.rightColumn = ((Column) binaryExpression.getRightExpression()).getColumnName();
                        // ((Column) binaryExpression.getRightExpression()).accept(this, level);
                    }
                } catch (Exception e) {
                    condition.isJoin = false;
                    condition.rightValue = binaryExpression.getRightExpression().toString();
                    // Remove the Right Value and set to empty String to create the Abstract Syntax Tree
                    StringValue sv = new StringValue();
                    sv.setValue("");
                    binaryExpression.setRightExpression((Expression) sv);
                }
                if (condition.operator.equals("-")) { // This is projection expression donot add it to conditions
                    condition.operator = condition.operator;
                } else {
                    conditions.add(condition);
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
                        conditions.add(condition);
                    } else {
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
    public void visit(WhenClause whenClause, String level) {
    }

    @Override
    public void visit(AllComparisonExpression allComparisonExpression, String level) {
        allComparisonExpression.getSubSelect().getSelectBody().accept(this, level);
    }

    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression, String level) {
        anyComparisonExpression.getSubSelect().getSelectBody().accept(this, level);
    }

    @Override
    public void visit(SubJoin subjoin, String level) {
        subjoin.getLeft().accept(this, level);
        subjoin.getJoin().getRightItem().accept(this, level);
    }

    @Override
    public void visit(Division division, String level) {
        visitBinaryExpression(division, level);
    }

    @Override
    public void visit(DoubleValue doubleValue, String level) {
    }

    @Override
    public void visit(EqualsTo equalsTo, String level) {
        visitBinaryExpression(equalsTo, level);
    }

    @Override
    public void visit(Function function, String level) {
        String origLevel = level;
        // 17-June-2015 Muji - Extract columns from the function
        try {
            if (level.contains("~")) {
                // Function call doesnt require Alias assignment to column
                // Strip out the alias from the level field, and sanitize it
                String strArr[] = level.split("~");
                level = strArr[0];
            }
            ExpressionList parameters = function.getParameters();
            if (parameters != null) {
                for (int i = 0; i < parameters.getExpressions().size(); i++) {
                    Expression param = parameters.getExpressions().get(i);

                    // Check if expression is BinaryExpress (i.e. has left and right expression
                    try {
                        BinaryExpression bExp = (BinaryExpression) param;
                        if ((bExp.getLeftExpression() != null) && (bExp.getRightExpression() != null)) {
                            bExp.getLeftExpression().accept(this, origLevel);
                            bExp.getRightExpression().accept(this, origLevel);
                            continue;
                        }
                    } catch (Exception e) {

                    }
                    // Check for nested function inside the parameter if found then call visit nested
                    try {
                        Function nestedFunc = (Function) param;
                        if (nestedFunc.getParameters() != null) {
                            visit(nestedFunc, origLevel);
                            continue;
                        }
                    } catch (Exception e) {
                    }

                    Attribute column = new Attribute();
                    try {
                        column.name = ((Column) param).getColumnName();
//                        column.level = level;
                        try {
                            column.tableName = ((Column) param).getTable().getName();
                            column.nameFQN = ((Column) param).getTable().getFullyQualifiedName();
                        } catch (Exception e) {
                        }
                        columns.add(column);
                    } catch (Exception e) {

                    }
                }
            }
        } catch (Exception e) {
            log.error(e.toString());
        }
    }

    @Override
    public void visit(GreaterThan greaterThan, String level) {
        visitBinaryExpression(greaterThan, level);
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals, String level) {
        visitBinaryExpression(greaterThanEquals, level);
    }

    @Override
    public void visit(InExpression inExpression, String level) {
        inExpression.getLeftExpression().accept(this, level);
        inExpression.getRightItemsList().accept(this, level);
    }

    @Override
    public void visit(SignedExpression signedExpression, String level) {
        signedExpression.getExpression().accept(this, level);
    }

    @Override
    public void visit(LikeExpression likeExpression, String level) {
        visitBinaryExpression(likeExpression, level);
    }

    @Override
    public void visit(ExistsExpression existsExpression, String level) {
        existsExpression.getRightExpression().accept(this, level);
    }

    @Override
    public void visit(IsNullExpression isNullExpression, String level) {
    }

    @Override
    public void visit(JdbcParameter jdbcParameter, String level) {
    }


    @Override
    public void visit(LongValue longValue, String level) {
    }

    @Override
    public void visit(MinorThan minorThan, String level) {
        visitBinaryExpression(minorThan, level);
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals, String level) {
        visitBinaryExpression(minorThanEquals, level);
    }

    @Override
    public void visit(Multiplication multiplication, String level) {
        visitBinaryExpression(multiplication, level);
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo, String level) {
        visitBinaryExpression(notEqualsTo, level);
    }

    @Override
    public void visit(NullValue nullValue, String level) {
    }

    @Override
    public void visit(OrExpression orExpression, String level) {
        visitBinaryExpression(orExpression, level);
    }

    @Override
    public void visit(Parenthesis parenthesis, String level) {
        parenthesis.getExpression().accept(this, level);
    }

    @Override
    public void visit(StringValue stringValue, String level) {
    }

    @Override
    public void visit(Subtraction subtraction, String level) {
        visitBinaryExpression(subtraction, level);
    }


    @Override
    public void visit(ExpressionList expressionList, String level) {
        for (Expression expression : expressionList.getExpressions()) {
            expression.accept(this, level);
        }

    }

    @Override
    public void visit(DateValue dateValue, String level) {
    }

    @Override
    public void visit(TimestampValue timestampValue, String level) {
    }

    @Override
    public void visit(TimeValue timeValue, String level) {
    }

    @Override
    public void visit(CaseExpression caseExpression, String level) {
    }


    @Override
    public void visit(Concat concat, String level) {
        visitBinaryExpression(concat, level);
    }

    @Override
    public void visit(Matches matches, String level) {
        visitBinaryExpression(matches, level);
    }

    @Override
    public void visit(BitwiseAnd bitwiseAnd, String level) {
        visitBinaryExpression(bitwiseAnd, level);
    }

    @Override
    public void visit(BitwiseOr bitwiseOr, String level) {
        visitBinaryExpression(bitwiseOr, level);
    }

    @Override
    public void visit(BitwiseXor bitwiseXor, String level) {
        visitBinaryExpression(bitwiseXor, level);
    }

    @Override
    public void visit(CastExpression cast, String level) {
        cast.getLeftExpression().accept(this, level);
    }

    @Override
    public void visit(Modulo modulo, String level) {
        visitBinaryExpression(modulo, level);
    }

    @Override
    public void visit(AnalyticExpression analytic, String level) {
    }

    @Override
    public void visit(SetOperationList list, String level) {
        for (PlainSelect plainSelect : list.getPlainSelects()) {
            visit(plainSelect, level);
        }
    }

    @Override
    public void visit(ExtractExpression eexpr, String level) {
    }

    @Override
    public void visit(LateralSubSelect lateralSubSelect, String level) {
        lateralSubSelect.getSubSelect().getSelectBody().accept(this, level);
    }

    @Override
    public void visit(MultiExpressionList multiExprList, String level) {
        for (ExpressionList exprList : multiExprList.getExprList()) {
            exprList.accept(this, level);
        }
    }

    @Override
    public void visit(ValuesList valuesList, String level) {
    }


    @Override
    public void visit(IntervalExpression iexpr, String level) {
    }

    @Override
    public void visit(JdbcNamedParameter jdbcNamedParameter, String level) {
    }

    @Override
    public void visit(OracleHierarchicalExpression oexpr, String level) {
        if (oexpr.getStartExpression() != null) {
            oexpr.getStartExpression().accept(this, level);
        }

        if (oexpr.getConnectExpression() != null) {
            oexpr.getConnectExpression().accept(this, level);
        }
    }

    @Override
    public void visit(RegExpMatchOperator rexpr, String level) {
        visitBinaryExpression(rexpr, level);
    }

    @Override
    public void visit(RegExpMySQLOperator rexpr, String level) {
        visitBinaryExpression(rexpr, level);
    }

    @Override
    public void visit(JsonExpression jsonExpr, String level) {
    }

    @Override
    public void visit(AllColumns allColumns, String level) {
    }

    @Override
    public void visit(AllTableColumns allTableColumns, String level) {
    }


    @Override
    public void visit(WithinGroupExpression wgexpr, String level) {
    }

    @Override
    public void visit(UserVariable var, String level) {
    }
}
