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
package com.haystack.parser.util.deparser;

import com.haystack.parser.expression.*;
import com.haystack.parser.expression.arithmetic.*;
import com.haystack.parser.expression.conditional.AndExpression;
import com.haystack.parser.expression.conditional.OrExpression;
import com.haystack.parser.expression.relational.*;
import com.haystack.parser.schema.Column;
import com.haystack.parser.schema.Table;
import com.haystack.parser.statement.select.SelectVisitor;
import com.haystack.parser.statement.select.SubSelect;

import java.util.Iterator;

/**
 * A class to de-parse (that is, tranform from JSqlParser hierarchy into a
 * string) an {@link com.haystack.parser.expression.Expression}
 */
public class ExpressionDeParser implements ExpressionVisitor, ItemsListVisitor {

    private StringBuilder buffer;
    private SelectVisitor selectVisitor;
    private boolean useBracketsInExprList = true;

    public ExpressionDeParser() {
    }

    /**
     * @param selectVisitor a SelectVisitor to de-parse SubSelects. It has to
     * share the same<br> StringBuilder as this object in order to work, as:
     *
     * <pre>
     * <code>
     * StringBuilder myBuf = new StringBuilder();
     * MySelectDeparser selectDeparser = new  MySelectDeparser();
     * selectDeparser.setBuffer(myBuf);
     * ExpressionDeParser expressionDeParser = new ExpressionDeParser(selectDeparser, myBuf);
     * </code>
     * </pre>
     *
     * @param buffer the buffer that will be filled with the expression
     */
    public ExpressionDeParser(SelectVisitor selectVisitor, StringBuilder buffer) {
        this.selectVisitor = selectVisitor;
        this.buffer = buffer;
    }

    public StringBuilder getBuffer() {
        return buffer;
    }

    public void setBuffer(StringBuilder buffer) {
        this.buffer = buffer;
    }

    @Override
    public void visit(Addition addition, String level) {
        visitBinaryExpression(addition, " + ", level);
    }

    @Override
    public void visit(AndExpression andExpression, String level) {
        visitBinaryExpression(andExpression, " AND ", level);
    }

    @Override
    public void visit(Between between, String level) {
        between.getLeftExpression().accept(this, level);
        if (between.isNot()) {
            buffer.append(" NOT");
        }

        buffer.append(" BETWEEN ");
        between.getBetweenExpressionStart().accept(this, level);
        buffer.append(" AND ");
        between.getBetweenExpressionEnd().accept(this, level);

    }

    @Override
    public void visit(EqualsTo equalsTo, String level) {
        visitOldOracleJoinBinaryExpression(equalsTo, " = ",level);
    }

    @Override
    public void visit(Division division, String level) {
        visitBinaryExpression(division, " / ", level);

    }

    @Override
    public void visit(DoubleValue doubleValue, String level) {
        buffer.append(doubleValue.toString());

    }

    public void visitOldOracleJoinBinaryExpression(OldOracleJoinBinaryExpression expression, String operator, String level) {
        if (expression.isNot()) {
            buffer.append(" NOT ");
        }
        expression.getLeftExpression().accept(this, level);
        if (expression.getOldOracleJoinSyntax() == EqualsTo.ORACLE_JOIN_RIGHT) {
            buffer.append("(+)");
        }
        buffer.append(operator);
        expression.getRightExpression().accept(this, level);
        if (expression.getOldOracleJoinSyntax() == EqualsTo.ORACLE_JOIN_LEFT) {
            buffer.append("(+)");
        }
    }

    @Override
    public void visit(GreaterThan greaterThan, String level) {
        visitOldOracleJoinBinaryExpression(greaterThan, " > ", level);
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals, String level) {
        visitOldOracleJoinBinaryExpression(greaterThanEquals, " >= ", level);

    }

    @Override
    public void visit(InExpression inExpression, String level) {
        if (inExpression.getLeftExpression() == null) {
            inExpression.getLeftItemsList().accept(this, level);
        } else {
            inExpression.getLeftExpression().accept(this, level);
            if (inExpression.getOldOracleJoinSyntax() == SupportsOldOracleJoinSyntax.ORACLE_JOIN_RIGHT) {
                buffer.append("(+)");
            }
        }
        if (inExpression.isNot()) {
            buffer.append(" NOT");
        }
        buffer.append(" IN ");

        inExpression.getRightItemsList().accept(this, level);
    }

    @Override
    public void visit(SignedExpression signedExpression, String level) {
        buffer.append(signedExpression.getSign());
        signedExpression.getExpression().accept(this, level);
    }

    @Override
    public void visit(IsNullExpression isNullExpression, String level) {
        isNullExpression.getLeftExpression().accept(this, level);
        if (isNullExpression.isNot()) {
            buffer.append(" IS NOT NULL");
        } else {
            buffer.append(" IS NULL");
        }
    }

    @Override
    public void visit(JdbcParameter jdbcParameter, String level) {
        buffer.append("?");

    }

    @Override
    public void visit(LikeExpression likeExpression, String level) {
        visitBinaryExpression(likeExpression, " LIKE ", level);
        String escape = likeExpression.getEscape();
        if (escape != null) {
            buffer.append(" ESCAPE '").append(escape).append('\'');
        }
    }

    @Override
    public void visit(ExistsExpression existsExpression, String level) {
        if (existsExpression.isNot()) {
            buffer.append("NOT EXISTS ");
        } else {
            buffer.append("EXISTS ");
        }
        existsExpression.getRightExpression().accept(this, level);
    }

    @Override
    public void visit(LongValue longValue, String level) {
        buffer.append(longValue.getStringValue());

    }

    @Override
    public void visit(MinorThan minorThan, String level) {
        visitOldOracleJoinBinaryExpression(minorThan, " < ", "0");

    }

    @Override
    public void visit(MinorThanEquals minorThanEquals, String level) {
        visitOldOracleJoinBinaryExpression(minorThanEquals, " <= ", level);

    }

    @Override
    public void visit(Multiplication multiplication, String level) {
        visitBinaryExpression(multiplication, " * ", level);

    }

    @Override
    public void visit(NotEqualsTo notEqualsTo, String level) {
        visitOldOracleJoinBinaryExpression(notEqualsTo, " " + notEqualsTo.getStringExpression() + " ", level);

    }

    @Override
    public void visit(NullValue nullValue, String level) {
        buffer.append("NULL");

    }

    @Override
    public void visit(OrExpression orExpression, String level) {
        visitBinaryExpression(orExpression, " OR ", level);

    }

    @Override
    public void visit(Parenthesis parenthesis, String level) {
        if (parenthesis.isNot()) {
            buffer.append(" NOT ");
        }

        buffer.append("(");
        parenthesis.getExpression().accept(this, level);
        buffer.append(")");

    }

    @Override
    public void visit(StringValue stringValue, String level) {
        buffer.append("'").append(stringValue.getValue()).append("'");

    }

    @Override
    public void visit(Subtraction subtraction, String level) {
        visitBinaryExpression(subtraction, " - ", level);

    }

    private void visitBinaryExpression(BinaryExpression binaryExpression, String operator, String level) {
        if (binaryExpression.isNot()) {
            buffer.append(" NOT ");
        }
        binaryExpression.getLeftExpression().accept(this, level);
        buffer.append(operator);
        binaryExpression.getRightExpression().accept(this, level);

    }

    @Override
    public void visit(SubSelect subSelect, String level) {
        buffer.append("(");
        subSelect.getSelectBody().accept(selectVisitor, level);
        buffer.append(")");
    }

    @Override
    public void visit(Column tableColumn, String level) {
        final Table table = tableColumn.getTable();
        String tableName = null;
        if (table != null) {
            if (table.getAlias() != null) {
                tableName = table.getAlias().getName();
            } else {
                tableName = table.getFullyQualifiedName();
            }
        }
        if (tableName != null && !tableName.isEmpty()) {
            buffer.append(tableName).append(".");
        }

        buffer.append(tableColumn.getColumnName());
    }

    @Override
    public void visit(Function function, String level) {
        if (function.isEscaped()) {
            buffer.append("{fn ");
        }

        buffer.append(function.getName());
        if (function.isAllColumns() && function.getParameters() == null) {
            buffer.append("(*)");
        } else if (function.getParameters() == null) {
            buffer.append("()");
        } else {
            boolean oldUseBracketsInExprList = useBracketsInExprList;
            if (function.isDistinct()) {
                useBracketsInExprList = false;
                buffer.append("(DISTINCT ");
            } else if (function.isAllColumns()) {
                useBracketsInExprList = false;
                buffer.append("(ALL ");
            }
            visit(function.getParameters(), level);
            useBracketsInExprList = oldUseBracketsInExprList;
            if (function.isDistinct() || function.isAllColumns()) {
                buffer.append(")");
            }
        }

        if (function.getAttribute() != null) {
            buffer.append(".").append(function.getAttribute());
        }

        if (function.isEscaped()) {
            buffer.append("}");
        }
    }

    @Override
    public void visit(ExpressionList expressionList, String level) {
        if (useBracketsInExprList) {
            buffer.append("(");
        }
        for (Iterator<Expression> iter = expressionList.getExpressions().iterator(); iter.hasNext();) {
            Expression expression = iter.next();
            expression.accept(this, level);
            if (iter.hasNext()) {
                buffer.append(", ");
            }
        }
        if (useBracketsInExprList) {
            buffer.append(")");
        }
    }

    public SelectVisitor getSelectVisitor() {
        return selectVisitor;
    }

    public void setSelectVisitor(SelectVisitor visitor) {
        selectVisitor = visitor;
    }

    @Override
    public void visit(DateValue dateValue, String level) {
        buffer.append("{d '").append(dateValue.getValue().toString()).append("'}");
    }

    @Override
    public void visit(TimestampValue timestampValue, String level) {
        buffer.append("{ts '").append(timestampValue.getValue().toString()).append("'}");
    }

    @Override
    public void visit(TimeValue timeValue, String level) {
        buffer.append("{t '").append(timeValue.getValue().toString()).append("'}");
    }

    @Override
    public void visit(CaseExpression caseExpression, String level) {
        buffer.append("CASE ");
        Expression switchExp = caseExpression.getSwitchExpression();
        if (switchExp != null) {
            switchExp.accept(this, level);
            buffer.append(" ");
        }

        for (Expression exp : caseExpression.getWhenClauses()) {
            exp.accept(this, level);
        }

        Expression elseExp = caseExpression.getElseExpression();
        if (elseExp != null) {
            buffer.append("ELSE ");
            elseExp.accept(this, level);
            buffer.append(" ");
        }

        buffer.append("END");
    }

    @Override
    public void visit(WhenClause whenClause, String level) {
        buffer.append("WHEN ");
        whenClause.getWhenExpression().accept(this, level);
        buffer.append(" THEN ");
        whenClause.getThenExpression().accept(this, level);
        buffer.append(" ");
    }

    @Override
    public void visit(AllComparisonExpression allComparisonExpression, String level) {
        buffer.append(" ALL ");
        allComparisonExpression.getSubSelect().accept((ExpressionVisitor) this, level);
    }

    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression, String level) {
        buffer.append(" ANY ");
        anyComparisonExpression.getSubSelect().accept((ExpressionVisitor) this, level);
    }

    @Override
    public void visit(Concat concat, String level) {
        visitBinaryExpression(concat, " || ", level);
    }

    @Override
    public void visit(Matches matches, String level) {
        visitOldOracleJoinBinaryExpression(matches, " @@ ", level);
    }

    @Override
    public void visit(BitwiseAnd bitwiseAnd, String level) {
        visitBinaryExpression(bitwiseAnd, " & ", level);
    }

    @Override
    public void visit(BitwiseOr bitwiseOr, String level) {
        visitBinaryExpression(bitwiseOr, " | ", level);
    }

    @Override
    public void visit(BitwiseXor bitwiseXor, String level) {
        visitBinaryExpression(bitwiseXor, " ^ ", level);
    }

    @Override
    public void visit(CastExpression cast, String level) {
        if (cast.isUseCastKeyword()) {
            buffer.append("CAST(");
            buffer.append(cast.getLeftExpression());
            buffer.append(" AS ");
            buffer.append(cast.getType());
            buffer.append(")");
        } else {
            buffer.append(cast.getLeftExpression());
            buffer.append("::");
            buffer.append(cast.getType());
        }
    }

    @Override
    public void visit(Modulo modulo, String level) {
        visitBinaryExpression(modulo, " % ", level);
    }

    @Override
    public void visit(AnalyticExpression aexpr, String level) {
        buffer.append(aexpr.toString());
    }

    @Override
    public void visit(ExtractExpression eexpr, String level) {
        buffer.append(eexpr.toString());
    }

    @Override
    public void visit(MultiExpressionList multiExprList, String level) {
        for (Iterator<ExpressionList> it = multiExprList.getExprList().iterator(); it.hasNext();) {
            it.next().accept(this, level);
            if (it.hasNext()) {
                buffer.append(", ");
            }
        }
    }

    @Override
    public void visit(IntervalExpression iexpr , String level) {
        buffer.append(iexpr.toString());
    }

    @Override
    public void visit(JdbcNamedParameter jdbcNamedParameter, String level) {
        buffer.append(jdbcNamedParameter.toString());
    }

    @Override
    public void visit(OracleHierarchicalExpression oexpr, String level) {
        buffer.append(oexpr.toString());
    }

    @Override
    public void visit(RegExpMatchOperator rexpr, String level) {
        visitBinaryExpression(rexpr, " " + rexpr.getStringExpression() + " ", level);
    }

    @Override
    public void visit(RegExpMySQLOperator rexpr, String level) {
        visitBinaryExpression(rexpr, " " + rexpr.getStringExpression() + " ", level);
    }

    @Override
    public void visit(JsonExpression jsonExpr, String level) {
        buffer.append(jsonExpr.toString());
    }

    @Override
    public void visit(WithinGroupExpression wgexpr, String level) {
        buffer.append(wgexpr.toString());
    }

    @Override
    public void visit(UserVariable var, String level) {
        buffer.append(var.toString());
    }

}
