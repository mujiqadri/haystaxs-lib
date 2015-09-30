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
package com.haystack.parser.expression;

import com.haystack.parser.expression.arithmetic.*;
import com.haystack.parser.expression.conditional.AndExpression;
import com.haystack.parser.expression.conditional.OrExpression;
import com.haystack.parser.expression.relational.*;
import com.haystack.parser.schema.Column;
import com.haystack.parser.statement.select.OrderByElement;
import com.haystack.parser.statement.select.SubSelect;

public class ExpressionVisitorAdapter implements ExpressionVisitor, ItemsListVisitor {
    @Override
    public void visit(NullValue value, String level) {

    }

    @Override
    public void visit(Function function, String level) {

    }

    @Override
    public void visit(SignedExpression expr, String level) {
        expr.getExpression().accept(this, level);
    }

    @Override
    public void visit(JdbcParameter parameter, String level) {

    }

    @Override
    public void visit(JdbcNamedParameter parameter, String level) {

    }

    @Override
    public void visit(DoubleValue value, String level) {

    }

    @Override
    public void visit(LongValue value, String level) {

    }

    @Override
    public void visit(DateValue value, String level) {

    }

    @Override
    public void visit(TimeValue value, String level) {

    }

    @Override
    public void visit(TimestampValue value, String level) {

    }

    @Override
    public void visit(Parenthesis parenthesis, String level) {
        parenthesis.getExpression().accept(this, level);
    }

    @Override
    public void visit(StringValue value, String level) {

    }

    @Override
    public void visit(Addition expr, String level) {
        visitBinaryExpression(expr, level);
    }

    @Override
    public void visit(Division expr, String level) {
        visitBinaryExpression(expr, level);
    }

    @Override
    public void visit(Multiplication expr, String level) {
        visitBinaryExpression(expr, level);
    }

    @Override
    public void visit(Subtraction expr, String level) {
        visitBinaryExpression(expr, level);
    }

    @Override
    public void visit(AndExpression expr, String level) {
        visitBinaryExpression(expr, level);
    }

    @Override
    public void visit(OrExpression expr, String level) {
        visitBinaryExpression(expr, level);
    }

    @Override
    public void visit(Between expr, String level) {
        expr.getLeftExpression().accept(this, level);
        expr.getBetweenExpressionStart().accept(this, level);
        expr.getBetweenExpressionEnd().accept(this, level);
    }

    @Override
    public void visit(EqualsTo expr, String level) {
        visitBinaryExpression(expr, level);
    }

    @Override
    public void visit(GreaterThan expr, String level) {
        visitBinaryExpression(expr, level);
    }

    @Override
    public void visit(GreaterThanEquals expr, String level) {
        visitBinaryExpression(expr, level);
    }

    @Override
    public void visit(InExpression expr, String level) {
        expr.getLeftExpression().accept(this, level);
        expr.getLeftItemsList().accept(this, level);
        expr.getRightItemsList().accept(this, level);
    }

    @Override
    public void visit(IsNullExpression expr, String level) {
        expr.getLeftExpression().accept(this, level);
    }

    @Override
    public void visit(LikeExpression expr, String level) {
        visitBinaryExpression(expr, level);
    }

    @Override
    public void visit(MinorThan expr, String level) {
        visitBinaryExpression(expr, level);
    }

    @Override
    public void visit(MinorThanEquals expr, String level) {
        visitBinaryExpression(expr, level);
    }

    @Override
    public void visit(NotEqualsTo expr, String level) {
        visitBinaryExpression(expr, level);
    }

    @Override
    public void visit(Column column, String level) {

    }

    @Override
    public void visit(SubSelect subSelect, String level) {
        
    }

    @Override
    public void visit(CaseExpression expr, String level) {
        expr.getSwitchExpression().accept(this, level);
        for (Expression x : expr.getWhenClauses()) {
            x.accept(this, level);
        }
        expr.getElseExpression().accept(this, level);
    }

    @Override
    public void visit(WhenClause expr, String level) {
        expr.getWhenExpression().accept(this, level);
        expr.getThenExpression().accept(this, level);
    }

    @Override
    public void visit(ExistsExpression expr, String level) {
        expr.getRightExpression().accept(this, level);
    }

    @Override
    public void visit(AllComparisonExpression expr, String level) {

    }

    @Override
    public void visit(AnyComparisonExpression expr, String level) {

    }

    @Override
    public void visit(Concat expr, String level) {
        visitBinaryExpression(expr, level);
    }

    @Override
    public void visit(Matches expr, String level) {
        visitBinaryExpression(expr, level);
    }

    @Override
    public void visit(BitwiseAnd expr, String level) {
        visitBinaryExpression(expr, level);
    }

    @Override
    public void visit(BitwiseOr expr, String level) {
        visitBinaryExpression(expr, level);
    }

    @Override
    public void visit(BitwiseXor expr, String level) {
        visitBinaryExpression(expr, level);
    }

    @Override
    public void visit(CastExpression expr, String level) {
        expr.getLeftExpression().accept(this, level);
    }

    @Override
    public void visit(Modulo expr, String level) {
        visitBinaryExpression(expr, level);
    }

    @Override
    public void visit(AnalyticExpression expr, String level) {
        expr.getExpression().accept(this, level);
        expr.getDefaultValue().accept(this, level);
        expr.getOffset().accept(this, level);
        for (OrderByElement element : expr.getOrderByElements()) {
            element.getExpression().accept(this, level);
        }

        expr.getWindowElement().getRange().getStart().getExpression().accept(this, level);
        expr.getWindowElement().getRange().getEnd().getExpression().accept(this, level);
        expr.getWindowElement().getOffset().getExpression().accept(this, level);
    }

    @Override
    public void visit(ExtractExpression expr, String level) {
        expr.getExpression().accept(this, level);
    }

    @Override
    public void visit(IntervalExpression expr, String level) {

    }

    @Override
    public void visit(OracleHierarchicalExpression expr, String level) {
        expr.getConnectExpression().accept(this, level);
        expr.getStartExpression().accept(this, level);
    }

    @Override
    public void visit(RegExpMatchOperator expr, String level) {
        visitBinaryExpression(expr, level);
    }

    @Override
    public void visit(ExpressionList expressionList, String level) {
        for (Expression expr : expressionList.getExpressions()) {
            expr.accept(this, level);
        }
    }

    @Override
    public void visit(MultiExpressionList multiExprList, String level) {
        for (ExpressionList list : multiExprList.getExprList()) {
            visit(list, level);
        }
    }

    protected void visitBinaryExpression(BinaryExpression expr, String level) {
        expr.getLeftExpression().accept(this, level);
        expr.getRightExpression().accept(this, level);
    }

    @Override
    public void visit(JsonExpression jsonExpr, String level) {
        visit(jsonExpr.getColumn(), level);
    }

	@Override
	public void visit(RegExpMySQLOperator expr, String level) {
		visitBinaryExpression(expr, level);	
	}

    @Override
    public void visit(WithinGroupExpression wgexpr, String level) {
        wgexpr.getExprList().accept(this, level);
        for (OrderByElement element : wgexpr.getOrderByElements()) {
            element.getExpression().accept(this, level);
        }
    }

    @Override
    public void visit(UserVariable var, String level) {
        
    }
}
