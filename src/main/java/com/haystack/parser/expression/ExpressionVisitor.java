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

import com.haystack.parser.expression.arithmetic.Addition;
import com.haystack.parser.expression.arithmetic.BitwiseAnd;
import com.haystack.parser.expression.arithmetic.BitwiseOr;
import com.haystack.parser.expression.arithmetic.BitwiseXor;
import com.haystack.parser.expression.arithmetic.Concat;
import com.haystack.parser.expression.arithmetic.Division;
import com.haystack.parser.expression.arithmetic.Modulo;
import com.haystack.parser.expression.arithmetic.Multiplication;
import com.haystack.parser.expression.arithmetic.Subtraction;
import com.haystack.parser.expression.conditional.AndExpression;
import com.haystack.parser.expression.conditional.OrExpression;
import com.haystack.parser.expression.relational.Between;
import com.haystack.parser.expression.relational.EqualsTo;
import com.haystack.parser.expression.relational.ExistsExpression;
import com.haystack.parser.expression.relational.GreaterThan;
import com.haystack.parser.expression.relational.GreaterThanEquals;
import com.haystack.parser.expression.relational.InExpression;
import com.haystack.parser.expression.relational.IsNullExpression;
import com.haystack.parser.expression.relational.LikeExpression;
import com.haystack.parser.expression.relational.Matches;
import com.haystack.parser.expression.relational.MinorThan;
import com.haystack.parser.expression.relational.MinorThanEquals;
import com.haystack.parser.expression.relational.NotEqualsTo;
import com.haystack.parser.expression.relational.RegExpMatchOperator;
import com.haystack.parser.expression.relational.RegExpMySQLOperator;
import com.haystack.parser.schema.Column;
import com.haystack.parser.statement.select.SubSelect;

public interface ExpressionVisitor {

	void visit(NullValue nullValue, String level);

	void visit(Function function, String level);

	void visit(SignedExpression signedExpression, String level);

	void visit(JdbcParameter jdbcParameter, String level);

    void visit(JdbcNamedParameter jdbcNamedParameter, String level);

	void visit(DoubleValue doubleValue, String level);

	void visit(LongValue longValue, String level);

	void visit(DateValue dateValue, String level);

	void visit(TimeValue timeValue, String level);

	void visit(TimestampValue timestampValue, String level);

	void visit(Parenthesis parenthesis, String level);

	void visit(StringValue stringValue, String level);

	void visit(Addition addition, String level);

	void visit(Division division, String level);

	void visit(Multiplication multiplication, String level);

	void visit(Subtraction subtraction, String level);

	void visit(AndExpression andExpression, String level);

	void visit(OrExpression orExpression, String level);

	void visit(Between between, String level);

	void visit(EqualsTo equalsTo, String level);

	void visit(GreaterThan greaterThan, String level);

	void visit(GreaterThanEquals greaterThanEquals, String level);

	void visit(InExpression inExpression, String level);

	void visit(IsNullExpression isNullExpression, String level);

	void visit(LikeExpression likeExpression, String level);

	void visit(MinorThan minorThan, String level);

	void visit(MinorThanEquals minorThanEquals, String level);

	void visit(NotEqualsTo notEqualsTo, String level);

	void visit(Column tableColumn, String level);

	void visit(SubSelect subSelect, String level);

	void visit(CaseExpression caseExpression, String level);

	void visit(WhenClause whenClause, String level);

	void visit(ExistsExpression existsExpression, String level);

	void visit(AllComparisonExpression allComparisonExpression, String level);

	void visit(AnyComparisonExpression anyComparisonExpression, String level);

	void visit(Concat concat, String level);

	void visit(Matches matches, String level);

	void visit(BitwiseAnd bitwiseAnd, String level);

	void visit(BitwiseOr bitwiseOr, String level);

	void visit(BitwiseXor bitwiseXor, String level);

	void visit(CastExpression cast, String level);

	void visit(Modulo modulo, String level);

	void visit(AnalyticExpression aexpr, String level);

    void visit(WithinGroupExpression wgexpr, String level);

	void visit(ExtractExpression eexpr, String level);

	void visit(IntervalExpression iexpr, String level);

	void visit(OracleHierarchicalExpression oexpr, String level);

	void visit(RegExpMatchOperator rexpr, String level);

    void visit(JsonExpression jsonExpr, String level);

	void visit(RegExpMySQLOperator regExpMySQLOperator, String level);

    void visit(UserVariable var, String level);
}
