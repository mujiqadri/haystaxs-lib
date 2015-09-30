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
package com.haystack.parser.statement.delete;

import com.haystack.parser.expression.Expression;
import com.haystack.parser.schema.Table;
import com.haystack.parser.statement.Statement;
import com.haystack.parser.statement.StatementVisitor;

public class Delete implements Statement {

	private Table table;
	private Expression where;

	@Override
	public void accept(StatementVisitor statementVisitor, String level) {
		statementVisitor.visit(this, level);
	}

	public Table getTable() {
		return table;
	}

	public Expression getWhere() {
		return where;
	}

	public void setTable(Table name) {
		table = name;
	}

	public void setWhere(Expression expression) {
		where = expression;
	}

	@Override
	public String toString() {
		return "DELETE FROM " + table + ((where != null) ? " WHERE " + where : "");
	}
}
