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
package com.haystack.parser.statement.select;

/**
 * All the columns of a table (as in "SELECT TableName.* FROM ...")
 */
import com.haystack.parser.schema.Table;

public class AllTableColumns implements SelectItem {

	private Table table;

	public AllTableColumns() {
	}

	public AllTableColumns(Table tableName) {
		this.table = tableName;
	}

	public Table getTable() {
		return table;
	}

	public void setTable(Table table) {
		this.table = table;
	}

	@Override
	public void accept(SelectItemVisitor selectItemVisitor, String level) {
		selectItemVisitor.visit(this, level);
	}

	@Override
	public String toString() {
		return table + ".*";
	}
}
