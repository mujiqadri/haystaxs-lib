/*
 * #%L
 * JSQLParser library
 * %%
 * Copyright (C) 2004 - 2014 JSQLParser
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
package com.haystack.parser.statement.execute;

import com.haystack.parser.expression.relational.ExpressionList;
import com.haystack.parser.statement.Statement;
import com.haystack.parser.statement.StatementVisitor;
import com.haystack.parser.statement.select.PlainSelect;

/**
 *
 * @author toben
 */
public class Execute implements Statement {

    private String name;
    private ExpressionList exprList;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ExpressionList getExprList() {
        return exprList;
    }

    public void setExprList(ExpressionList exprList) {
        this.exprList = exprList;
    }
    
    @Override
    public void accept(StatementVisitor statementVisitor, String level) {
        statementVisitor.visit(this, level);
    }

    @Override
    public String toString() {
        return "EXECUTE " + name + " " + PlainSelect.getStringList(exprList.getExpressions(), true, false);
    }

}
