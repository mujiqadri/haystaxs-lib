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
package com.haystack.parser.statement;

import com.haystack.parser.statement.alter.Alter;
import com.haystack.parser.statement.create.index.CreateIndex;
import com.haystack.parser.statement.create.table.CreateTable;
import com.haystack.parser.statement.create.view.CreateView;
import com.haystack.parser.statement.delete.Delete;
import com.haystack.parser.statement.drop.Drop;
import com.haystack.parser.statement.execute.Execute;
import com.haystack.parser.statement.insert.Insert;
import com.haystack.parser.statement.replace.Replace;
import com.haystack.parser.statement.select.Select;
import com.haystack.parser.statement.truncate.Truncate;
import com.haystack.parser.statement.update.Update;

public class StatementVisitorAdapter implements StatementVisitor {
    @Override
    public void visit(Select select, String level) {

    }

    @Override
    public void visit(Delete delete, String level) {

    }

    @Override
    public void visit(Update update, String level) {

    }

    @Override
    public void visit(Insert insert, String level) {

    }

    @Override
    public void visit(Replace replace, String level) {

    }

    @Override
    public void visit(Drop drop, String level) {

    }

    @Override
    public void visit(Truncate truncate, String level) {

    }

    @Override
    public void visit(CreateIndex createIndex, String level) {

    }

    @Override
    public void visit(CreateTable createTable, String level) {

    }

    @Override
    public void visit(CreateView createView, String level) {

    }

    @Override
    public void visit(Alter alter, String level) {

    }

    @Override
    public void visit(Statements stmts, String level) {

    }

    @Override
    public void visit(Execute execute, String level) {
        
    }
}
