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

import java.util.Iterator;

import com.haystack.parser.statement.StatementVisitor;
import com.haystack.parser.statement.Statements;
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
import com.haystack.parser.statement.select.WithItem;
import com.haystack.parser.statement.truncate.Truncate;
import com.haystack.parser.statement.update.Update;

public class StatementDeParser implements StatementVisitor {

    private StringBuilder buffer;

    public StatementDeParser(StringBuilder buffer) {
        this.buffer = buffer;
    }

    @Override
    public void visit(CreateIndex createIndex, String level) {
        CreateIndexDeParser createIndexDeParser = new CreateIndexDeParser(buffer);
        createIndexDeParser.deParse(createIndex);
    }

    @Override
    public void visit(CreateTable createTable, String level) {
        CreateTableDeParser createTableDeParser = new CreateTableDeParser(buffer);
        createTableDeParser.deParse(createTable);
    }

    @Override
    public void visit(CreateView createView, String level) {
        CreateViewDeParser createViewDeParser = new CreateViewDeParser(buffer);
        createViewDeParser.deParse(createView);
    }

    @Override
    public void visit(Delete delete, String level) {
        SelectDeParser selectDeParser = new SelectDeParser();
        selectDeParser.setBuffer(buffer);
        ExpressionDeParser expressionDeParser = new ExpressionDeParser(selectDeParser, buffer);
        selectDeParser.setExpressionVisitor(expressionDeParser);
        DeleteDeParser deleteDeParser = new DeleteDeParser(expressionDeParser, buffer);
        deleteDeParser.deParse(delete);
    }

    @Override
    public void visit(Drop drop, String level) {
        // TODO Auto-generated method stub
    }

    @Override
    public void visit(Insert insert, String level) {
        SelectDeParser selectDeParser = new SelectDeParser();
        selectDeParser.setBuffer(buffer);
        ExpressionDeParser expressionDeParser = new ExpressionDeParser(selectDeParser, buffer);
        selectDeParser.setExpressionVisitor(expressionDeParser);
        InsertDeParser insertDeParser = new InsertDeParser(expressionDeParser, selectDeParser, buffer);
        insertDeParser.deParse(insert);

    }

    @Override
    public void visit(Replace replace, String level) {
        SelectDeParser selectDeParser = new SelectDeParser();
        selectDeParser.setBuffer(buffer);
        ExpressionDeParser expressionDeParser = new ExpressionDeParser(selectDeParser, buffer);
        selectDeParser.setExpressionVisitor(expressionDeParser);
        ReplaceDeParser replaceDeParser = new ReplaceDeParser(expressionDeParser, selectDeParser, buffer);
        replaceDeParser.deParse(replace);
    }

    @Override
    public void visit(Select select, String level) {
        SelectDeParser selectDeParser = new SelectDeParser();
        selectDeParser.setBuffer(buffer);
        ExpressionDeParser expressionDeParser = new ExpressionDeParser(selectDeParser, buffer);
        selectDeParser.setExpressionVisitor(expressionDeParser);
        if (select.getWithItemsList() != null && !select.getWithItemsList().isEmpty()) {
            buffer.append("WITH ");
            for (Iterator<WithItem> iter = select.getWithItemsList().iterator(); iter.hasNext();) {
                WithItem withItem = iter.next();
                withItem.accept(selectDeParser, level);
                if (iter.hasNext()) {
                    buffer.append(",");
                }
                buffer.append(" ");
            }
        }
        select.getSelectBody().accept(selectDeParser, level);
    }

    @Override
    public void visit(Truncate truncate, String level) {
    }

    @Override
    public void visit(Update update, String level) {
        SelectDeParser selectDeParser = new SelectDeParser();
        selectDeParser.setBuffer(buffer);
        ExpressionDeParser expressionDeParser = new ExpressionDeParser(selectDeParser, buffer);
        UpdateDeParser updateDeParser = new UpdateDeParser(expressionDeParser, selectDeParser, buffer);
        selectDeParser.setExpressionVisitor(expressionDeParser);
        updateDeParser.deParse(update);

    }

    public StringBuilder getBuffer() {
        return buffer;
    }

    public void setBuffer(StringBuilder buffer) {
        this.buffer = buffer;
    }

    @Override
    public void visit(Alter alter, String level) {
        AlterDeParser alterDeParser = new AlterDeParser(buffer);
        alterDeParser.deParse(alter);
    }

    @Override
    public void visit(Statements stmts, String level) {
        stmts.accept(this, level);
    }

    @Override
    public void visit(Execute execute, String level) {
        SelectDeParser selectDeParser = new SelectDeParser();
        selectDeParser.setBuffer(buffer);
        ExpressionDeParser expressionDeParser = new ExpressionDeParser(selectDeParser, buffer);
        ExecuteDeParser executeDeParser = new ExecuteDeParser(expressionDeParser, buffer);
        selectDeParser.setExpressionVisitor(expressionDeParser);
        executeDeParser.deParse(execute);
    }
}
