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

import com.haystack.parser.expression.Expression;
import com.haystack.parser.expression.ExpressionVisitor;
import com.haystack.parser.expression.relational.ExpressionList;
import com.haystack.parser.expression.relational.ItemsListVisitor;
import com.haystack.parser.expression.relational.MultiExpressionList;
import com.haystack.parser.schema.Column;
import com.haystack.parser.statement.insert.Insert;
import com.haystack.parser.statement.select.SelectExpressionItem;
import com.haystack.parser.statement.select.SelectVisitor;
import com.haystack.parser.statement.select.SubSelect;
import com.haystack.parser.statement.select.WithItem;

/**
 * A class to de-parse (that is, tranform from JSqlParser hierarchy into a
 * string) an {@link com.haystack.parser.statement.insert.Insert}
 */
public class InsertDeParser implements ItemsListVisitor {

    private StringBuilder buffer;
    private ExpressionVisitor expressionVisitor;
    private SelectVisitor selectVisitor;

    public InsertDeParser() {
    }

    /**
     * @param expressionVisitor a {@link ExpressionVisitor} to de-parse
     * {@link com.haystack.parser.expression.Expression}s. It has to share the
     * same<br>
     * StringBuilder (buffer parameter) as this object in order to work
     * @param selectVisitor a {@link SelectVisitor} to de-parse
     * {@link com.haystack.parser.statement.select.Select}s. It has to share the
     * same<br>
     * StringBuilder (buffer parameter) as this object in order to work
     * @param buffer the buffer that will be filled with the insert
     */
    public InsertDeParser(ExpressionVisitor expressionVisitor, SelectVisitor selectVisitor, StringBuilder buffer) {
        this.buffer = buffer;
        this.expressionVisitor = expressionVisitor;
        this.selectVisitor = selectVisitor;
    }

    public StringBuilder getBuffer() {
        return buffer;
    }

    public void setBuffer(StringBuilder buffer) {
        this.buffer = buffer;
    }

    public void deParse(Insert insert) {
        buffer.append("INSERT INTO ");
        buffer.append(insert.getTable().getFullyQualifiedName());
        if (insert.getColumns() != null) {
            buffer.append(" (");
            for (Iterator<Column> iter = insert.getColumns().iterator(); iter.hasNext();) {
                Column column = iter.next();
                buffer.append(column.getColumnName());
                if (iter.hasNext()) {
                    buffer.append(", ");
                }
            }
            buffer.append(")");
        }

        if (insert.getItemsList() != null) {
            insert.getItemsList().accept(this,"0");
        }

        if (insert.getSelect() != null) {
            buffer.append(" ");
            if (insert.isUseSelectBrackets()) {
                buffer.append("(");
            }
            if (insert.getSelect().getWithItemsList() != null) {
                buffer.append("WITH ");
                for (WithItem with : insert.getSelect().getWithItemsList()) {
                    with.accept(selectVisitor,"0");
                }
                buffer.append(" ");
            }
            insert.getSelect().getSelectBody().accept(selectVisitor,"0");
            if (insert.isUseSelectBrackets()) {
                buffer.append(")");
            }
        }

        if (insert.isReturningAllColumns()) {
            buffer.append(" RETURNING *");
        } else if (insert.getReturningExpressionList() != null) {
            buffer.append(" RETURNING ");
            for (Iterator<SelectExpressionItem> iter = insert.getReturningExpressionList().iterator(); iter.hasNext();) {
                buffer.append(iter.next().toString());
                if (iter.hasNext()) {
                    buffer.append(", ");
                }
            }
        }
    }

    @Override
    public void visit(ExpressionList expressionList, String level) {
        buffer.append(" VALUES (");
        for (Iterator<Expression> iter = expressionList.getExpressions().iterator(); iter.hasNext();) {
            Expression expression = iter.next();
            expression.accept(expressionVisitor, level);
            if (iter.hasNext()) {
                buffer.append(", ");
            }
        }
        buffer.append(")");
    }

    @Override
    public void visit(MultiExpressionList multiExprList, String level) {
        buffer.append(" VALUES ");
        for (Iterator<ExpressionList> it = multiExprList.getExprList().iterator(); it.hasNext();) {
            buffer.append("(");
            for (Iterator<Expression> iter = it.next().getExpressions().iterator(); iter.hasNext();) {
                Expression expression = iter.next();
                expression.accept(expressionVisitor, level);
                if (iter.hasNext()) {
                    buffer.append(", ");
                }
            }
            buffer.append(")");
            if (it.hasNext()) {
                buffer.append(", ");
            }
        }
    }

    @Override
    public void visit(SubSelect subSelect, String level) {
        subSelect.getSelectBody().accept(selectVisitor, level);
    }

    public ExpressionVisitor getExpressionVisitor() {
        return expressionVisitor;
    }

    public SelectVisitor getSelectVisitor() {
        return selectVisitor;
    }

    public void setExpressionVisitor(ExpressionVisitor visitor) {
        expressionVisitor = visitor;
    }

    public void setSelectVisitor(SelectVisitor visitor) {
        selectVisitor = visitor;
    }
}
