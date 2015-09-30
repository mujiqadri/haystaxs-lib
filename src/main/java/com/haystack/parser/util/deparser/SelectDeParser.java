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

import com.haystack.parser.expression.Alias;
import com.haystack.parser.expression.Expression;
import com.haystack.parser.expression.ExpressionVisitor;
import com.haystack.parser.schema.Column;
import com.haystack.parser.schema.Table;
import com.haystack.parser.statement.select.*;

import java.util.*;

/**
 * A class to de-parse (that is, tranform from JSqlParser hierarchy into a
 * string) a {@link com.haystack.parser.statement.select.Select}
 */
public class SelectDeParser implements SelectVisitor, OrderByVisitor, SelectItemVisitor, FromItemVisitor, PivotVisitor {

    private StringBuilder buffer;
    private ExpressionVisitor expressionVisitor;

    public SelectDeParser() {
    }

    /**
     * @param expressionVisitor a {@link ExpressionVisitor} to de-parse
     * expressions. It has to share the same<br>
     * StringBuilder (buffer parameter) as this object in order to work
     * @param buffer the buffer that will be filled with the select
     */
    public SelectDeParser(ExpressionVisitor expressionVisitor, StringBuilder buffer) {
        this.buffer = buffer;
        this.expressionVisitor = expressionVisitor;
    }

    @Override
    public void visit(PlainSelect plainSelect, String level) {
        buffer.append("SELECT ");
        if (plainSelect.getDistinct() != null) {
            buffer.append("DISTINCT ");
            if (plainSelect.getDistinct().getOnSelectItems() != null) {
                buffer.append("ON (");
                for (Iterator<SelectItem> iter = plainSelect.getDistinct().getOnSelectItems().iterator(); iter.hasNext();) {
                    SelectItem selectItem = iter.next();
                    selectItem.accept(this, level);
                    if (iter.hasNext()) {
                        buffer.append(", ");
                    }
                }
                buffer.append(") ");
            }

        }
        Top top = plainSelect.getTop();
        if (top != null) {
            buffer.append(top).append(" ");
        }

        for (Iterator<SelectItem> iter = plainSelect.getSelectItems().iterator(); iter.hasNext();) {
            SelectItem selectItem = iter.next();
            selectItem.accept(this, level);
            if (iter.hasNext()) {
                buffer.append(", ");
            }
        }

        if (plainSelect.getIntoTables() != null) {
            buffer.append(" INTO ");
            for (Iterator<Table> iter = plainSelect.getIntoTables().iterator(); iter.hasNext();) {
                visit(iter.next(), level);
                if (iter.hasNext()) {
                    buffer.append(", ");
                }
            }
        }

        if (plainSelect.getFromItem() != null) {
            buffer.append(" FROM ");
            plainSelect.getFromItem().accept(this, level);
        }

        if (plainSelect.getJoins() != null) {
            for (Join join : plainSelect.getJoins()) {
                deparseJoin(join);
            }
        }

        if (plainSelect.getWhere() != null) {
            buffer.append(" WHERE ");
            plainSelect.getWhere().accept(expressionVisitor, level);
        }

        if (plainSelect.getOracleHierarchical() != null) {
            plainSelect.getOracleHierarchical().accept(expressionVisitor, level);
        }

        if (plainSelect.getGroupByColumnReferences() != null) {
            buffer.append(" GROUP BY ");
            for (Iterator<Expression> iter = plainSelect.getGroupByColumnReferences().iterator(); iter.hasNext();) {
                Expression columnReference = iter.next();
                columnReference.accept(expressionVisitor, level);
                if (iter.hasNext()) {
                    buffer.append(", ");
                }
            }
        }

        if (plainSelect.getHaving() != null) {
            buffer.append(" HAVING ");
            plainSelect.getHaving().accept(expressionVisitor, level);
        }

        if (plainSelect.getOrderByElements() != null) {
            deparseOrderBy(plainSelect.isOracleSiblings(), plainSelect.getOrderByElements());
        }

        if (plainSelect.getLimit() != null) {
            deparseLimit(plainSelect.getLimit());
        }
        if (plainSelect.getOffset() != null) {
            deparseOffset(plainSelect.getOffset());
        }
        if (plainSelect.getFetch() != null) {
            deparseFetch(plainSelect.getFetch());
        }
        if (plainSelect.isForUpdate()) {
            buffer.append(" FOR UPDATE");
        }

    }

    @Override
    public void visit(OrderByElement orderBy, String level) {
        orderBy.getExpression().accept(expressionVisitor, level);
        if (!orderBy.isAsc()) {
            buffer.append(" DESC");
        } else if (orderBy.isAscDescPresent()) {
            buffer.append(" ASC");
        }
        if (orderBy.getNullOrdering() != null) {
            buffer.append(' ');
            buffer.append(orderBy.getNullOrdering() == OrderByElement.NullOrdering.NULLS_FIRST ? "NULLS FIRST" : "NULLS LAST");
        }
    }

    public void visit(Column column) {
        buffer.append(column.getFullyQualifiedName());
    }

    @Override
    public void visit(AllTableColumns allTableColumns, String level) {
        buffer.append(allTableColumns.getTable().getFullyQualifiedName()).append(".*");
    }

    @Override
    public void visit(SelectExpressionItem selectExpressionItem, String level) {
        selectExpressionItem.getExpression().accept(expressionVisitor, level);
        if (selectExpressionItem.getAlias() != null) {
            buffer.append(selectExpressionItem.getAlias().toString());
        }

    }

    @Override
    public void visit(SubSelect subSelect, String level) {
        buffer.append("(");
        if (subSelect.getWithItemsList() != null && !subSelect.getWithItemsList().isEmpty()) {
            buffer.append("WITH ");
            for (Iterator<WithItem> iter = subSelect.getWithItemsList().iterator(); iter.hasNext();) {
                WithItem withItem = iter.next();
                withItem.accept(this, level);
                if (iter.hasNext()) {
                    buffer.append(",");
                }
                buffer.append(" ");
            }
        }
        subSelect.getSelectBody().accept(this, level);
        buffer.append(")");
        Pivot pivot = subSelect.getPivot();
        if (pivot != null) {
            pivot.accept(this);
        }
        Alias alias = subSelect.getAlias();
        if (alias != null) {
            buffer.append(alias.toString());
        }
    }

    @Override
    public void visit(Table tableName, String level) {
        buffer.append(tableName.getFullyQualifiedName());
        Pivot pivot = tableName.getPivot();
        if (pivot != null) {
            pivot.accept(this);
        }
        Alias alias = tableName.getAlias();
        if (alias != null) {
            buffer.append(alias);
        }
    }

    @Override
    public void visit(Pivot pivot) {
        List<Column> forColumns = pivot.getForColumns();
        buffer.append(" PIVOT (")
                .append(PlainSelect.getStringList(pivot.getFunctionItems()))
                .append(" FOR ")
                .append(PlainSelect.getStringList(forColumns, true, forColumns != null && forColumns.size() > 1))
                .append(" IN ")
                .append(PlainSelect.getStringList(pivot.getInItems(), true, true))
                .append(")");
    }

    @Override
    public void visit(PivotXml pivot) {
        List<Column> forColumns = pivot.getForColumns();
        buffer.append(" PIVOT XML (")
                .append(PlainSelect.getStringList(pivot.getFunctionItems()))
                .append(" FOR ")
                .append(PlainSelect.getStringList(forColumns, true, forColumns != null && forColumns.size() > 1))
                .append(" IN (");
        if (pivot.isInAny()) {
            buffer.append("ANY");
        } else if (pivot.getInSelect() != null) {
            buffer.append(pivot.getInSelect());
        } else {
            buffer.append(PlainSelect.getStringList(pivot.getInItems()));
        }
        buffer.append("))");
    }

    public void deparseOrderBy(List<OrderByElement> orderByElements) {
        deparseOrderBy(false, orderByElements);
    }

    public void deparseOrderBy(boolean oracleSiblings, List<OrderByElement> orderByElements) {
        if (oracleSiblings) {
            buffer.append(" ORDER SIBLINGS BY ");
        } else {
            buffer.append(" ORDER BY ");
        }
        for (Iterator<OrderByElement> iter = orderByElements.iterator(); iter.hasNext();) {
            OrderByElement orderByElement = iter.next();
            orderByElement.accept(this);
            if (iter.hasNext()) {
                buffer.append(", ");
            }
        }
    }

    public void deparseLimit(Limit limit) {
        // LIMIT n OFFSET skip
        if (limit.isRowCountJdbcParameter()) {
            buffer.append(" LIMIT ");
            buffer.append("?");
        } else if (limit.getRowCount() >= 0) {
            buffer.append(" LIMIT ");
            buffer.append(limit.getRowCount());
        } else if (limit.isLimitNull()) {
            buffer.append(" LIMIT NULL");
        }

        if (limit.isOffsetJdbcParameter()) {
            buffer.append(" OFFSET ?");
        } else if (limit.getOffset() != 0) {
            buffer.append(" OFFSET ").append(limit.getOffset());
        }

    }

    public void deparseOffset(Offset offset) {
        // OFFSET offset
    	// or OFFSET offset (ROW | ROWS)
        if (offset.isOffsetJdbcParameter()) {
            buffer.append(" OFFSET ?");
        } else if (offset.getOffset() != 0) {
            buffer.append(" OFFSET ");
            buffer.append(offset.getOffset());
        }
        if (offset.getOffsetParam() != null) {
        	buffer.append(" ").append(offset.getOffsetParam());
        }

    }

    public void deparseFetch(Fetch fetch) {
        // FETCH (FIRST | NEXT) row_count (ROW | ROWS) ONLY
    	buffer.append(" FETCH ");
    	if (fetch.isFetchParamFirst()) {
    		buffer.append("FIRST ");
    	} else {
    		buffer.append("NEXT ");
    	}
    	if (fetch.isFetchJdbcParameter()) {
    		buffer.append("?");
    	} else {
    		buffer.append(fetch.getRowCount());
    	}
    	buffer.append(" ").append(fetch.getFetchParam()).append(" ONLY");

    }

    public StringBuilder getBuffer() {
        return buffer;
    }

    public void setBuffer(StringBuilder buffer) {
        this.buffer = buffer;
    }

    public ExpressionVisitor getExpressionVisitor() {
        return expressionVisitor;
    }

    public void setExpressionVisitor(ExpressionVisitor visitor) {
        expressionVisitor = visitor;
    }

    @Override
    public void visit(SubJoin subjoin, String level) {
        buffer.append("(");
        subjoin.getLeft().accept(this, level);
        deparseJoin(subjoin.getJoin());
        buffer.append(")");

        if (subjoin.getPivot() != null) {
            subjoin.getPivot().accept(this);
        }
    }

    public void deparseJoin(Join join) {
        if (join.isSimple()) {
            buffer.append(", ");
        } else {

            if (join.isRight()) {
                buffer.append(" RIGHT");
            } else if (join.isNatural()) {
                buffer.append(" NATURAL");
            } else if (join.isFull()) {
                buffer.append(" FULL");
            } else if (join.isLeft()) {
                buffer.append(" LEFT");
            } else if (join.isCross()) {
                buffer.append(" CROSS");
            }

            if (join.isOuter()) {
                buffer.append(" OUTER");
            } else if (join.isInner()) {
                buffer.append(" INNER");
            }

            buffer.append(" JOIN ");

        }

        FromItem fromItem = join.getRightItem();
        fromItem.accept(this,"0");
        if (join.getOnExpression() != null) {
            buffer.append(" ON ");
            join.getOnExpression().accept(expressionVisitor,"0");
        }
        if (join.getUsingColumns() != null) {
            buffer.append(" USING (");
            for (Iterator<Column> iterator = join.getUsingColumns().iterator(); iterator.hasNext();) {
                Column column = iterator.next();
                buffer.append(column.getFullyQualifiedName());
                if (iterator.hasNext()) {
                    buffer.append(", ");
                }
            }
            buffer.append(")");
        }

    }

    @Override
    public void visit(SetOperationList list, String level) {
        for (int i = 0; i < list.getPlainSelects().size(); i++) {
            if (i != 0) {
                buffer.append(' ').append(list.getOperations().get(i - 1)).append(' ');
            }
            buffer.append("(");
            PlainSelect plainSelect = list.getPlainSelects().get(i);
            plainSelect.accept(this, level);
            buffer.append(")");
        }
        if (list.getOrderByElements() != null) {
            deparseOrderBy(list.getOrderByElements());
        }

        if (list.getLimit() != null) {
            deparseLimit(list.getLimit());
        }
        if (list.getOffset() != null) {
            deparseOffset(list.getOffset());
        }
        if (list.getFetch() != null) {
            deparseFetch(list.getFetch());
        }
    }

    @Override
    public void visit(WithItem withItem, String level) {
        buffer.append(withItem.getName());
        if (withItem.getWithItemList()!=null) {
            buffer.append(" ").append(PlainSelect.getStringList(withItem.getWithItemList(), true, true));
        }
        buffer.append(" AS (");
        withItem.getSelectBody().accept(this, level);
        buffer.append(")");
    }

    @Override
    public void visit(LateralSubSelect lateralSubSelect, String level) {
        buffer.append(lateralSubSelect.toString());
    }

    @Override
    public void visit(ValuesList valuesList, String level) {
        buffer.append(valuesList.toString());
    }

    @Override
    public void visit(AllColumns allColumns, String level) {
        buffer.append('*');
    }
}
