package com.haystack.visitor;

import com.haystack.domain.Attribute;
import com.haystack.domain.Condition;
import com.haystack.domain.QryTable;
import net.sf.jsqlparser.schema.Column;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Ghaffar on 6/18/2016.
 */
public class Query {
    String level; //1, 1.1, 1.2, 1.2.1

    public ArrayList<QryTable> tables;
    public ArrayList<Attribute> columns;
    public ArrayList<Condition> conditions;
    private List<String> otherItemNames;

    public ArrayList<Query> subQueries;

    Query parentQuery;

    public Query(Query parentQuery){
        this.parentQuery = parentQuery;

        if(parentQuery == null){
            level = "1";
        }else{
            this.level =  parentQuery.level + "." +( parentQuery.subQueries.size() +1);
        }

        tables = new ArrayList<QryTable>();
        columns = new ArrayList<Attribute>();
        conditions = new ArrayList<Condition>();
        otherItemNames = new ArrayList<String>();
        subQueries = new ArrayList<Query>();
    }

    public boolean addTable(QryTable table, String currLevel){
        if(level.equals(currLevel)){

            return tables.add(table);

        }else{
            if(isImmediateChild( level, currLevel)) {

                Query subQuery = containsSubQuery(this.subQueries, currLevel);

                if(subQuery == null){
                    subQuery = new Query(this);
                    subQuery.addTable(table, currLevel);
                    this.subQueries.add(subQuery);
                }else{
                    subQuery.addTable(table, currLevel);
                }

            }else{
                for(Query currQuery: subQueries){
                    if(currQuery.addTable(table, currLevel)){
                        return true;
                    }
                }
            }
        }

        return false;
    }

    private Query containsSubQuery(ArrayList<Query> subQueries, String childLevel){
        Iterator<Query> subQueriesIterator = this.subQueries.iterator();
        while(subQueriesIterator.hasNext()){
            Query currQuery = subQueriesIterator.next();
            if(currQuery.level.equals(childLevel)){
                return currQuery;
            }
        }

        return null;
    }

    private boolean isImmediateChild(String parentLevel, String childLevel){
        try {

            String rawNoOfDotsParent[] = parentLevel.split("\\.");
            String rawNoOfDotsChild[] = childLevel.split("\\.");

            for (int i = 0; i < rawNoOfDotsParent.length; i++) {
                if ( !(rawNoOfDotsParent[i].equals(rawNoOfDotsChild[i])) ) {
                    return false;
                }
            }

            if (rawNoOfDotsChild.length - 1 == rawNoOfDotsParent.length) {
                return true;
            }
        }catch(Exception ex){
            return false;
        }

        return false;
    }

    public boolean addColumn(Attribute column, String currLevel){
        if(level.equals(currLevel)){
            return columns.add(column);
        }else{
            if(isImmediateChild( level, currLevel)) {

                Query subQuery = containsSubQuery(this.subQueries, currLevel);

                if(subQuery == null){
                    subQuery = new Query(this);
                    subQuery.addColumn(column, currLevel);
                    this.subQueries.add(subQuery);
                }else{
                    subQuery.addColumn(column, currLevel);
                }

            }else{
                for(Query currQuery: subQueries){
                    if(currQuery.addColumn(column, currLevel)){
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public boolean addCondition(Condition condition, String currLevel){
        if(level.equals(currLevel)){
            return conditions.add(condition);
        }else{
            if(isImmediateChild( level, currLevel)) {

                Query subQuery = containsSubQuery(this.subQueries, currLevel);

                if(subQuery == null){
                    subQuery = new Query(this);
                    subQuery.addCondition(condition, currLevel);
                    this.subQueries.add(subQuery);
                }else{
                    subQuery.addCondition(condition, currLevel);
                }

            }else{
                for(Query currQuery: subQueries){
                    if(currQuery.addCondition(condition, currLevel)){
                        return true;
                    }
                }
            }
        }
        return false;
    }
}
