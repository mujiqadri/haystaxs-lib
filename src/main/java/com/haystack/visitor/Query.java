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


    private String level; //1, 1.1, 1.2, 1.2.1

    public ArrayList<QryTable> tables;
    public ArrayList<Attribute> columns;
    public ArrayList<Condition> conditions;
    private List<String> otherItemNames;

    public ArrayList<Query> subQueries;

    //Query parentQuery;

    public Query(Query parentQuery){
        //this.parentQuery = parentQuery;

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

    public boolean addTable(Query parentQuery, QryTable newTable, String newLevel) {
        if (parentQuery.level.equals(newLevel)) {
            return parentQuery.tables.add(newTable);
        }else{
            if (isImmediateChild(parentQuery.level, newLevel)) {

                Query subQuery = containsSubQuery(parentQuery.subQueries, newLevel);

                if(subQuery == null){
                    subQuery = new Query(this);
                    subQuery.level = newLevel;
                    subQuery.tables.add(newTable);
                    parentQuery.subQueries.add(subQuery);
                }else{
                    this.addTable(subQuery, newTable, newLevel);
                }

            }else{
                Query subQuery = containsSubQuery(parentQuery.subQueries, newLevel);

                if (subQuery != null) {
                    this.addTable(subQuery, newTable, newLevel);
                    return true;
                }
            }
        }
        return false;
    }


    public boolean addColumn(Query parentQuery, Attribute newColumn, String newLevel) {
        if (parentQuery.level.equals(newLevel)) {
            return parentQuery.columns.add(newColumn);
        }else{
            if (isImmediateChild(parentQuery.level, newLevel)) {

                Query subQuery = containsSubQuery(parentQuery.subQueries, newLevel);

                if(subQuery == null){
                    subQuery = new Query(this);
                    subQuery.level = newLevel;
                    subQuery.columns.add(newColumn);
                    parentQuery.subQueries.add(subQuery);
                }else{
                    this.addColumn(subQuery, newColumn, newLevel);
                }

            }else{
                Query subQuery = containsSubQuery(parentQuery.subQueries, newLevel);

                if (subQuery != null) {
                    this.addColumn(subQuery, newColumn, newLevel);
                    return true;
                }
            }
        }
        return false;
    }

    public boolean addCondition(Query parentQuery, Condition newCondition, String newLevel) {
        if (parentQuery.level.equals(newLevel)) {
            return parentQuery.conditions.add(newCondition);
        }else{
            if (isImmediateChild(parentQuery.level, newLevel)) {

                Query subQuery = containsSubQuery(parentQuery.subQueries, newLevel);

                if(subQuery == null){
                    subQuery = new Query(this);
                    subQuery.level = newLevel;
                    subQuery.conditions.add(newCondition);
                    parentQuery.subQueries.add(subQuery);
                }else{
                    this.addCondition(subQuery, newCondition, newLevel);
                }

            }else{
                Query subQuery = containsSubQuery(parentQuery.subQueries, newLevel);

                if (subQuery != null) {
                    this.addCondition(subQuery, newCondition, newLevel);
                    return true;
                }
            }
        }
        return false;
    }

    private Query containsSubQuery(ArrayList<Query> subQueries, String childLevel) {
        Iterator<Query> subQueriesIterator = this.subQueries.iterator();
        while (subQueriesIterator.hasNext()) {
            Query currQuery = subQueriesIterator.next();
            if (currQuery.level.equals(childLevel)) {
                return currQuery;
            }
        }

        return null;
    }

    private boolean isImmediateChild(String parentLevel, String childLevel) {
        try {

            String rawNoOfDotsParent[] = parentLevel.split("\\.");
            String rawNoOfDotsChild[] = childLevel.split("\\.");

            for (int i = 0; i < rawNoOfDotsParent.length; i++) {
                if (!(rawNoOfDotsParent[i].equals(rawNoOfDotsChild[i]))) {
                    return false;
                }
            }

            if (rawNoOfDotsChild.length - 1 == rawNoOfDotsParent.length) {
                return true;
            }
        } catch (Exception ex) {
            return false;
        }

        return false;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }
}
