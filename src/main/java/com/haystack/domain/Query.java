package com.haystack.domain;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

public class Query {

    private Date logTime;
    private String logUser;
    private String logDatabase;
    private String logPid;
    private String logThread;
    private String logHost;
    private String logSegment;
    private String queryText;
    public List<String> tableIds;   // List of All Table Id used in the Query
    public List<Join> joins;        // List of all joins used in the Query
    public List<Attribute> projection;      // List of all columns used in the select clause and sub-select clauses in the query
    public List<Attribute> filter;          // List of all columns used in the where and having clause including the join conditions

    public Query(){
        tableIds = new ArrayList<String>();
        joins = new ArrayList<Join>();
        projection = new ArrayList<Attribute>();
        filter = new ArrayList<Attribute>();
    }

    public String getLogDatabase() {
        return logDatabase;
    }

    public String getLogUser() {

        return logUser;
    }

    public Date getLogTime() {

        return logTime;
    }

    public String getLogSegment() {
        return logSegment;
    }

    public String getLogHost() {

        return logHost;
    }

    public String getLogThread() {

        return logThread;
    }

    public String getLogPid() {

        return logPid;
    }

    public String getQueryText() {

        return queryText;
    }

    public void setQueryText(String str) {
        this.queryText = str;
    }
    public void setFilter(List<Attribute> filter) {

        this.filter = filter;
    }

    public void setProjection(List<Attribute> projection) {

        this.projection = projection;
    }

    public void setJoins(List<Join> joins) {

        this.joins = joins;
    }

    public void setTableIds(List<String> tableIds) {

        this.tableIds = tableIds;
    }

    public void add(Date logTime, String logUser, String logDatabase, String logPid, String logThread, String logHost,
                    String logSegment, String queryText){

        this.logTime = logTime;
        this.logUser = logUser;
        this.logDatabase = logDatabase;
        this.logPid = logPid;
        this.logThread = logThread;
        this.logHost = logHost;
        this.logSegment = logSegment;
        this.queryText = queryText;
    }
}
