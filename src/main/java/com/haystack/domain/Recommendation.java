package com.haystack.domain;

/**
 * Created by qadrim on 16-01-19.
 */
public class Recommendation {
    public Integer recommendation_id;
    public String oid;
    public String database;
    public String schema;
    public String tableName;

    public enum recommendationType {DK, DATATYPE, STORAGE, COMPRESSION, PARTITION}

    public String Description;
    public String Columns;

}
