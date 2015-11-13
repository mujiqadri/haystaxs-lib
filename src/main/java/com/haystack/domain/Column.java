package com.haystack.domain;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by qadrim on 15-03-04.
 */
public class Column {

    public String column_name;
    public Integer ordinal_position;
    public String data_type;
    public Boolean isDK;
    public Integer character_maximum_length;
    public Integer numeric_precision;
    public Integer numeric_precision_radix;
    public Integer numeric_scale;
    private Integer usageFrequency;
    private Integer whereUsage;
    public HashMap<String, String> whereConditionValue; // Stores all the where condition values against this column
    public HashMap<String, Integer> whereConditionFreq; // Count the frequency of each where condition key

    private String resolvedTableName;
    private String resolvedSchemaName;

    // Partition columns
    public Boolean isPartitioned;
    public int partitionLevel;
    public int positionInPartitionKey;

    public Column()
    {
        usageFrequency = 0;
        whereUsage = 0;
        whereConditionValue = new HashMap<String, String>();
        whereConditionFreq = new HashMap<String, Integer>();
    }
    public void setResolvedNames(String schemaName, String tableName){
        if (schemaName != null && tableName != null) {
            resolvedSchemaName = schemaName;
            resolvedTableName = tableName;
        }
    }
    public String getResolvedTableName(){
        return resolvedTableName;
    }
    public String getResolvedSchemaName(){
        return resolvedSchemaName;
    }

    public void incrementUsageScore(){
        usageFrequency++;
    }
    public Integer getUsageScore(){
        return usageFrequency;
    }
    public void incrementWhereUsageScore(){
        whereUsage++;
    }
}
