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

    public String getFullDataType() {
        String datatype = this.data_type.toUpperCase();
        String fullDataType = "";
        if (this.character_maximum_length > 0) {
            fullDataType = data_type + "(" + this.character_maximum_length + ")";
        }
        if (this.numeric_scale > 0 && this.numeric_precision > 0) {
            fullDataType = data_type + "(" + this.numeric_precision + "," + this.numeric_scale + ")";
        }
        if (fullDataType.length() == 0) {
            fullDataType = datatype;
        }
        return fullDataType;
    }

    public String isMatchDataType(Column rightColumn) {
        Boolean match = true;
        String anamoly = "";
        if (this.data_type.equals(rightColumn.data_type)) {
            if (this.character_maximum_length != rightColumn.character_maximum_length) {
                anamoly = "Length mismatch";
            }
            if (this.numeric_precision != rightColumn.numeric_precision) {
                anamoly = "Numeric precision mismatch";
            }
            if (this.numeric_scale != rightColumn.numeric_scale) {
                anamoly = "Numeric scale mismatch";
            }
        } else { // DataType mismatch
            anamoly = "Datatype mismatch";
        }
        if (anamoly.length() > 0) {
            anamoly += " between " + this.column_name + "=" + this.getFullDataType() + " and " + rightColumn.column_name
                    + "=" + rightColumn.getFullDataType();
        }
        return anamoly;
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

    public java.lang.Integer getWhereUsage() {
        return whereUsage;
    }
}
