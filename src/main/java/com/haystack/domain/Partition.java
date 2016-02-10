package com.haystack.domain;

import java.util.HashMap;
import java.util.List;

/**
 * Created by qadrim on 15-11-13.
 */
public class Partition {
    public String tableName;      // Name of Table on which partition is created
    public String partitionName;  // Name of the partition
    public int level;
    public String type; // range or list
    public int rank;
    public int position;
    public String listValues;
    public String rangeStart;
    public Boolean rangeStartInclusive;
    public String rangeEnd;
    public Boolean rangeEndInclusive;
    public String everyClause;
    public Boolean isDefault;
    public String boundary;
    public HashMap<String, Partition> childPartitions;
    public Integer relTuples;
    public Integer relPages;
    public String parentPartitionTableName;
    public String parentPartitionName;
    public String sizeForDisplay;

    public Partition() {
        childPartitions = new HashMap<String, Partition>();
    }

}
