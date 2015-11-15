package com.haystack.domain;

import java.util.HashMap;
import java.util.List;

/**
 * Created by qadrim on 15-11-13.
 */
public class Partition {
    String tableName;      // Name of Table on which partition is created
    String partitionName;  // Name of the partition
    int level;
    String type; // range or list
    int rank;
    int position;
    String listValues;
    String rangeStart;
    Boolean rangeStartInclusive;
    String rangeEnd;
    Boolean rangeEndInclusive;
    String everyClause;
    Boolean isDefault;
    String boundary;
    HashMap<String, Partition> childPartitions;
    Integer relTuples;
    Integer relPages;
    String parentPartitionTableName;
    String parentPartitionName;
    String sizeForDisplay;

    public Partition() {
        childPartitions = new HashMap<String, Partition>();
    }

}
