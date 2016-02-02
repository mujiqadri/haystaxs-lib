package com.haystack.domain;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by qadrim on 15-03-04.
 */
public class Table {
    public String oid;
    public String database;
    public String schema;
    public String tableName;
    public String dkArray;
    public TableStats stats;


    public HashMap<String, Column> columns;
    public HashMap<String, Column> dk;
    public HashMap<String, Column> partitionColumn;
    public HashMap<String, Join> joins;
    public HashMap<String, Partition> partitions;


    static Logger log = LoggerFactory.getLogger(Table.class.getName());


    public Table(){
        stats = new TableStats();
        columns = new HashMap<String, Column>();
        dk = new HashMap<String, Column>();
        partitionColumn = new HashMap<String, Column>();
        joins = new HashMap<String, Join>();
        partitions = new HashMap<String, Partition>();

    }


    // Add Partition
    public void addPartition(HashMap<String, Partition> partitions, Partition partition) {
        try {
            if (partition.level == 0) {
                this.partitions.put(partition.tableName, partition);
            } else {
                String parentPartitionTbl = partition.parentPartitionTableName;

                // Else Find the parentPartition and then add it to the child
                for (Map.Entry<String, Partition> entry : partitions.entrySet()) {
                    // Loop through partition to find substring of partitiontablename
                    String key = entry.getKey();
                    Partition value = entry.getValue();

                    if (parentPartitionTbl.contains(key)) {
                        // Check if level matches completely
                        if (value.level == partition.level - 1) {
                            value.childPartitions.put(partition.tableName, partition);
                        } else { // go deep
                            addPartition(value.childPartitions, partition);
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("Error in adding partition to Table:" + tableName + "==" + e.toString());
        }
    }

    // Add Join
    public void addJoin(Join join){
        try {
            boolean found = false;
            // Check if join already exists in the Hashmap, if yes then incrementJoinUsage
            // Else add the join in Hashmap
            Iterator<Map.Entry<String, Join>> entries = joins.entrySet().iterator();
            while(entries.hasNext()){
                Map.Entry<String, Join> entry = entries.next();
                Join currJoin = entry.getValue();

                if (currJoin.isEqual(join)) {
                    found = true;
                    currJoin.incrementSupportCount();
                }
            }
            if (found == false) {
                String key = String.valueOf(joins.size() + 1);
                joins.put(key, join);
            }
        }catch(Exception e){
            log.error(join.toString() + "===" + e.toString());
        }
    }
    // TODO Extract ordinal position from dkArray and set dk from columns;
    public void setDistributionKey(){
        if (dkArray.equals("NONE")){
            return;
        }
        String splitArr[] = dkArray.split(",");
        for( int i=0; i < splitArr.length; i++){
            Integer currAttrPos = Integer.parseInt(splitArr[i]);

            Iterator<Map.Entry<String, Column>> entries = columns.entrySet().iterator();
            while(entries.hasNext()){
                Map.Entry<String, Column> entry = entries.next();
                Column currColumn = entry.getValue();

                if (currColumn.ordinal_position == currAttrPos){
                    currColumn.isDK = true;
                    dk.put(entry.getKey(),currColumn);
                    columns.put(entry.getKey(),currColumn);
                    break;
                }
            }
        }
    }

    public Integer getColumnUsage(){
        Integer score = 0;
        Iterator<Map.Entry<String, Column>> entries = columns.entrySet().iterator();
        while(entries.hasNext()){
            Map.Entry<String, Column> entry = entries.next();
            Column currColumn = entry.getValue();

            score += currColumn.getUsageScore();
        }
        return score;
    }


    // TODO Generates Insert Statement for Catalog Service to execute
    //  in Haystack database
    public ArrayList<String> saveTable(){

        ArrayList<String> sqlInsertStmts = new ArrayList<String>();
        saveColumns();

        return sqlInsertStmts;
    }

    // TODO Generates Insert Statement for all Columns
    private void saveColumns(){

    }
}
