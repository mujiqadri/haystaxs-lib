package com.haystack.domain;

import java.math.BigInteger;
/**
 * Created by qadrim on 15-03-04.
 */
public class TableStats {

    public float skew;
    public boolean isColumnar;
    public boolean isCompressed;
    public String storageMode;
    public String compressType;
    public Integer compressLevel;
    public Double noOfRows;
    public float sizeOnDisk;
    public float sizeUnCompressed;
    public float compressionRatio;
    public Integer noOfColumns;
    public boolean isPartitioned;
    public BigInteger blockSize;
    public Integer relPages;
    public String sizeForDisplayCompressed;  // Stores the scale of the size MB / GB/ TB
    public String sizeForDisplayUnCompressed;  // Stores the scale of the size MB / GB/ TB

    private float modelScore;
    public Double avgColUsage;
    public int rank; // Rank by size
    public Double percentile; // Percentile by size
    // Added 03Sep2015 Muji
    private Integer usageFrequency;  // No of times this table was used in the query workload
    private Double executionTime;   // Total time in seconds taken by this table in the workload
    private float workloadScore; // Total Workload Score for the this table

    public TableStats(){
        modelScore = 0;
        usageFrequency = 0;
        executionTime = 0.0;
        workloadScore = 0;
        relPages = 0;
        avgColUsage = 0.0;
    }
    public void addExecutionTime( Double timeSlice){
        executionTime += timeSlice;
    }
    public void addWorkloadScore(float score) {
        workloadScore += score;
    }
    public float getWorkloadScore(){
        return workloadScore;
    }
    public void incrementUsageFrequency(){
        usageFrequency++;
    }
    public Integer getTableUsageFrequency(){
        return usageFrequency;
    }
    public void setModelScore(float score){
        modelScore = score;
    }
    public float getModelScore(){
        return modelScore;
    }

    public void addChildStats(Integer iRelPages, Integer iRelTuples) {
        if (iRelPages > 0) {
            relPages = relPages + iRelPages;
            sizeOnDisk = ((relPages * 32) / (1024 * 1024));
            sizeUnCompressed = sizeOnDisk;
        }
        if (iRelTuples > 0) {
            noOfRows = noOfRows + (iRelTuples);
        }
    }
}
