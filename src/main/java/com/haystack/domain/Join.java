package com.haystack.domain;

import java.lang.reflect.Array;
import java.util.*;

/**
 * Created by qadrim on 15-03-04.
 */
public class Join {
    public String leftSchema;
    public String leftTable;
    public String rightSchema;
    public String rightTable;
    public HashMap<String, JoinTuple> joinTuples;
    public String level;
    private int supportCount;  // This is the joinUsage score, how many times this join has occured in the query set
    private float confidence;  // This will be calculated based on below formula
                        // joinUsage * (LeftTable (Avg cols) * (# of Rows)) * (RightTable (Avg cols) * (# of Rows))

    public Join (){
        supportCount = 1;
        joinTuples = new HashMap<String, JoinTuple>();
    }

    public void incrementSupportCount() {
        supportCount++;
    }
    public boolean isEqual(Join currJoin){
        boolean isMatch = true;
        boolean tablesMatched = false;
        try {
            if (currJoin.leftSchema.equals(leftSchema) && currJoin.leftTable.equals(leftTable) &&
                    currJoin.rightSchema.equals(rightSchema) && currJoin.rightTable.equals(rightTable)) {
                tablesMatched = true;
            } else { // try flipping over the tables
                if (currJoin.leftSchema.equals(rightSchema) && currJoin.leftTable.equals(rightTable) &&
                        currJoin.rightSchema.equals(leftSchema) && currJoin.rightTable.equals(leftTable)) {
                    tablesMatched = true;
                }
            }
            if (tablesMatched) {// Tables matched now match join conditions
                Iterator<Map.Entry<String, JoinTuple>> entries = joinTuples.entrySet().iterator();
                boolean doTuplesMatch = true;
                while (entries.hasNext()) {
                    Map.Entry<String, JoinTuple> entry = entries.next();
                    JoinTuple localJoinTuple = entry.getValue();

                    if (localJoinTuple.matchJoinConditions(currJoin.joinTuples) == false) {
                        isMatch = false;
                        return isMatch;
                    }
                }
            }
        }catch (Exception e){

            return false;
        }
        if (tablesMatched) {
            return isMatch;
        } else
        {
            return tablesMatched;
        }
    }

    public int getSupportCount() {
        return supportCount;
    }

    public void setConfidence(float confidenceCalculated) {
        confidence = confidenceCalculated;
    }

    public float getConfidence() {
        return confidence;
    }

}
