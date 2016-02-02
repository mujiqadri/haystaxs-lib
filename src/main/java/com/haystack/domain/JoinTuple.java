package com.haystack.domain;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by qadrim on 15-06-18.
 */
public class JoinTuple {
    public String leftcolumn;
    public String rightcolumn;


    public boolean matchJoinConditions(HashMap<String, JoinTuple> joinTupleHashMap) {
        boolean isMatched = false;
        try {
            Iterator<Map.Entry<String, JoinTuple>> entries = joinTupleHashMap.entrySet().iterator();

            while (entries.hasNext()) {
                Map.Entry<String, JoinTuple> entry = entries.next();
                JoinTuple joinTuple = entry.getValue();
                if(leftcolumn.equals(joinTuple.leftcolumn) && rightcolumn.equals(joinTuple.rightcolumn)){
                    return true;
                } else {
                    if(leftcolumn.equals(joinTuple.rightcolumn) && rightcolumn.equals(joinTuple.leftcolumn)) {
                        return true;
                    }
                }
            }
        } catch (Exception e) {
            return false;
        }
        return isMatched;
    }
}
