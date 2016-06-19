package com.haystack.domain;

/**
 * Created by qadrim on 15-04-20.
 */
public class Condition {

    public Condition (){
        isJoin = true;
    }
    public boolean isJoin;
    public String leftTable;
    public String leftColumn;
    public String rightTable;
    public String rightColumn;

    public String operator;

    // For Partition Clauses
    public String leftValue;
    public String rightValue;
    public String leftExpression;
    public String rightExpression;
    public String fullExpression;
    //public String level;
}
