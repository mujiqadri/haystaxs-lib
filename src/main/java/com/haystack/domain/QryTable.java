package com.haystack.domain;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by qadrim on 15-03-04.
 */
public class QryTable {
    public String tableId;
    public String database;
    public String schema;
    public String tablename;
    public String alias;
    //public String level;
    public Integer colUsage;  // This show how many columns were used in the query projection and where clause
    public float workloadScore;
    public float workloadPercentage;
}
