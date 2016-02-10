package com.haystack.service.database;

import com.haystack.util.Credentials;

import java.sql.Timestamp;

/**
 * Created by qadrim on 16-02-04.
 */
public class Redshift extends Cluster {

    @Override
    public String loadTables(boolean returnJson) {
        return null;
    }


    @Override
    public void loadQueries(Integer clusterId, Timestamp lastRefreshTime) {

    }

    @Override
    public void refreshTableStats(Integer clusterId) {

    }
}
