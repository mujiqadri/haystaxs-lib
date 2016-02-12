package com.haystack.service.database;

import com.haystack.domain.Tables;
import com.haystack.util.Credentials;
import com.haystack.util.DBConnectService;

import java.sql.Timestamp;

/**
 * Created by qadrim on 16-02-04.
 */
public class Redshift extends Cluster {

    public Redshift() {

        this.dbtype = DBConnectService.DBTYPE.REDSHIFT;
    }
    @Override
    public Tables loadTables(Credentials credentials, Boolean isGPSD) {
        return null;
    }


    @Override
    public void loadQueries(Integer clusterId, Timestamp lastRefreshTime) {

    }


}
