package com.haystack.service.database;

import com.haystack.domain.Tables;
import com.haystack.util.Credentials;
import com.haystack.util.DBConnectService;

import java.sql.Timestamp;

/**
 * Created by qadrim on 16-02-04.
 */
public class Netezza extends Cluster {
    public Netezza() {
        this.dbtype = DBConnectService.DBTYPE.NETEZZA;

    }


    @Override
    public void loadQueries(Integer clusterId, Timestamp lastRefreshTime) {

    }

    @Override
    public Tables loadTables(Credentials credentials, Boolean isGPSD) {
        return null;
    }


}
