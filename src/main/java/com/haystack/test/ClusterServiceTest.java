package com.haystack.test;

import com.haystack.domain.Tables;
import com.haystack.parser.util.TablesNamesFinder;
import com.haystack.service.ClusterService;
import com.haystack.service.ModelService;
import com.haystack.util.ConfigProperties;
import junit.framework.TestCase;
import com.haystack.domain.Query;

import java.sql.SQLException;

public class ClusterServiceTest extends TestCase {

    public void testRefresh() throws Exception {


        Integer clusterId = 1001;


        ConfigProperties configProperties = new ConfigProperties();

        configProperties.loadProperties();

        ClusterService clusterService = new ClusterService(configProperties);

        clusterService.refresh();
    }
}