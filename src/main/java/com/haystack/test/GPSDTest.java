package com.haystack.test;

import com.haystack.service.CatalogService;
import com.haystack.service.ClusterService;
import com.haystack.util.ConfigProperties;
import junit.framework.TestCase;

/**
 * Created by qadrim on 15-09-30.
 */
public class GPSDTest extends TestCase {

    public void testQueryFile() throws Exception {

        Integer querylogID = 28;
        String queryLogDirectory = "/tpcds_at_gmail_dot_com/querylogs/" + querylogID.toString();  // The zipped file should be unzipped in a temp folder on Master Node
        //String queryLogDirectory = "/querylogs";

        ConfigProperties configProperties = new ConfigProperties();

        configProperties.loadProperties();

        CatalogService catalogService = new CatalogService(configProperties);


        catalogService.processQueryLog(querylogID, queryLogDirectory);

        System.out.print("test finished");
    }

    public void testgetGPSDJson() throws Exception {
        ConfigProperties configProperties = new ConfigProperties();

        configProperties.loadProperties();

        CatalogService catalogService = new CatalogService(configProperties);

        String json = catalogService.getGPSDJson(33);

        System.out.print(json);
    }

    public void testGPSD() throws Exception {

        String userName = "tpcds";
        //String filename = "/Work/01-haystack/HayStack-1.2-22APR2015-JAR/HayStack-1.2-22APR2015/src/main/resources/gpsd-output-bmo.sql";
        //String filename = "/Work/uploads/mujtaba_dot_qadri_at_gmail_dot_com/gpsd/gpsd_emea_output_10302105.sql";
        String filename = "/Work/01-haystack/gpsd/tpc-ds_gpsd-30Oct2015.sql";

        ConfigProperties configProperties = new ConfigProperties();

        configProperties.loadProperties();

        CatalogService catalogService = new CatalogService(configProperties);

        catalogService.executeGPSD(33, userName, filename);

        System.out.print("test finished");
    }

    public void testWorkload() throws Exception {
        Integer workloadId = 19;

        ConfigProperties configProperties = new ConfigProperties();

        configProperties.loadProperties();

        CatalogService catalogService = new CatalogService(configProperties);

        String result = catalogService.processWorkload(workloadId);
        System.out.print("test finished");
    }

}
