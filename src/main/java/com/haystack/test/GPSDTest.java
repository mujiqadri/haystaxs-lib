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

        Integer querylogID = 27;
        String queryLogDirectory = "/mujtaba_dot_qadri_at_gmail_dot_com/querylogs/" + querylogID.toString();  // The zipped file should be unzipped in a temp folder on Master Node
        //String queryLogDirectory = "/querylogs";

        ConfigProperties configProperties = new ConfigProperties();

        configProperties.loadProperties();

        CatalogService catalogService = new CatalogService(configProperties);


        catalogService.processQueryLog(querylogID, queryLogDirectory);

        System.out.print("test finished");
    }

    public void testGPSD() throws Exception {

        String userId = "CITI_EMEA";
        //String filename = "/Work/01-haystack/HayStack-1.2-22APR2015-JAR/HayStack-1.2-22APR2015/src/main/resources/gpsd-output-bmo.sql";
        String filename = "/Work/uploads/mujtaba_dot_qadri_at_gmail_dot_com/gpsd/gpsd_emea_output_10302105.sql";

        ConfigProperties configProperties = new ConfigProperties();

        configProperties.loadProperties();

        CatalogService catalogService = new CatalogService(configProperties);

        catalogService.executeGPSD(19, userId, filename);

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
