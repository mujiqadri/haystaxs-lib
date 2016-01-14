package com.haystack.service;


import com.haystack.domain.Query;
import com.haystack.domain.Tables;
import com.haystack.parser.util.TablesNamesFinder;
import com.haystack.util.ConfigProperties;
import com.haystack.util.Credentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by qadrim on 15-04-23.
 */
public class MainService {
    static Logger log = LoggerFactory.getLogger(ModelService.class.getName());
    static ConfigProperties configProperties = new ConfigProperties();

    // CatalogService will store and retreive data from
    static CatalogService catalogService;

    // ClusterService will get stats table and query data from cluster
    static ClusterService clusterService;

    // Model Service will parse SQL and annotate the model
    static ModelService modelService;

    public void run(){
        log.info("<=========== Starting Haystack ==========> ");

        try {
            // Read properties from config.properties
            configProperties.loadProperties();


            // Connect to haystack database using credentials
            catalogService = new CatalogService(configProperties);

            clusterService = new ClusterService(configProperties);

            Tables tablelist = clusterService.getTablesfromCluster();

            clusterService.getQueriesfromDB();

            catalogService.saveQueries(clusterService.querylist); // Save Queries in Haystack Schema

            modelService = new ModelService();

            // Set the Cached Tables into the Model for future annotation
            modelService.setTableList(tablelist);

            for (int i = 0; i < clusterService.querylist.size(); i++) {
                log.info("Processing Query # " + i);
                Query query = clusterService.querylist.get(i);

                String json = modelService.processSQL(1, query, 0, 3, "public");
            }

        } catch(Exception e){
            log.error(e.toString());
        }

        log.info("<=========== Ending Haystack ==========> Cya Later, Bye");
    }
}
