package com.haystack.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.InputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by qadrim on 15-03-03.
 */
public class ConfigProperties {

    static Logger log = LoggerFactory.getLogger(ConfigProperties.class.getName());

    public Properties properties = new Properties();

    public void loadProperties() throws IOException {

        String propFileName = "config.properties";

        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);

        if(inputStream != null){
            properties.load(inputStream);
        } else {
            log.error("property file '" + propFileName + "' not found in classpath");
        }
    }


    public Credentials getHaystackDBCredentials() {

        Credentials credentials = new Credentials();
        credentials.setHostName(properties.getProperty("haystack.datasource.host"));
        credentials.setPort(properties.getProperty("haystack.datasource.port"));
        credentials.setDatabase(properties.getProperty("haystack.datasource.database"));
        credentials.setUserName(properties.getProperty("haystack.datasource.username"));
        credentials.setPassword(properties.getProperty("haystack.datasource.password"));
        return credentials;
    }

    public Credentials getUserDataCredentials() {

        Credentials clusterCred = new Credentials();
        clusterCred.setHostName(properties.getProperty("haystack.ds_cluster.host"));
        clusterCred.setPort(properties.getProperty("haystack.ds_cluster.port"));
        clusterCred.setDatabase(properties.getProperty("haystack.ds_cluster.database"));
        clusterCred.setUserName(properties.getProperty("haystack.ds_cluster.username"));
        clusterCred.setPassword(properties.getProperty("haystack.ds_cluster.password"));

        return clusterCred;
    }

    public Credentials getGPSDCredentials() {
        Credentials clusterCred = new Credentials();
        clusterCred.setHostName(properties.getProperty("gpsd.host"));
        clusterCred.setPort(properties.getProperty("gpsd.port"));
        clusterCred.setDatabase(properties.getProperty("gpsd.database"));
        clusterCred.setUserName(properties.getProperty("gpsd.username"));
        clusterCred.setPassword(properties.getProperty("gpsd.password"));

        return clusterCred;
    }
    public String getSchemaChangeThreshold(){
        String ret = "";
        try {
            ret = properties.getProperty("schema.change.threshold.days").toString();
        }
        catch(Exception e){
            log.error("Error in fetching config property schema.change.threshold.days\n");
        }
        return ret;
    }

}
