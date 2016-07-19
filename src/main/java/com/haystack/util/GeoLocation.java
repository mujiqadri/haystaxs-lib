package com.haystack.util;

import com.google.gson.GsonBuilder;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;

/**
 * Created by Ghafffar on 7/19/2016.
 */
public class GeoLocation {

    private static Logger log = LoggerFactory.getLogger(GeoLocation.class.getName());

    private HttpClient httpClient;

    private String webServiceLocation;
    private Coordination coordination;

    HashMap<String, Coordination> ipsHashMap;

    public GeoLocation(){
//        webServiceLocation = "http://ip-api.com/json/"; //150 reuest per min
        webServiceLocation = "http://freegeoip.net/json/"; //limit 10k per hour
        httpClient = HttpClientBuilder.create().build();

        ipsHashMap = new HashMap<String, Coordination>();
    }

    public void setIP(String ip) {

        //For perfomance optimization
        if(ipsHashMap.containsKey(ip)){
            this.coordination = ipsHashMap.get(ip);
            return;
        }

        try {
            HttpGet httpGetMethod = new HttpGet(webServiceLocation + ip);
            httpGetMethod.addHeader("accept", "application/json");

            HttpResponse response = httpClient.execute(httpGetMethod);

            if (response.getStatusLine().getStatusCode() != 200) {
                log.error("Failed : HTTP error code : " + response.getStatusLine().getStatusCode());
                return;
            }

            String jsonResponse = EntityUtils.toString(response.getEntity());

            coordination = new GsonBuilder().create().fromJson(jsonResponse, Coordination.class);
            ipsHashMap.put(ip, coordination);
        }catch(Exception ex){
            log.error("Coudn't connect with the geolocation webservice: " +webServiceLocation +" error: " +ex.getMessage());
        }
    }

    public double getLongitude(){
        if(coordination == null){
            return 0;
        }
        return coordination.getLongitude();
    }

    public double getLatitude(){
        if(coordination == null){
            return 0;
        }
        return coordination.getLatitude();
    }

    private class Coordination{
        private double latitude;
        private double longitude;

        public void setLatitude(double lat){
            this.latitude = lat;
        }

        public void setLongitude(double lon){
            this.longitude = lon;
        }

        public double getLongitude(){return longitude;}
        public double getLatitude(){return  latitude;}
    }
}
