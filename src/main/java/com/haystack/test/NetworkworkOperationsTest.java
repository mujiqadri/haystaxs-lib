package com.haystack.test;

import com.haystack.util.GeoLocation;
import junit.framework.TestCase;

import java.io.IOException;

/**
 * Created by Ghafffar on 7/18/2016.
 */
public class NetworkworkOperationsTest  extends TestCase{

    public void testIpToGeoLocatoin() throws IOException {
        String ip = "103.255.4.249";

        GeoLocation geoLocation = new GeoLocation();
        geoLocation.setIP(ip);

        System.out.println("Lat: " + geoLocation.getLatitude());
        System.out.println("Lon: " + geoLocation.getLongitude());

    }

}
