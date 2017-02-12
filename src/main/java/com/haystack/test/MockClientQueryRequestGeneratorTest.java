package com.haystack.test;

import junit.framework.TestCase;

import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by GhaffarMallah on 2/11/2017.
 */
public class MockClientQueryRequestGeneratorTest{

    public static void main(String args[]) throws ClassNotFoundException{

        final int TIME_IN_HOURS = 2;
        final long LOOP_END_TIME = System.nanoTime() + TimeUnit.HOURS.toNanos(TIME_IN_HOURS);

        final String DB_CONNECTION_STRING = "jdbc:redshift://testcluster.czx8qf1xgt0z.us-west-2.redshift.amazonaws.com:5439/haystack";
        final String DB_USER = "admin";
        final String DB_USER_PASSWORD = "Ghaffar12345";

        List<String> queries = new ArrayList<String>();
        queries.add("select\n" +
                "  ac.ad_campaign_id as ad_campaign_id,\n" +
                "  adv.advertiser_id as advertiser_id,\n" +
                "  cs.spending as spending,\n" +
                "  ims.imp_total as imp_total,\n" +
                "  cs.click_total as click_total,\n" +
                "  (1.0*click_total)/imp_total as CTR,\n" +
                "  spending/click_total as CPC,\n" +
                "  spending/(imp_total/1000) as CPM\n" +
                "from\n" +
                "  ad_campaigns ac\n" +
                "join\n" +
                "  advertisers adv on (ac.advertiser_id = adv.advertiser_id)\n" +
                "join\n" +
                "(\n" +
                "  select\n" +
                "    il.ad_campaign_id,\n" +
                "    count(*) as imp_total\n" +
                "  from\n" +
                "    imp_logs il\n" +
                "  group by\n" +
                "    il.ad_campaign_id\n" +
                ") ims on (ims.ad_campaign_id = ac.ad_campaign_id)\n" +
                "join\n" +
                "(\n" +
                "  select\n" +
                "    cl.ad_campaign_id,\n" +
                "    sum(cl.bid_price) as spending,\n" +
                "    count(*) as click_total\n" +
                "  from\n" +
                "    click_logs cl\n" +
                "  group by\n" +
                "    cl.ad_campaign_id\n" +
                ") cs on (cs.ad_campaign_id = ac.ad_campaign_id);");
        queries.add("SELECT (SELECT count(ad_campaign_id) FROM ad_campaigns) as total_add_campaigns, (SELECT count(publisher_id) FROM publishers) as total_publishers, (SELECT count(advertiser_id) FROM advertisers) as total_advertisers;");
        queries.add("SELECT ad.advertiser_id, adc.ad_campaign_id, count(imp.publisher_id) as total_imp_logs FROM advertisers ad LEFT JOIN ad_campaigns adc ON (ad.advertiser_id = adc.advertiser_id) LEFT JOIN imp_logs as imp ON (adc.ad_campaign_id = imp.ad_campaign_id) GROUP BY ad.advertiser_id, adc.ad_campaign_id;");
        queries.add("SELECT ad.advertiser_id, adc.ad_campaign_id, cl.bid_price, cl.country as total_imp_logs FROM advertisers ad LEFT JOIN ad_campaigns adc ON (ad.advertiser_id = adc.advertiser_id) LEFT JOIN click_logs as cl ON (adc.ad_campaign_id = cl.ad_campaign_id) GROUP BY ad.advertiser_id, adc.ad_campaign_id, cl.bid_price, cl.country;");

        Class.forName("com.amazon.redshift.jdbc.Driver");

        Connection connection = null;
        Statement statement = null;

        try {
            connection = DriverManager.getConnection(DB_CONNECTION_STRING, DB_USER, DB_USER_PASSWORD);
            statement = connection.createStatement();
        }catch(SQLException ex){
            ex.printStackTrace();
        }

        Iterator<String> iterator = queries.iterator();

        int counter = 1;

        while(System.nanoTime() < LOOP_END_TIME){

            if(iterator.hasNext() == false) iterator = queries.iterator();

            String currQuery = iterator.next();
            System.out.print("Q# " +counter);

            try {
                ResultSet resultSet = statement.executeQuery(currQuery);
                resultSet.close();
                System.out.println(" Done");
            }catch(SQLException ex){
                System.out.println(" ERROR");
            }

            counter++;
        }
    }
}
