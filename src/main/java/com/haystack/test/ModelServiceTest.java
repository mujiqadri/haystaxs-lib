package com.haystack.test;

import com.haystack.domain.Tables;
import com.haystack.parser.util.TablesNamesFinder;
import com.haystack.service.ClusterService;
import com.haystack.service.ModelService;
import com.haystack.util.ConfigProperties;
import junit.framework.TestCase;
import com.haystack.domain.Query;

import java.sql.SQLException;

public class ModelServiceTest extends TestCase {

    public void testProcessSQL() throws Exception {

        String myQuery =
                "select  asceding.rnk, i1.i_product_name best_performing, i2.i_product_name worst_performing\n" +
                        "from(select *\n" +
                        "     from (select item_sk,rank() over (order by rank_col asc) rnk\n" +
                        "           from (select ss_item_sk item_sk,avg(ss_net_profit) rank_col \n" +
                        "                 from store_sales ss1\n" +
                        "                 where ss_store_sk = 30\n" +
                        "                 group by ss_item_sk\n" +
                        "                 having avg(ss_net_profit) > 0.9*(select avg(ss_net_profit) rank_col\n" +
                        "                                                  from store_sales\n" +
                        "                                                  where ss_store_sk = 30\n" +
                        "                                                    and ss_hdemo_sk is null\n" +
                        "                                                  group by ss_store_sk))V1)V11\n" +
                        "     where rnk  < 11) asceding,\n" +
                        "    (select *\n" +
                        "     from (select item_sk,rank() over (order by rank_col desc) rnk\n" +
                        "           from (select ss_item_sk item_sk,avg(ss_net_profit) rank_col\n" +
                        "                 from store_sales ss1\n" +
                        "                 where ss_store_sk = 30\n" +
                        "                 group by ss_item_sk\n" +
                        "                 having avg(ss_net_profit) > 0.9*(select avg(ss_net_profit) rank_col\n" +
                        "                                                  from store_sales\n" +
                        "                                                  where ss_store_sk = 30\n" +
                        "                                                    and ss_hdemo_sk is null\n" +
                        "                                                  group by ss_store_sk))V2)V21\n" +
                        "     where rnk  < 11) descending,\n" +
                        "item i1,\n" +
                        "item i2\n" +
                        "where asceding.rnk = descending.rnk \n" +
                        "  and i1.i_item_sk=asceding.item_sk\n" +
                        "  and i2.i_item_sk=descending.item_sk\n" +
                        "order by asceding.rnk\n" +
                        "limit 100;";
        //myQuery = " select a as b from T;";
        //Query qry = new Query();
        //qry.setQueryText(myQuery);

        String current = new java.io.File(".").getCanonicalPath();
        System.out.println("Current dir:" + current);
        String currentDir = System.getProperty("user.dir");
        System.out.println("Current dir using System:" + currentDir);

        Integer userId = 3;

        ConfigProperties configProperties = new ConfigProperties();

        configProperties.loadProperties();

        ClusterService clusterService = new ClusterService(configProperties);

        //clusterService.readSQLFile("/Work/01-haystack/HayStack-1.2-22APR2015-JAR/HayStack-1.2-22APR2015/src/main/resources/query_0.sql");

        // Load Table stats into memory
        Tables tablelist = clusterService.getTablesfromCluster();

        ModelService ms = new ModelService();

        // Set the Cached Tables into the Model for future annotation
        ms.setTableList(tablelist);
        //ms.annotateModel(qry,clusterService.tablelist);

        for (int i = 0; i < clusterService.fileQueries.size(); i++) {
            Query qry = new Query();
            qry.setQueryText(clusterService.fileQueries.get(i));
            ms.processSQL(qry, 400.5, userId, "public");
        }

        ms.scoreModel();
        String str = ms.getModelJSON();

        System.out.print(str);
    }
}