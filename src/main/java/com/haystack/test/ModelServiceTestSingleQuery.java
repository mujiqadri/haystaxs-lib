package com.haystack.test;

import com.haystack.domain.Tables;
import com.haystack.parser.util.parserDOM;
import com.haystack.service.CatalogService;
import com.haystack.service.ClusterService;
import com.haystack.service.ModelService;
import com.haystack.util.ConfigProperties;
import junit.framework.TestCase;
import com.haystack.domain.Query;

import java.sql.SQLException;

/**
 * Created by qadrim on 15-07-15.
 */
public class ModelServiceTestSingleQuery extends TestCase {


    public void testProcessSQL() throws Exception {

        String myQuery =
                "WITH all_sales AS (\n" +
                        " SELECT d_year\n" +
                        "       ,i_brand_id\n" +
                        "       ,i_class_id\n" +
                        "       ,i_category_id\n" +
                        "       ,i_manufact_id\n" +
                        "       ,SUM(sales_cnt) AS sales_cnt\n" +
                        "       ,SUM(sales_amt) AS sales_amt\n" +
                        " FROM (SELECT d_year\n" +
                        "             ,i_brand_id\n" +
                        "             ,i_class_id\n" +
                        "             ,i_category_id\n" +
                        "             ,i_manufact_id\n" +
                        "             ,cs_quantity - COALESCE(cr_return_quantity,0) AS sales_cnt\n" +
                        "             ,cs_ext_sales_price - COALESCE(cr_return_amount,0.0) AS sales_amt\n" +
                        "       FROM catalog_sales JOIN item ON i_item_sk=cs_item_sk\n" +
                        "                          JOIN date_dim ON d_date_sk=cs_sold_date_sk\n" +
                        "                          LEFT JOIN catalog_returns ON (cs_order_number=cr_order_number \n" +
                        "                                                    AND cs_item_sk=cr_item_sk)\n" +
                        "       WHERE i_category='Shoes'\n" +
                        "       UNION\n" +
                        "       SELECT d_year\n" +
                        "             ,i_brand_id\n" +
                        "             ,i_class_id\n" +
                        "             ,i_category_id\n" +
                        "             ,i_manufact_id\n" +
                        "             ,ss_quantity - COALESCE(sr_return_quantity,0) AS sales_cnt\n" +
                        "             ,ss_ext_sales_price - COALESCE(sr_return_amt,0.0) AS sales_amt\n" +
                        "       FROM store_sales JOIN item ON i_item_sk=ss_item_sk\n" +
                        "                        JOIN date_dim ON d_date_sk=ss_sold_date_sk\n" +
                        "                        LEFT JOIN store_returns ON (ss_ticket_number=sr_ticket_number \n" +
                        "                                                AND ss_item_sk=sr_item_sk)\n" +
                        "       WHERE i_category='Shoes'\n" +
                        "       UNION\n" +
                        "       SELECT d_year\n" +
                        "             ,i_brand_id\n" +
                        "             ,i_class_id\n" +
                        "             ,i_category_id\n" +
                        "             ,i_manufact_id\n" +
                        "             ,ws_quantity - COALESCE(wr_return_quantity,0) AS sales_cnt\n" +
                        "             ,ws_ext_sales_price - COALESCE(wr_return_amt,0.0) AS sales_amt\n" +
                        "       FROM web_sales JOIN item ON i_item_sk=ws_item_sk\n" +
                        "                      JOIN date_dim ON d_date_sk=ws_sold_date_sk\n" +
                        "                      LEFT JOIN web_returns ON (ws_order_number=wr_order_number \n" +
                        "                                            AND ws_item_sk=wr_item_sk)\n" +
                        "       WHERE i_category='Shoes') sales_detail\n" +
                        " GROUP BY d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id)\n" +
                        " SELECT  prev_yr.d_year AS prev_year\n" +
                        "                          ,curr_yr.d_year AS year\n" +
                        "                          ,curr_yr.i_brand_id\n" +
                        "                          ,curr_yr.i_class_id\n" +
                        "                          ,curr_yr.i_category_id\n" +
                        "                          ,curr_yr.i_manufact_id\n" +
                        "                          ,prev_yr.sales_cnt AS prev_yr_cnt\n" +
                        "                          ,curr_yr.sales_cnt AS curr_yr_cnt\n" +
                        "                          ,curr_yr.sales_cnt-prev_yr.sales_cnt AS sales_cnt_diff\n" +
                        "                          ,curr_yr.sales_amt-prev_yr.sales_amt AS sales_amt_diff\n" +
                        " FROM all_sales curr_yr, all_sales prev_yr\n" +
                        " WHERE curr_yr.i_brand_id=prev_yr.i_brand_id\n" +
                        "   AND curr_yr.i_class_id=prev_yr.i_class_id\n" +
                        "   AND curr_yr.i_category_id=prev_yr.i_category_id\n" +
                        "   AND curr_yr.i_manufact_id=prev_yr.i_manufact_id\n" +
                        "   AND curr_yr.d_year=2000\n" +
                        "   AND prev_yr.d_year=2000-1\n" +
                        "   AND CAST(curr_yr.sales_cnt AS DECIMAL(17,2))/CAST(prev_yr.sales_cnt AS DECIMAL(17,2))<0.9\n" +
                        " ORDER BY sales_cnt_diff\n" +
                        " limit 100;";
        //myQuery = " select a as b from T;";
        Query qry = new Query();
        qry.setQueryText(myQuery);


        ConfigProperties configProperties = new ConfigProperties();

        configProperties.loadProperties();

        ClusterService clusterService = new ClusterService(configProperties);

        // Load Table stats into memory
        Tables tablelist = clusterService.getTablesfromCluster();

        ModelService ms = new ModelService();

        // Set the Cached Tables into the Model for future annotation
        ms.setTableList(tablelist);
        //ms.annotateModel(qry,clusterService.tablelist);

        ms.processSQL(1, qry, 400.5, 3, "public");

        ms.scoreModel();
        String str = ms.getModelJSON();

        System.out.print(str);
    }


    public void testRefresh() throws Exception {


        Integer clusterId = 36;


        ConfigProperties configProperties = new ConfigProperties();

        configProperties.loadProperties();

        ClusterService clusterService = new ClusterService(configProperties);

        clusterService.refresh(clusterId);
    }

    public void testWorkload() throws Exception {
        Integer workloadId = 9;   // 35 for tpc-ds, 20 for citi queries

        ConfigProperties configProperties = new ConfigProperties();

        configProperties.loadProperties();

        CatalogService catalogService = new CatalogService(configProperties);

        String result = catalogService.processWorkload(workloadId);
        System.out.print("test finished");
    }
}