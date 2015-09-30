-- done ----
alter table work.store_Sales set distributed by (ss_item_sk, ss_ticket_number);
alter table work.catalog_sales set distributed by (cs_item_sk, cs_order_number);
alter table work.store_returns set distributed by (sr_item_sk, sr_ticket_number);

-- running----

-- pending ---


alter table work.catalog_returns set distributed by (cr_item_sk, cr_order_number);
alter table work.web_sales set distributed by (ws_item_sk, ws_order_number);
alter table work.web_returns set distributed by (wr_item_sk, wr_order_number);
alter table work.inventory set distributed by (inv_date_sk, inv_item_sk, inv_warehouse_sk);
alter table work.store set distributed by (s_store_sk);
alter table work.call_center set distributed by (cc_call_center_sk);
alter table work.catalog_page set distributed by (cp_catalog_page_sk);
alter table work.web_site set distributed by (web_site_sk);
alter table work.web_page set distributed by (wp_web_page_sk);
alter table work.warehouse set distributed by (w_warehouse_sk);
alter table work.customer set distributed by (c_customer_sk);
alter table work.customer_address set distributed by (ca_address_sk);
alter table work.customer_demographics set distributed by (cd_demo_sk);
alter table work.date_Dim set distributed by (d_date_sk);
alter table work.household_demographics set distributed by (hd_demo_sk);
alter table work.item set distributed by (i_item_sk);
alter table work.income_band set distributed by (ib_income_band_sk);
alter table work.promotion set distributed by (p_promo_sk);
alter table work.reason set distributed by (r_reason_sk);


