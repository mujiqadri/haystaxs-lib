package com.haystack.domain;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;
import com.haystack.util.DBConnectService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.math.BigInteger;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.math.BigDecimal;

import org.codehaus.jackson.map.ObjectMapper;


import com.google.gson.Gson;
import com.google.gson.*;
import java.lang.reflect.Type;
/**
 * Created by qadrim on 15-04-23.
 */
public class Tables  {
    public HashMap<String,Table> tableHashMap;
    //public HashMap<String,Join> joinHashMap;  // Commented because join is now created as child for Left and Right Table

    private static Logger log = LoggerFactory.getLogger(Tables.class.getName());

    public Tables(){
        tableHashMap = new HashMap<String, Table>();
    }
    public void add(String key, Table t){
        tableHashMap.put(key, t);
    }

    // String tablename can contain alias e.g. MyTable:i2 where i2 is the alias separated by semicolon

    public String getJSON(){
        ObjectMapper mapper = new ObjectMapper();
        String sw = "";
        try {
            //Gson objGson = new Gson();
            GsonBuilder gsonBuilder = new GsonBuilder();

            gsonBuilder.registerTypeAdapter(Double.class,  new JsonSerializer<Double>() {
                @Override
                public JsonElement serialize(final Double src, final Type typeOfSrc, final JsonSerializationContext context) {
                    BigDecimal value = BigDecimal.valueOf(src);

                    return new JsonPrimitive(value);
                }
            });

            //gson = gsonBuilder.create();

            Gson objGson =  gsonBuilder.setPrettyPrinting().create();
            sw = objGson.toJson(tableHashMap);
        }catch(Exception e){
            log.error("Error generating json " + e.toString());
        }
        return sw.toString();
    }
    public Column findColumn(String schemaName, String tableName, String columnName ){
        Table tbl = null;
        Column col = null;
        try {
            if (schemaName == null) {
                schemaName = findSchema(tableName);
            }
            if (schemaName == null) { // Derived table return null
                return col;
            }
            tbl = findTable(schemaName, tableName);

            Set<Map.Entry<String,Column>> set = tbl.columns.entrySet();

            for(Map.Entry<String, Column> entry : set ){
                String key = entry.getKey();
                Column currCol = entry.getValue();
                if (currCol.column_name.toLowerCase().equals(columnName.toLowerCase())){
                        return currCol;
                }
            }
        } catch (Exception e){
            return col;
        }
        return col;
    }

    public Table findTable(String schemaName, String tableName){
        String key = schemaName + ":" + tableName;
        Table tbl = tableHashMap.get(key);
        return tbl;
    }
    public String findSchema(String tablename){
        // === TODO take the tablename and search for the schema
        Set<Map.Entry<String, Table>> set = tableHashMap.entrySet();

        for(Map.Entry<String, Table> entry : set ){
            String key = entry.getKey();
            Table currTbl = entry.getValue();
            if (currTbl.tableName.equals(tablename)){
                return currTbl.schema.toString();
            }
        }
        return null;
    }
    public Table findTableByAlias(String alias){

        return new Table();
    }


    public String getSizePretty(float sizeOnDisk){
        String result = "";
        float newSize = 0;

        if (sizeOnDisk > 1024) { // size greater than a Terabyte, convert it to TB
            newSize = sizeOnDisk / 1024;
            result = String.format("%.2f", newSize);
            result += " TB";
        }
        if ((sizeOnDisk < 1024) && (sizeOnDisk >= 1)) { // size should remain in GB
            result = String.format("%.2f", sizeOnDisk);
            result += " GB";
        }
        if (sizeOnDisk < 1) {  // size should be in MB
            newSize = sizeOnDisk * 1024;
            result = String.format("%.2f", newSize);
            result += " MB";
        }

        return result;
    }
    public void load(DBConnectService dbConn){
        try{
            // Get all Tables from load them into Tables objects (All Schemas)
            log.info("Loading haystack tables and columns into memory");

            String sqlTbl = "select table_oid, db_name, schema_name, table_name, COALESCE(storage_mode,'NONE') storage_mode,\n" +
                    "COALESCE(noofcols,-1) noofcols, COALESCE(IsColumnar,'F') IsColumnar, COALESCE(NoOfRows,-1) NoOfRows,\n" +
                    "COALESCE(sizeInGB,-1) sizeInGB, COALESCE(sizeInGBu,-1) sizeInGBu, COALESCE(compressType,'NONE') compressType,\n" +
                    "COALESCE(compressLevel,-1) compressLevel, COALESCE(compressRatio,-1) compressRatio, COALESCE(skew,-1) skew,\n" +
                    " COALESCE(score,-1) score, COALESCE(dkarray,'NONE') dkarray\n" +
                    "from haystack.tables A\n" +
                    "where (A.runid, A.schema_name) in (\n" +
                    "                    select max(run_id), run_schema\n" +
                    "                    from haystack.run_log\n" +
                    "                    group by run_schema\n" +
                    "                    )\n" +
                    "                    order by schema_name, table_name";
            ResultSet rsTbl = dbConn.execQuery(sqlTbl);

            while (rsTbl.next()){
                Table tbl = new Table();
                tbl.oid = rsTbl.getString("table_oid");
                tbl.database = rsTbl.getString("db_name");
                tbl.schema = rsTbl.getString("schema_name");
                tbl.tableName = rsTbl.getString("table_name");

                tbl.stats = new TableStats();
                tbl.stats.storageMode = rsTbl.getString("storage_mode");
                tbl.stats.noOfColumns = Integer.parseInt(rsTbl.getString("noofcols"));
                tbl.stats.isColumnar =  (rsTbl.getString("IsColumnar").equals("t")) ? true : false;
                tbl.stats.noOfRows = new BigInteger(rsTbl.getString("NoOfRows"));
                tbl.stats.sizeOnDisk = Float.parseFloat(rsTbl.getString("sizeinGB"));
                tbl.stats.sizeUnCompressed = Float.parseFloat(rsTbl.getString("sizeinGBu"));
                tbl.stats.compressType = rsTbl.getString("compressType");
                tbl.stats.compressLevel = Integer.parseInt(rsTbl.getString("compressLevel"));
                tbl.stats.compressionRatio = Float.parseFloat(rsTbl.getString("compressRatio"));
                tbl.stats.skew = Float.parseFloat(rsTbl.getString("skew"));

                tbl.stats.sizeForDisplayCompressed = getSizePretty(tbl.stats.sizeOnDisk); // Normalize size and unit to display atleast 2 digits
                tbl.stats.sizeForDisplayUnCompressed = getSizePretty(tbl.stats.sizeUnCompressed); // Normalize size and unit to display atleast 2 digits


                //03Sep2015 commented out because of rename score to ModelScore, no need to load score from DB
                // tbl.stats.score = Float.parseFloat(rsTbl.getString("score"));

                tbl.dkArray = rsTbl.getString("dkarray");


                // Get all columns and populate them in appropriate table object
                String sqlCol = "select B.table_oid, column_name, ordinal_position, data_type, coalesce(isdk, 'f') isdk, \n" +
                        "coalesce(character_maximum_length, -1) character_maximum_length, \n" +
                        "coalesce(numeric_precision,-1) numeric_precision,\n" +
                        "coalesce(numeric_precision_radix,-1) numeric_precision_radix,\n" +
                        "coalesce(numeric_scale,-1) numeric_scale\n" +
                        "from haystack.columns A, haystack.Tables B\n" +
                        "where A.table_oid = B.table_oid \n" +
                        "and B.table_oid = '" + tbl.oid +"'" +
                        "and (B.runid, B.schema_name) in \n" +
                        "                        (\n" +
                        "                        select max(run_id), run_schema\n" +
                        "                        from haystack.run_log\n" +
                        "                        group by run_schema\n" +
                        "                        )\n" +
                        "order by B.table_oid, ordinal_position";
                ResultSet rsCol = dbConn.execQuery(sqlCol);

                while(rsCol.next()){
                    String tableOID = rsCol.getString("table_oid");

                    //tbl.columns = new HashMap<String, Column>(tbl.stats.noOfColumns);

                    Column col = new Column();
                    col.column_name = rsCol.getString("column_name");
                    col.ordinal_position = rsCol.getInt("ordinal_position");
                    col.data_type = rsCol.getString("data_type");
                    col.isDK = Boolean.parseBoolean(rsCol.getString("isdk"));
                    col.character_maximum_length = rsCol.getInt("character_maximum_length");
                    col.numeric_precision = rsCol.getInt("numeric_precision");
                    col.numeric_precision_radix = rsCol.getInt("numeric_precision_radix");
                    col.numeric_scale = rsCol.getInt("numeric_scale");

                    tbl.columns.put(col.column_name, col);

                }
                rsCol.close();
                String key = tbl.schema + ":" + tbl.tableName;
                tbl.setDistributionKey();
                tableHashMap.put(key, tbl);
            }
            rsTbl.close();
        }
        catch(Exception e){
            log.error("Error in loading tables " + e.toString());

        }
    }
}
