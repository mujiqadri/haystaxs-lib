package com.haystack.domain;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;
import com.haystack.parser.util.IntTypeAdapter;
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
    public HashMap<String, Recommendation> recommendations;
    //public HashMap<String,Join> joinHashMap;  // Commented because join is now created as child for Left and Right Table

    private static Logger log = LoggerFactory.getLogger(Tables.class.getName());

    public Tables(){
        tableHashMap = new HashMap<String, Table>();
        recommendations = new HashMap<String, Recommendation>();
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
                    try {
                        BigDecimal value = BigDecimal.valueOf(src);
                        return new JsonPrimitive(value);
                    } catch (Exception e) {
                        return new JsonPrimitive(-1);
                    }

                }
            });
            gsonBuilder.registerTypeAdapter(int.class, new IntTypeAdapter());
            gsonBuilder.registerTypeAdapter(Integer.class, new IntTypeAdapter()).create();

            //gson = gsonBuilder.create();

            Gson objGson =  gsonBuilder.setPrettyPrinting().create();
            sw = objGson.toJson(tableHashMap);
        }catch(Exception e){
            log.error("Error generating json " + e.toString());
        }
        return sw.toString();
    }

    // TODO

    public float getSizeOfTable(String tableName_schema_key) {
        return 0;
    }
    public Column findColumn(String schemaName, String tableName, String columnName, String current_search_path) {
        Table tbl = null;
        Column col = null;
        try {
            if (schemaName == null) {
                schemaName = findSchema(tableName, current_search_path);
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

    public String findSchema(String tablename, String current_search_path) {
        // === TODO take the tablename and search for the schema
        Set<Map.Entry<String, Table>> set = tableHashMap.entrySet();

        Integer schemaRank = -1;
        String return_Schema = null;
        String arrSP[] = current_search_path.split(",");

        for(Map.Entry<String, Table> entry : set ){
            String key = entry.getKey();
            Table currTbl = entry.getValue();
            if (currTbl.tableName.equals(tablename)){
                Integer newSchemaRank = getSchemaRank(currTbl.schema.toString(), arrSP);
                if ((schemaRank == -1) || (newSchemaRank < schemaRank)) {
                    schemaRank = newSchemaRank;
                    return_Schema = currTbl.schema.toString();
                }
            }
        }
        return (return_Schema);
    }

    private Integer getSchemaRank(String schemaName, String[] arrSP) {
        for (int i = 0; i < arrSP.length; i++) {
            if (arrSP[i].equals(schemaName)) {
                return (i + 1);
            }
        }
        return -1;
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

    public String loadfromStats(DBConnectService dbConn, boolean return_Json) {
        String jsonResult = "";
        try {


            String sqlTbl = "\t\tSELECT tbl.oid as table_oid, sch.nspname as schema_name, relname as table_name,\n" +
                    // Changed Muji 23Jan 2016 :tbl.reltuples::bigint
                    //"\t\t  to_char(tbl.reltuples::numeric,'9999999999999999D99') as NoOfRows,\n" +
                    "\t\t  tbl.reltuples::bigint as NoOfRows,\n" +

                    "\t\t  tbl.relpages as relPages,\n" +
                    "\t\t  ((tbl.relpages::numeric * 32) /(1024*1024)) sizeinGB,\n" +
                    "\t\t  tbl.oid                  AS tableId, relkind ,\n" +
                    "\t\t  CASE WHEN tbl.relstorage = 'a' THEN 'append-optimized'\n" +
                    "\t\t  WHEN tbl.relstorage = 'h' THEN 'heap'\n" +
                    "\t\t  WHEN tbl.relstorage = 'p' THEN 'parquet'\n" +
                    "\t\t  WHEN tbl.relstorage = 'c' THEN 'columnar'\n" +
                    "\t\t  ELSE 'error'\n" +
                    "\t\t  END                               AS storage_Mode,\n" +
                    "\t\ttbl.relnatts AS noOfColumns,\n" +
                    "\n" +
                    "\t\t-- appendonly table stats,\n" +
                    "\t\tCASE WHEN columnstore = TRUE THEN 't' ELSE 'f' END as IsColumnar,\n" +
                    "\t\tcol.compresstype AS compresstype,\n" +
                    "\t\tCOALESCE(col.compresslevel,0) AS compresslevel,\n" +
                    "\t\tcase when length(coalesce(par.tablename,'')) = 0 then 0 else 1 end as IsPartitioned,\n" +
                    "\t\tarray_to_string(dk.attrnums, ',') as dkarray \n" +
                    "\t\tFROM pg_class AS tbl INNER JOIN pg_namespace AS sch\n" +
                    "\t\tON tbl.relnamespace = sch.oid\n" +
                    "\t\tLEFT OUTER JOIN gp_distribution_policy dk ON tbl.oid = dk.localoid\n" +
                    "\t\tLEFT OUTER JOIN pg_appendonly col ON tbl.oid = col.relid\n" +
                    "\t\t\n" +
                    "\t\tLEFT OUTER JOIN ( select distinct schemaname, tablename from pg_partitions ) as par\n" +
                    "\t\t ON sch.nspname = par.schemaname AND tbl.relname = par.tablename\n" +
                    "\t\tWHERE   tbl.relname not in ( select partitiontablename from pg_partitions) -- Don't fetch leaf partitions\n" +
                    "\t\tand tbl.oid not in (select reloid from pg_exttable) -- exclude external tables\n" +
                    "\t\tand nspname not in ('information_schema','pg_catalog','gptext')\n" +
                    "\t\tand relkind = 'r'  -- Check if its a Table\n" +
                    "\t\tand relname not like 'rpt%'\n" +
                    "\t\tand relname not like  'es1_%'\n" +
                    "\t\tand ((tbl.relpages > 0 ) OR (tbl.relpages = 0 and par.tablename is not null))\n" +
                    "\t\t--AND tbl.relchecks = 0\n" +
                    "\t\torder by 3 -- desc\n";
            ResultSet rsTbl = dbConn.execQuery(sqlTbl);

            while (rsTbl.next()) {   // Fetch all parent level tables, if table is partitioned then load all Partitions
                Table tbl = new Table();
                tbl.oid = rsTbl.getString("table_oid");
                tbl.database = dbConn.getDB();
                tbl.schema = rsTbl.getString("schema_name");
                tbl.tableName = rsTbl.getString("table_name");

                tbl.stats = new TableStats();
                tbl.stats.storageMode = rsTbl.getString("storage_mode");
                tbl.stats.relPages = rsTbl.getInt("relpages");
                tbl.stats.noOfColumns = Integer.parseInt(rsTbl.getString("noOfColumns"));
                tbl.stats.isColumnar = (rsTbl.getString("IsColumnar").equals("t")) ? true : false;
                String sNoOfRows = rsTbl.getString("NoOfRows");

                tbl.stats.noOfRows = Double.valueOf(sNoOfRows);
                tbl.stats.sizeOnDisk = Float.parseFloat(rsTbl.getString("sizeinGB"));
                tbl.stats.sizeUnCompressed = Float.parseFloat(rsTbl.getString("sizeinGB"));
                tbl.stats.compressType = rsTbl.getString("compressType");
                tbl.stats.compressLevel = Integer.parseInt(rsTbl.getString("compressLevel"));
                //tbl.stats.compressionRatio = Float.parseFloat(rsTbl.getString("compressRatio"));
                //tbl.stats.skew = Float.parseFloat(rsTbl.getString("skew"));
                tbl.stats.sizeForDisplayCompressed = getSizePretty(tbl.stats.sizeOnDisk); // Normalize size and unit to display atleast 2 digits
                tbl.stats.sizeForDisplayUnCompressed = getSizePretty(tbl.stats.sizeUnCompressed); // Normalize size and unit to display atleast 2 digits
                tbl.dkArray = rsTbl.getString("dkarray");

                if (rsTbl.getInt("IsPartitioned") == 1) {   // Table is partitioned fetch partition details, and Rollup NoOfRows and size to Topmost Parent Table
                    // Load Partitions
                    String partSQL = "\n" +
                            "\t\t\tselect C.relpages, C.reltuples, partitiontablename, partitionname, parentpartitiontablename, parentpartitionname, partitiontype, partitionlevel, partitionrank,\n" +
                            "\t\t\tpartitionposition, partitionlistvalues, partitionrangestart, partitionstartinclusive, partitionrangeend, partitionendinclusive, \n" +
                            "\t\t\tpartitioneveryclause, partitionisdefault, partitionboundary\n" +
                            "\t\t\tfrom pg_partitions A \n" +
                            "\t\t\t\tINNER JOIN pg_namespace B ON B.nspname = A.schemaname \n" +
                            "\t\t\t\tINNER JOIN pg_class C ON C.relname = A.partitiontablename AND C.relnamespace = B.oid\n" +
                            "\t\t\twhere A.schemaname = '" + tbl.schema + "' and A.tablename = '" + tbl.tableName + "' order by partitionlevel, partitionposition; \n";

                    ResultSet rsPar = dbConn.execQuery(partSQL);

                    while (rsPar.next()) {


                        Partition partition = new Partition();
                        partition.relPages = rsPar.getInt("relpages");
                        partition.relTuples = rsPar.getInt("reltuples");
                        partition.tableName = rsPar.getString("partitiontablename");
                        partition.partitionName = rsPar.getString("partitionname");
                        partition.type = rsPar.getString("partitiontype");
                        partition.level = rsPar.getInt("partitionlevel");
                        partition.rank = rsPar.getInt("partitionrank");
                        partition.position = rsPar.getInt("partitionposition");
                        partition.listValues = rsPar.getString("partitionlistvalues");
                        partition.rangeStart = rsPar.getString("partitionrangestart");
                        partition.rangeStartInclusive = rsPar.getBoolean("partitionstartinclusive");
                        partition.rangeEnd = rsPar.getString("partitionrangeend");
                        partition.rangeEndInclusive = rsPar.getBoolean("partitionendinclusive");
                        partition.everyClause = rsPar.getString("partitioneveryclause");
                        partition.isDefault = rsPar.getBoolean("partitionisdefault");
                        partition.boundary = rsPar.getString("partitionboundary");

                        partition.parentPartitionTableName = rsPar.getString("parentpartitiontablename");
                        partition.parentPartitionName = rsPar.getString("parentpartitionname");

                        tbl.stats.addChildStats(partition.relPages, partition.relTuples);
                        tbl.addPartition(tbl.partitions, partition);


                    }
                    rsPar.close();

                    // Recalculate TableSize based on partitions
                    tbl.stats.sizeOnDisk = ((tbl.stats.relPages * 32) / (1024 * 1024));
                    tbl.stats.sizeUnCompressed = tbl.stats.sizeOnDisk;
                    tbl.stats.sizeForDisplayCompressed = getSizePretty(tbl.stats.sizeOnDisk); // Normalize size and unit to display atleast 2 digits
                    tbl.stats.sizeForDisplayUnCompressed = getSizePretty(tbl.stats.sizeUnCompressed); // Normalize size and unit to display atleast 2 digits
                }

                // Get Column Details
                String sqlCol = "\n" +
                        "\t\tselect  A.column_name, A.ordinal_position, A.data_type,  NOT(A.ordinal_position != ALL (D.attrnums)) as IsDK,\n" +
                        "\t\t\tA.character_maximum_length, A.numeric_precision, A.numeric_precision_radix, A.numeric_scale, \n" +
                        "\t\t\tcase when E.Partitionlevel is null then false else true end as IsPartitioned, E.partitionlevel, position_in_partition_key\n" +
                        "\t\tfrom information_schema.columns A\n" +
                        "\t\t\tINNER JOIN pg_class B ON  B.relname = A.table_name\n" +
                        "\t\t\tINNER JOIN pg_namespace C ON  B.relnamespace = C.oid and C.nspname = A.table_schema\n" +
                        "\t\t\tLEFT OUTER JOIN gp_distribution_policy D on B.oid = D.localoid \n" +
                        "\t\t\tLEFT OUTER JOIN pg_partition_columns E on E.schemaname = A.table_schema AND E.tablename = A.table_name AND E.columnname = A.column_name\n" +
                        "\t\twhere A.table_schema = '" + tbl.schema + "' and A.table_name = '" + tbl.tableName + "' \n" +
                        "\t\torder by A.ordinal_position";

                ResultSet rsCol = dbConn.execQuery(sqlCol);

                while (rsCol.next()) {
                    Column col = new Column();
                    col.column_name = rsCol.getString("column_name");
                    col.ordinal_position = rsCol.getInt("ordinal_position");
                    col.data_type = rsCol.getString("data_type");
                    col.isDK = Boolean.parseBoolean(rsCol.getString("isdk"));
                    col.character_maximum_length = rsCol.getInt("character_maximum_length");
                    col.numeric_precision = rsCol.getInt("numeric_precision");
                    col.numeric_precision_radix = rsCol.getInt("numeric_precision_radix");
                    col.numeric_scale = rsCol.getInt("numeric_scale");
                    col.isPartitioned = rsCol.getBoolean("IsPartitioned");
                    col.partitionLevel = rsCol.getInt("partitionlevel");
                    col.positionInPartitionKey = rsCol.getInt("position_in_partition_key");
                    if (col.isPartitioned == true) {
                        tbl.partitionColumn.put(col.column_name, col);
                    }
                    tbl.columns.put(col.column_name, col);
                }
                rsCol.close();
                String key = tbl.schema + ":" + tbl.tableName;
                tbl.setDistributionKey();
                tableHashMap.put(key, tbl);
            }
            rsTbl.close();

            if (return_Json) {
                jsonResult = getJSON();
            }
        } catch (Exception e) {
            log.error("Error in loading tables from Stats" + e.toString());
        }
        return jsonResult;
    }

    // Load function reads the table structure from Haystack DB (Tables and Columns)
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
                tbl.stats.noOfRows = rsTbl.getDouble("NoOfRows");
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
