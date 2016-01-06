package com.haystack.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;

/**
 * Created by qadrim on 15-11-22.
 */
public class HSException {
    private DBConnectService dbConnect;
    private String haystackSchema;
    private ConfigProperties configProperties = new ConfigProperties();
    private static Logger log = LoggerFactory.getLogger(HSException.class.getName());


    public HSException(String error_class_method, String error_text, String error_exception_msg, String error_context_info, Integer userId) {
        try {
            configProperties.loadProperties();
            this.haystackSchema = configProperties.properties.getProperty("main.schema");
            dbConnect = new DBConnectService(DBConnectService.DBTYPE.POSTGRES, "");
            dbConnect.connect(configProperties.getHaystackDBCredentials());
            error_context_info = error_context_info.replace("'", " ");
            String sql = String.format("INSERT INTO %s.internal_errors(error_type, error_class, error_text, error_exception_msg, error_context_info, occured_on,user_id)" +
                    " VALUES ('LIB','%s', '%s', '%s', '%s', now(), %d);", haystackSchema, error_class_method, error_text, error_exception_msg, error_context_info, userId);

            dbConnect.execNoResultSet(sql);
        } catch (Exception e) {
            log.error("Exception while connecting to Haystack Database to log Exception" + e.toString());
        }
    }

    public HSException(String error_class_method, String error_text, String error_exception_msg, String error_context_info, String userName) {
        try {
            configProperties.loadProperties();
            this.haystackSchema = configProperties.properties.getProperty("main.schema");
            dbConnect = new DBConnectService(DBConnectService.DBTYPE.POSTGRES, "");
            dbConnect.connect(configProperties.getHaystackDBCredentials());

            String sql = String.format("select user_id from %s.users where user_name ='%s'", haystackSchema, userName);
            ResultSet rsUser = dbConnect.execQuery(sql);
            rsUser.next();
            Integer userId = rsUser.getInt("user_id");

            error_context_info = error_context_info.replace("'", " ");

            sql = String.format("INSERT INTO %s.internal_errors(error_type, error_class, error_text, error_exception_msg, error_context_info, occured_on,user_id)" +
                    " VALUES ('LIB','%s', '%s', '%s', '%s', now(), %d );", haystackSchema, error_class_method, error_text, error_exception_msg, error_context_info, userId);

            dbConnect.execNoResultSet(sql);
        } catch (Exception e) {
            log.error("Exception while connecting to Haystack Database to log Exception" + e.toString());
        }
    }
}
