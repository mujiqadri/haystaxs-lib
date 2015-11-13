package com.haystack.util;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.*;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class DBConnectService {

    static Logger log = LoggerFactory.getLogger(DBConnectService.class.getName());
    public Connection getConn() {
        return conn;
    }
    private static String sqlDirectory;

    public enum DBTYPE {
        GREENPLUM, NETEZZA, TERADATA, HAWQ, POSTGRES
    }

    DBTYPE dbTYPE;
    private ScriptRunner scriptRunner;
    private Connection conn;
    private String host;
    private String port;
    private String dbName;
    private String user;
    private String pass;

    public DBConnectService(DBTYPE dbtype, String sqlDirectory) {
        dbTYPE = dbtype;
        this.sqlDirectory = sqlDirectory;
    }

    public void setCredentials(Credentials cred) {

        this.host = cred.getHostName();
        this.port = cred.getPort();
        this.dbName = cred.getDatabase();
        this.user = cred.getUserName();
        this.pass = cred.getPassword();
    }

    public String getDB() {
        return this.dbName;
    }

    private String getdbConnectURL() {
        StringBuilder url = new StringBuilder();
        switch (this.dbTYPE) {
            case GREENPLUM:
                url.append("jdbc:postgresql://").append(this.host).append(":").append(this.port).append("/").append(this.dbName);
                break;
            case HAWQ:
                url.append("jdbc:postgresql://").append(this.host).append(":").append(this.port).append("/").append(this.dbName);
                break;
            case POSTGRES:
                url.append("jdbc:postgresql://").append(this.host).append(":").append(this.port).append("/").append(this.dbName);
                break;
            default:
                log.error("DBTYPE ERROR " + this.dbTYPE);
        }
        return url.toString();
    }

    public boolean connect(String host, String port, String dbName, String user, String pass) throws SQLException, ClassNotFoundException {

        this.host = host;
        this.port = port;
        this.dbName = dbName;
        this.user = user;
        this.pass = pass;
        boolean status = connect();

        return status;
    }

    public boolean connect(Credentials credentials)throws SQLException, ClassNotFoundException{
        this.setCredentials(credentials);
        return connect();
    }

    public void close() {
        try {
            conn.close();
        } catch (Exception e) {

        }
    }
    public boolean connect() throws SQLException, ClassNotFoundException {
        if (host.isEmpty() || port.isEmpty() || dbName.isEmpty() || user.isEmpty() || pass.isEmpty()) {
            log.error("Database credentials missing, All fields are mandatory.");
            throw new SQLException("Database credentials missing, All fields are mandatory.");
        }
        switch (this.dbTYPE) {
            case GREENPLUM:
                Class.forName("org.postgresql.Driver");
                break;
            case HAWQ:
                Class.forName("org.postgresql.Driver");
                break;
            case POSTGRES:
                Class.forName("org.postgresql.Driver");
                break;
            default:
                log.error("DBTYPE ERROR " + this.dbTYPE);
        }

        this.conn = DriverManager.getConnection(this.getdbConnectURL(), this.user, this.pass);

        log.trace("DBConnect Success:" + this.host );
        return true;
    }

    public void execScript(String scriptFile) throws SQLException, IOException{
        scriptRunner = new ScriptRunner(this.conn, true, true);

        String fullFilePath = System.getProperty("user.dir") + sqlDirectory + scriptFile + ".sql";

        Reader reader = this.getReaderforFile(fullFilePath);

        scriptRunner.runScript(reader);
    }

    public String getSQLfromFile(String sqlFile) {
        String sql = "";
        try {

            String fullFilePath = System.getProperty("user.dir") + sqlDirectory + sqlFile + ".sql";
            BufferedReader in = new BufferedReader(new FileReader(fullFilePath));
            StringBuffer sb = new StringBuffer();
            String str;
            while ((str = in.readLine()) != null) {
                sb.append(str + "\n");
            }
            in.close();
            sql = sb.toString();
        } catch (Exception ex) {
            log.error(ex.toString());
        }
        return sql;
    }

    public PreparedStatement prepareStatement(String selectSQL ) throws SQLException{
        return conn.prepareStatement(selectSQL);
    }
    public static BufferedReader getReaderforFile (String sqlFile){
        BufferedReader reader = null;
        try {

            //String fullFilePath = System.getProperty("user.dir") + sqlDirectory + sqlFile + ".sql";
            reader = new BufferedReader(new FileReader(sqlFile));

        } catch (Exception ex) {
            log.error(ex.toString());
        }
        return reader;
    }

    public ResultSet execQuery(String query) throws SQLException {

        return this.conn.createStatement().executeQuery(query);

    }
    public int execNoResultSet(String query) throws SQLException {
        return this.conn.createStatement().executeUpdate(query);
    }

    public int update(String table, Map keyColumns, Map valColumns) throws SQLException {

        StringBuilder setKeys = new StringBuilder();
        StringBuilder whereKeys = new StringBuilder();

        Iterator it = keyColumns.entrySet().iterator();
        // Get where keys
        while (it.hasNext()) {

            Map.Entry pair = (Map.Entry) it.next();
            String col = (String) pair.getKey();
            whereKeys.append(col).append("=");

            if (keyColumns.get(col) instanceof String) {
                whereKeys.append("'").append(keyColumns.get(col)).append("' AND ");
            } else whereKeys.append(keyColumns.get(col)).append(" AND ");
            it.remove();

        }
        Iterator it2 = valColumns.entrySet().iterator();
        // Get where keys
        while (it2.hasNext()) {

            Map.Entry pair = (Map.Entry) it.next();
            String col = (String) pair.getKey();
            setKeys.append(col).append("=");

            if (valColumns.get(col) instanceof String) {
                setKeys.append("'").append(valColumns.get(col)).append("' AND ");
            } else setKeys.append(valColumns.get(col)).append(" AND ");
            it.remove();

        }
        whereKeys.setLength(whereKeys.length() - 4);
        setKeys.setLength(setKeys.length() - 4);

        String query = String.format("UPDATE %s SET %s WHERE %s", table,
                setKeys.toString(), whereKeys.toString());

        return this.conn.createStatement().executeUpdate(query);
    }

    public int insert(String table, Map values) throws SQLException {

        StringBuilder columns = new StringBuilder();
        StringBuilder vals = new StringBuilder();

        Iterator it = values.entrySet().iterator();
        while (it.hasNext()) {

            Map.Entry pair = (Map.Entry) it.next();
            String col = (String) pair.getKey();
            columns.append(col).append(",");

            if (values.get(col) instanceof String) {
                vals.append("'").append(values.get(col)).append("', ");
            } else vals.append(values.get(col)).append(",");
            it.remove(); // avoids a ConcurrentModificationException
        }


        columns.setLength(columns.length() - 1);
        vals.setLength(vals.length() - 1);

        String query = String.format("INSERT INTO %s (%s) VALUES (%s)", table,
                columns.toString(), vals.toString());

        return this.conn.createStatement().executeUpdate(query);
    }

}
