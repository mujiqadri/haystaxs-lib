package com.haystack.util;

/**
 * Created by qadrim on 15-03-03.
 */
public class Credentials {

    public Credentials(){

    }

    private String hostName;
    private String port;
    private String database;
    private String userName;
    private String password;

    public void setCredentials(String hostName, String port, String database, String userName, String password)
    {
        this.hostName = hostName;
        this.port = port;
        this.database = database;
        this.userName = userName;
        this.password = password;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getPort() {
        return port;
    }

    public String getUserName() {
        return userName;
    }

    public String getPassword() {
        return password;
    }

    public String getDatabase() {

        return database;
    }

    public String getHostName() {

        return hostName;
    }
}
