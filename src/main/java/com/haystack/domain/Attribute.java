package com.haystack.domain;

/**
 * Created by qadrim on 15-03-04.
 */
public class Attribute {

    public String tableId;
    public String schema;
    public String tableName;
    public String name;
    public String type;
    public String alias;
    public int length;
    public String nameFQN;

    public Attribute (){

    }
    public Attribute(String name, String type, int length){
        this.name = name;
        this.type = type;
        this.length = length;
    }
    public Attribute(String name, String type){
        this.name = name;
        this.type = type;
    }

    public String getFQDN(){
        return schema + "." + tableName + "." + name ;
    }

}
