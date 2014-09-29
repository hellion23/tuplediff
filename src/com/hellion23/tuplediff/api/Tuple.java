package com.hellion23.tuplediff.api;

/**
 * @author: Hermann Leung
 * Date: 6/3/14
 */
public class Tuple {
    Schema schema;

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public Comparable get (String fieldName) {
        return null;
    }
    public Comparable get (int index) {
        return null;
    }
    public Comparable getKey (){
        return null;
    }
    public Comparable [] getValues (){
        return null;
    }

    public void fullyPopulate() {

    }
}

