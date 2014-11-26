package com.hellion23.tuplediff.api;

import java.util.Map;

/**
 * @author: Hermann Leung
 * Date: 6/3/14
 */
public class Tuple <T extends Field> {
    Schema schema;
    Map<T, Comparable> fieldToValues;
    Comparable key;

     public Tuple(Schema schema, Map<T, Comparable> row) {
        this.schema = schema;
        this.fieldToValues = row;
        key = schema.getTupleStreamKey().createKeyForTuple(this);
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public Comparable getKey (){
        return key;
    }

    public void setKey (Comparable key) {
        this.key = key;
    }

    public Comparable getValue (T f) {
        return fieldToValues.get(f);
    }

    public Map<T, Comparable> getAllValues () { return fieldToValues; }
}

