package com.hellion23.tuplediff.api;

import com.hellion23.tuplediff.api.db.SqlSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author: Hermann Leung
 * Date: 6/3/14
 */
public class Tuple {
    Schema schema;
    Map<Field, Comparable> fieldToValues;
    Comparable key;

    public Tuple(Schema schema, Map<Field, Comparable> row) {
        this.schema = schema;
        this.fieldToValues = row;
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

    public Comparable getValue (Field f) {
        return fieldToValues.get(f);
    }

    public Map<Field, Comparable> getAllValues () { return fieldToValues; }
}

