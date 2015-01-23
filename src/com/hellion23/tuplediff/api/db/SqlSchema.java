package com.hellion23.tuplediff.api.db;

import com.hellion23.tuplediff.api.Schema;
import com.hellion23.tuplediff.api.TupleStreamKey;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

/**
 * Created by margaret on 9/30/2014.
 */
public class SqlSchema extends Schema <SqlField> {
    String vendor;
    String version;

    public SqlSchema(TupleStreamKey tupleStreamKey,
                     Collection<SqlField> keyFields,
                     Collection<SqlField> compareFields,
                     Collection<SqlField> allFields) {
        this.allFields = new ArrayList<SqlField> (allFields);
        this.compareFields = new ArrayList<SqlField> (compareFields);
        this.keyFields = new ArrayList<SqlField> (keyFields);
        this.tupleStreamKey = tupleStreamKey;
        this.strict = false;
        allFieldMap = new HashMap<String, SqlField>();
        for (SqlField f : allFields) {
            allFieldMap.put(f.getName(), f);
        }
    }

    public String getVendor() {
        return vendor;
    }

    public void setVendor(String vendor) {
        this.vendor = vendor;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }
}