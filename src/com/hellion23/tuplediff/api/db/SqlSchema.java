package com.hellion23.tuplediff.api.db;

import com.hellion23.tuplediff.api.Schema;
import com.hellion23.tuplediff.api.TupleStreamKey;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Created by margaret on 9/30/2014.
 */
public class SqlSchema extends Schema <SqlField> {
    String vendor;
    String version;

    public SqlSchema(TupleStreamKey tupleStreamKey, Collection<SqlField> keyFields, Collection<SqlField> allFields) {
        this.allFields = new ArrayList<SqlField> (allFields);
        this.compareFields = new ArrayList<SqlField> (allFields);
        this.compareFields.removeAll(keyFields);
        this.keyFields = new ArrayList<SqlField> (keyFields);
        this.tupleStreamKey = tupleStreamKey;
        this.strict = false;
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