package com.hellion23.tuplediff.api;

import com.hellion23.tuplediff.api.db.SqlField;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * @author: Hermann Leung
 * Date: 6/3/14
 */
public abstract class Schema <T extends Field> {
    protected List<T> allFields;
    protected Map<String, T> allFieldMap;
    protected List<T> keyFields;
    protected List<T > compareFields;
    protected TupleStreamKey tupleStreamKey;
    protected boolean strict = false;

    public TupleStreamKey getTupleStreamKey() {
        return tupleStreamKey;
    }

    public List<T> getAllFields() {
        return allFields;
    }
    public List<T> getKeyFields() {
        return keyFields;
    }
    public List<T> getCompareFields() {
        return compareFields;
    }
    public T getField (String name) {return allFieldMap.get(name);}

    public boolean isStrict() {
        return strict;
    }

}
