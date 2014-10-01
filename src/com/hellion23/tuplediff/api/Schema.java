package com.hellion23.tuplediff.api;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author: Hermann Leung
 * Date: 6/3/14
 */
public class Schema <T extends Field> {
    List<T> allFields;
    List<T> keyFields;
    List<T> compareFields;
    boolean strict;

    public void Schema (Collection<T> keyFields, Collection<T> compareFields, boolean strict) {
        this.keyFields = new ArrayList<T> (keyFields);
        this.compareFields = new ArrayList<T> (compareFields);
        this.allFields = new ArrayList<T> (keyFields);
        allFields.addAll(compareFields);
        this.strict = strict;
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

    public boolean isStrict() {
        return strict;
    }

}
