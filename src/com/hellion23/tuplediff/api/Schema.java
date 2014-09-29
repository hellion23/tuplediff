package com.hellion23.tuplediff.api;

/**
 * @author: Hermann Leung
 * Date: 6/3/14
 */
public class Schema {
    Field[] allFields;
    Field[] keyFields;
    Field[] compareFields;
    boolean strict;

    public void Schema (Field [] keyFields, Field[] compareFields, boolean strict) {
        this.keyFields = keyFields;
        this.compareFields = compareFields;
        this.strict = strict;
        allFields = new Field[this.keyFields.length + this.compareFields.length];
        for (int i=0;i<keyFields.length; i++) {
            allFields[i]=keyFields[i];
        }
        for (int i=0; i<compareFields.length;i++){
            allFields[keyFields.length+i] = compareFields[i];
        }
    }

    public Field[] getAllFields() {
        return allFields;
    }

    public Field[] getKeyFields() {
        return keyFields;
    }
    public Field[] getCompareFields() {
        return compareFields;
    }

    public boolean isStrict() {
        return strict;
    }

}
