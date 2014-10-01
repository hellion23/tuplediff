package com.hellion23.tuplediff.api;

import java.util.List;

/**
 * @author: Hermann Leung
 * Date: 9/26/2014
 */
public class TupleKey {
    List<String> fieldNames;

    public List<String> getFieldNames() {
        return fieldNames;
    }

    public void setFieldNames(List<String> fieldNames) {
        this.fieldNames = fieldNames;
    }

    public static class TKComparable implements Comparable<TKComparable> {

        @Override
        public int compareTo(TKComparable o) {
            return 0;
        }
    }
}
