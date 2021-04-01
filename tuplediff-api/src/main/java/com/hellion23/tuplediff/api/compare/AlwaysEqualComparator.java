package com.hellion23.tuplediff.api.compare;

/**
* Created by hleung on 7/30/2017.
*/
public class AlwaysEqualComparator implements TypedComparator<Object> {
    @Override
    public int compare(Object o1, Object o2) {
        return 0;
    }

    @Override
    public String toString () {
        return "AlwaysEqualComparator";
    }
}
