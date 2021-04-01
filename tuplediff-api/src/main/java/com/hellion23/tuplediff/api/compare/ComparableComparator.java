package com.hellion23.tuplediff.api.compare;

import java.util.Comparator;

/**
 * When 2 Comparable objects are of the same class, tuplediff will select this as the default.
 *
* Created by hleung on 7/30/2017.
*/
public class ComparableComparator implements TypedComparator<Comparable> {
    public static Comparator <Comparable> comparator = Comparator.comparing((Comparable c) -> c);
    @Override
    public int compare(Comparable o1, Comparable o2) {
        return comparator.compare(o1, o2);
    }

    @Override
    public String toString () {
        return "ComparableComparator";
    }
}
