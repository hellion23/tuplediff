package com.hellion23.tuplediff.api.compare;

import java.util.Comparator;
import java.util.Objects;

/**
 * If the thing to be compared is a Comparable, then do the default Comparable comparison on it.
 * If it is not, first check to see if it is equals, if not, then do a full toString() on it and then compare
 * that String. This is obviously extremely expensive and should be avoided!
 *
 */
public class ObjectComparator implements TypedComparator<Object> {
    public static Comparator<Comparable> comparableComparator = Comparator.comparing((Comparable c) -> c);

    @Override
    public int compare(Object o1, Object o2) {
        if (o1 instanceof Comparable && o2 instanceof Comparable) {
            return comparableComparator.compare((Comparable)o1, (Comparable) o2);
        }
        else {
            return Objects.equals(o1, o2) ? 0 : comparableComparator.compare(o1.toString(), o2.toString());
        }
    }

    @Override
    public String toString () {
        return "ObjectComparator";
    }
}
