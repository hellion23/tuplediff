package com.hellion23.tuplediff.api.compare;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


/**
 * The LinkedList is already does a deep equals comparison, but is not implement Comparator. This class implements
 * the compareTo using a Comparator that compares lists of Comparable items. The implementation iterates through
 * the List in order and does a compareTo on each Comparable item, returning the first non-zero value.
 * This object is to support TDTuple values resulting from the conversion of Strings into more complex non-primitive
 * representations such as XML or JSON.
 *
 * Not threadsafe.
 *
 * @param <T>
 */
public class ComparableLinkedList <T extends Comparable> extends LinkedList<T> implements Comparable <ComparableLinkedList> {
    private final static Logger LOG = LoggerFactory.getLogger(ComparatorLibrary.class);

    public static final Comparator COMPARATOR = Comparator.nullsFirst(new ComparableLinkedListComparator());

    public ComparableLinkedList() {
        super();
    }

    public ComparableLinkedList(Collection<? extends T> c) {
        super(c);
    }

    @Override
    public int compareTo(ComparableLinkedList o) {
        return COMPARATOR.compare(this, o);
    }

    static class ComparableLinkedListComparator implements TypedComparator<ComparableLinkedList> {

        @Override
        public int compare(ComparableLinkedList o1, ComparableLinkedList o2) {
            if (o1 == o2) return 0;

            int ans = Integer.compare(o1.size(), o2.size());
            if (ans != 0) return ans;

            Iterator <? extends Comparable> it1 = o1.iterator();
            Iterator <? extends Comparable> it2 = o2.iterator();
            while (it1.hasNext()) {
                ans = ComparatorLibrary.NULL_SAFE_COMPARATOR.compare(it1.next(), it2.next());
                if (ans != 0) return ans;
            }
            return 0;
        }

        @Override
        public String toString() {
            return "ComparableLinkedListComparator";
        }
    }

}
