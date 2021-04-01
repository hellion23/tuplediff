package com.hellion23.tuplediff.api.compare;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * HashMap already does a deep equals, this will compare Comparable keys then the Comparable value of the same Map.Entry
 * in the collection map.entrySet(). The comparison will return the first non-zero int from iterating the Map.Entry.
 * This object is to support TDTuple values resulting from the conversion of Strings into more complex non-primitive
 * representations such as XML or JSON.
 *
 * This does NOT act like a TreeMap even though the key is expected to be a Comparable since this is a LinkedHashMap.
 * Do not expect lookups to return
 *
 * Not threadsafe
 *
 * @param <K> Comparable key
 * @param <V> Comparable object
 */
public class ComparableLinkedHashMap<K extends Comparable, V extends Comparable> extends LinkedHashMap <K,V> implements Comparable <ComparableLinkedHashMap> {
    private final static Logger LOG = LoggerFactory.getLogger(ComparableLinkedHashMap.class);

    public static final Comparator COMPARATOR = Comparator.nullsFirst(new ComparableHashMapComparator());

    public ComparableLinkedHashMap(int initialCapacity, float loadFactor) {
        super(initialCapacity, loadFactor);
    }

    public ComparableLinkedHashMap(int initialCapacity) {
        super(initialCapacity);
    }

    public ComparableLinkedHashMap() {
        super();
    }

    public ComparableLinkedHashMap(Map m) {
        super(m);
    }

    @Override
    public int compareTo(ComparableLinkedHashMap o) {
        return COMPARATOR.compare(this, o);
    }

    static class ComparableHashMapComparator implements TypedComparator <ComparableLinkedHashMap> {

        @Override
        public int compare(ComparableLinkedHashMap o1, ComparableLinkedHashMap o2) {
            if (o1 == o2) return 0;

            int ans = Integer.compare(o1.size(), o2.size());
            if (ans != 0) return ans;

            Iterator <Map.Entry<? extends Comparable, ? extends Comparable>> it1 = o1.entrySet().iterator();
            Iterator <Map.Entry<? extends Comparable, ? extends Comparable>> it2 = o2.entrySet().iterator();
            while (it1.hasNext()) {
                Map.Entry<? extends Comparable, ? extends Comparable> c1 = it1.next();
                Map.Entry<? extends Comparable, ? extends Comparable> c2 = it2.next();

                ans = ComparatorLibrary.NULL_SAFE_COMPARATOR.compare(c1.getKey(), c2.getKey());
//                LOG.info(String.format("Comparing KEY %s WITH %s: answer: %s", c1.getKey(), c2.getKey(), ans));
                if (ans != 0) return ans;

                ans = ComparatorLibrary.NULL_SAFE_COMPARATOR.compare(c1.getValue(), c2.getValue());
//                LOG.info(String.format("Comparing VALUE %s WITH %s: answer: %s", c1.getValue(), c2.getValue(), ans));
                if (ans != 0) return ans;

            }
            return 0;
        }

        @Override
        public String toString() {
            return "ComparableHashMapComparator";
        }
    }


}
