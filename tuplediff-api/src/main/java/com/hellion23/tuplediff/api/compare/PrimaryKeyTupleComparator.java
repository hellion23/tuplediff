package com.hellion23.tuplediff.api.compare;

import com.hellion23.tuplediff.api.model.TDTuple;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * This Comparator is how TDCompare compares between any 2 Tuples based on a subset of fields (primary keys).
 * This is true within a Stream and between Streams.
 * Also a nullsFirst comparator.
 *
 */
@Slf4j
public class PrimaryKeyTupleComparator<R extends TDTuple> implements Comparator<R> {
    private final static Logger LOG = LoggerFactory.getLogger(PrimaryKeyTupleComparator.class);
    String [] pk;
    FieldComparator [] comps;
    LinkedHashMap<String, FieldComparator> map = new LinkedHashMap<>();

    public PrimaryKeyTupleComparator(final List<String> primaryKey, final List<FieldComparator> pkcomps) {
        log.info("PrimaryKeyTupleComparator for keys: " + primaryKey);
        pk = primaryKey.toArray (new String [primaryKey.size()]);
        comps = pkcomps.toArray(new FieldComparator [pkcomps.size()]);
        for (int i=0; i<pk.length; i++) {
            map.put(pk[i], comps[i]);
        }
    }

    @Override
    public int compare(R left, R right) {
        int result =0;
        for (int i=0; i<pk.length; i++) {
            result = comps[i].compare(left.getValue(pk[i]), right.getValue(pk[i]));
            if (result != 0) {
                break;
            }
        }
//        LOG.info("PK Comparing result " + result + " :\n " + left + "\n " + right);
        return result;
    }

    public LinkedHashMap<String, FieldComparator> map() {
        return map;
    }

    public int indexOf (String column) {
        for (int i=0; i<pk.length; i++) {
            if (pk[i].equals(column)) return i;
        }
        return -1;
    }

    @Override
    public String toString() {
        return "PrimaryKeyTupleComparator{" + map +"}";
    }
}
