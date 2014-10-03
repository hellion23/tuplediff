package com.hellion23.tuplediff.api;

import java.util.List;

/**
 * A TupleStreamKey encapsulates several concepts:
 * 1) A unique ID generator for a Tuple (i.e. no Tuple with the same key can be emitted by a TupleStream
 *      more than once)
 * 2) An enforcement of total order of a TupleStream's Tuple emission.
 * 3) A means by which a Tuple on one TupleStream can be matched with a Tuple from another TupleStream.
 *
 * With respect to relational DB's, a TupleStreamKey may be thought of as a primary key.
 *
 * A TupleStreamKey is shared by both TupleStreams being compared. The createKeyForTuple method is used by both
 * TupleStreams to generate the key.
 *
 * @author: Hermann Leung
 * Date: 9/26/2014
 */
public class TupleStreamKey
{
    List<Field> fields;

    public TupleStreamKey(List<Field> fields) {
        this.fields = fields;
    }

    public List<Field> getFields() {
        return fields;
    }

    public void setFields(List<Field> fields) {
        this.fields = fields;
    }

    public TKComparable createKeyForTuple (Tuple tuple) {
        return new TKComparable(tuple);
    }

    static class TKComparable implements Comparable<TKComparable> {
        Comparable c[];

        public TKComparable (Tuple t) {
            List<Field> k = t.getSchema().getKeyFields();
            c = new Comparable [k.size()];
            for (int i = 0; i<c.length; i++) {
                c[i] = t.getValue(k.get(i));
            }
        }

        @Override
        public int compareTo(TKComparable o) {
            int ans = 0;
            for (int i=0; i<c.length; i++) {
                final Comparable a = c[i];
                final Comparable b = o.c[i];
                if (a == null && b == null)
                    continue;
                else if (a==null)
                    return -1;
                else if (b==null)
                    return 1;
                else
                    ans = a.compareTo(b);
                if (ans !=0 )
                    return ans;
            }
            return ans;
        }
    }
}
