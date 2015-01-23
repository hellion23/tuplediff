package com.hellion23.tuplediff.api;

import com.hellion23.tuplediff.api.monitor.Nameable;

/**
 *  A provider that returns Tuples in an order defined by the TupleStreamKey.
 *
 * @author: Hermann Leung
 * Date: 6/3/14
 */
public interface TupleStream extends Nameable {
    public TupleStreamKey getTupleStreamKey();
    public void setTupleStreamKey(TupleStreamKey key);
    public Schema getSchema();
    public void setSchema(Schema schema);
    public boolean hasNext();
    public Tuple getNext();

    /**
     * This method prepares the TupleStream to be read.
     */
    public void open();

    /**
     * This method should clean up any resources, close silently and be idempotent.
     */
    public void close();
}
