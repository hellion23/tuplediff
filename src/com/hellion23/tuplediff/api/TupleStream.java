package com.hellion23.tuplediff.api;

import com.hellion23.tuplediff.api.monitor.Monitorable;
import com.hellion23.tuplediff.api.monitor.Nameable;

/**
 * @author: Hermann Leung
 * Date: 6/3/14
 */
public interface TupleStream extends Nameable, Monitorable {
    public TupleStreamKey getTupleStreamKey();
    public void setTupleStreamKey(TupleStreamKey key);
    public void init();
    public void open();
    public Schema getSchema();
    public boolean hasNext();
    public Tuple getNext();
    public void close();
    public boolean isClosed();
}
