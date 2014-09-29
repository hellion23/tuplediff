package com.hellion23.tuplediff.api.listener;

import com.hellion23.tuplediff.api.TupleBreak;
import com.hellion23.tuplediff.api.monitor.Monitorable;

/**
 * Created by Hermann on 9/28/2014.
 */
public interface TupleBreakListener extends Monitorable {
    public void handleTupleBreak (TupleBreak tupleBreak);
    public void init();
    public void close();
}
