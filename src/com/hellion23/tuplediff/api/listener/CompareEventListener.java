package com.hellion23.tuplediff.api.listener;

import com.hellion23.tuplediff.api.CompareEvent;
import com.hellion23.tuplediff.api.Schema;
import com.hellion23.tuplediff.api.monitor.Monitorable;

/**
 * Created by Hermann on 9/28/2014.
 */
public interface CompareEventListener extends Monitorable {
    public void handleTupleBreak (CompareEvent compareEvent);
    public void init(Schema schema);
}
