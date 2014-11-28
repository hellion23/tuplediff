package com.hellion23.tuplediff.api.listener;

import com.hellion23.tuplediff.api.CompareEvent;
import com.hellion23.tuplediff.api.Schema;

/**
 * Created by Hermann on 9/28/2014.
 */
public interface CompareEventListener {
    public void handleCompareEvent(CompareEvent compareEvent);
    public void init(Schema leftSchema, Schema rightSchema);
    public void close();
}
