package com.hellion23.tuplediff.api;

import com.hellion23.tuplediff.api.listener.TupleBreakListener;
import com.hellion23.tuplediff.api.monitor.Monitor;

/**
 * Created by Hermann on 9/28/2014.
 */
public class CompareConfig {
    TupleStream leftStream;
    TupleStream rightStream;
    TupleKey tupleKey;
    Monitor monitor;
    TupleBreakListener tupleBreakListener;
}
