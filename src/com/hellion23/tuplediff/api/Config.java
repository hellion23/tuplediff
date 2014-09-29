package com.hellion23.tuplediff.api;

import com.hellion23.tuplediff.api.listener.CompareEventListener;
import com.hellion23.tuplediff.api.monitor.Monitor;
import com.hellion23.tuplediff.api.monitor.Nameable;

/**
 * Created by Hermann on 9/28/2014.
 */
public class Config implements Nameable {
    TupleStream leftStream;
    TupleStream rightStream;
    TupleKey tupleKey;
    Monitor monitor;
    String name;
    CompareEventListener compareEventListener;

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return this.name;
    }
}
