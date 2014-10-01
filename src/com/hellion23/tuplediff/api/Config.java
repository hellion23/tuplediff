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

    public TupleStream getLeftStream() {
        return leftStream;
    }

    public void setLeftStream(TupleStream leftStream) {
        this.leftStream = leftStream;
    }

    public TupleStream getRightStream() {
        return rightStream;
    }

    public void setRightStream(TupleStream rightStream) {
        this.rightStream = rightStream;
    }

    public TupleKey getTupleKey() {
        return tupleKey;
    }

    public void setTupleKey(TupleKey tupleKey) {
        this.tupleKey = tupleKey;
    }

    public Monitor getMonitor() {
        return monitor;
    }

    public void setMonitor(Monitor monitor) {
        this.monitor = monitor;
    }

    public CompareEventListener getCompareEventListener() {
        return compareEventListener;
    }

    public void setCompareEventListener(CompareEventListener compareEventListener) {
        this.compareEventListener = compareEventListener;
    }
}
