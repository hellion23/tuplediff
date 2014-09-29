package com.hellion23.tuplediff.api;

import com.hellion23.tuplediff.api.listener.TupleBreakListener;
import com.hellion23.tuplediff.api.monitor.Monitor;
import com.hellion23.tuplediff.api.monitor.Monitorable;
import com.hellion23.tuplediff.api.monitor.Nameable;

/**
 * Created by Hermann on 9/28/2014.
 */
public class TupleComparison implements Nameable, Monitorable {

    public void compare (CompareConfig config) {
        TupleKey key = config.tupleKey;
        TupleStream leftStream = config.leftStream;
        TupleStream rightStream = config.rightStream;
        leftStream.setTupleKey(key);
        rightStream.setTupleKey(key);
        TupleBreakListener tupleBreakListener = config.tupleBreakListener;
        tupleBreakListener.init();

        leftStream.init();
        rightStream.init();

        validateSchemas(leftStream, rightStream);

        Monitor monitor = config.monitor;
        monitor.registerMonitorable(this);
        monitor.registerMonitorable(leftStream);
        monitor.registerMonitorable(rightStream);
        monitor.registerMonitorable(tupleBreakListener);


    }

    private void validateSchemas(TupleStream leftStream, TupleStream rightStream) {
    }

    @Override
    public void setMonitor(Monitor monitor) {

    }

    @Override
    public void stop(boolean isNormal) {

    }

    @Override
    public void setName(String name) {

    }

    @Override
    public String getName() {
        return null;
    }
}
