package com.hellion23.tuplediff.api;

import com.hellion23.tuplediff.api.listener.CompareEventListener;
import com.hellion23.tuplediff.api.monitor.Monitor;
import com.hellion23.tuplediff.api.monitor.Monitorable;
import com.hellion23.tuplediff.api.monitor.Nameable;

import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.logging.Logger;

/**
 * Created by Hermann on 9/28/2014.
 */
public class TupleComparison implements Nameable, Monitorable
{
    private static final Logger logger = Logger.getLogger(TupleComparison.class.getName());
    String name;
    ThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(2);
    TupleStream leftStream, rightStream;
    TupleKey key;
    CompareEventListener compareEventListener;
    Monitor monitor;
    boolean alreadyRun = false;
    boolean shouldCompare = true;
    ComparisonResult result;

    public TupleComparison (Config config) {
        key = config.tupleKey;
        leftStream = config.leftStream;
        rightStream = config.rightStream;
        compareEventListener = config.compareEventListener;
        result = new ComparisonResult(compareEventListener);
        this.name = config.getName() + " TupleComparison";
    }


    public ComparisonResult compare () {
        assert(!alreadyRun);
        try {
            monitor.handleEvent(this, Monitor.STARTED);
            leftStream.setTupleKey(key);
            leftStream.setName("LEFT STREAM");
            leftStream.init();
            rightStream.setTupleKey(key);
            leftStream.setName("RIGHT STREAM");
            rightStream.init();

            validateSchemas(leftStream, rightStream);
            compareEventListener.init(leftStream.getSchema());

            monitor.registerMonitorable(this);
            monitor.registerMonitorable(leftStream);
            monitor.registerMonitorable(rightStream);
            monitor.registerMonitorable(compareEventListener);

            executor.execute(new StreamRunnable(leftStream));
            executor.execute(new StreamRunnable(rightStream));

            compareTuples();

        }
        catch (Exception ex) {
            logger.severe(getName() + " encountered exception comparing " + ex.getMessage());
            monitor.handleEvent(this, Monitor.FAILED, ex);
        }
        finally {
            alreadyRun = true;
            monitor.handleEvent(this, Monitor.ENDED);
            monitor.reportResults(result);
        }
        return result;
    }

    private void compareTuples() {
        Tuple left, right;
        while (shouldCompare) {
            if (leftStream.hasNext() && rightStream.hasNext()) {
                left = leftStream.getNext();
                handleComparisonEvent (CompareEvent.TYPE.DATA_LEFT, left, null, null);
                right = rightStream.getNext();
                handleComparisonEvent (CompareEvent.TYPE.DATA_RIGHT, null, right, null);
                while (shouldCompare) {
                    int comp = left.getKey().compareTo(right.getKey());
                    if (comp == 0) {
                        List <String> breakFields = compareRows (left, right);
                        if (breakFields != null) {
                            handleComparisonEvent (CompareEvent.TYPE.PAIR_BREAK, left, right, breakFields);
                        }
                        else {
                            monitor.handleEvent(this, Monitor.TUPLE_PAIR_MATCHED);
                        }
                        if (leftStream.hasNext() && rightStream.hasNext()) {
                            left = leftStream.getNext();
                            handleComparisonEvent (CompareEvent.TYPE.DATA_LEFT, left, null, null);
                            right = rightStream.getNext();
                            handleComparisonEvent (CompareEvent.TYPE.DATA_RIGHT, null, right, null);
                        }
                        else break;
                    }
                    else if (comp < 0) {
                        handleComparisonEvent (CompareEvent.TYPE.LEFT_BREAK, left, null, null);
                        if (leftStream.hasNext()) {
                            left = leftStream.getNext();
                            handleComparisonEvent (CompareEvent.TYPE.DATA_LEFT, left, null, null);
                        }
                        else {
                            handleComparisonEvent (CompareEvent.TYPE.PAIR_BREAK, null, right, null);
                            break;
                        }
                    }
                    else {
                        handleComparisonEvent (CompareEvent.TYPE.RIGHT_BREAK, null, right, null);
                        if (rightStream.hasNext()) {
                            right = rightStream.getNext();
                            handleComparisonEvent (CompareEvent.TYPE.DATA_RIGHT, null, right, null);
                        }
                        else {
                            handleComparisonEvent (CompareEvent.TYPE.LEFT_BREAK, left, null, null);
                            break;
                        }
                    }
                }
            }

            while (shouldCompare && leftStream.hasNext()) {
                left = leftStream.getNext();
                handleComparisonEvent (CompareEvent.TYPE.DATA_LEFT, left, null, null);
                monitor.handleEvent(this, Monitor.TUPLE_LEFT_BREAK);
                handleComparisonEvent (CompareEvent.TYPE.LEFT_BREAK, left, null, null);
            }

            while (shouldCompare && rightStream.hasNext()) {
                right = rightStream.getNext();
                handleComparisonEvent (CompareEvent.TYPE.DATA_RIGHT, null, right, null);
                monitor.handleEvent(this, Monitor.TUPLE_RIGHT_BREAK);
                handleComparisonEvent (CompareEvent.TYPE.RIGHT_BREAK, null, right, null);
            }
        }
    }

    protected void handleComparisonEvent (CompareEvent.TYPE event, Tuple left, Tuple right, List<String> breakFields) {
        switch (event) {
            case DATA_LEFT:
                monitor.handleEvent(this, Monitor.TUPLE_LEFT_SEEN);
                break;
            case DATA_RIGHT:
                monitor.handleEvent(this, Monitor.TUPLE_RIGHT_SEEN);
                break;
            case PAIR_MATCHED:
                monitor.handleEvent(this, Monitor.TUPLE_PAIR_MATCHED, left, right, null);
                break;
            case PAIR_BREAK:
                monitor.handleEvent(this, Monitor.TUPLE_PAIR_BREAK);
                storeEvent(new CompareEvent(event, left, right, breakFields));
                break;
            case LEFT_BREAK:
                monitor.handleEvent(this, Monitor.TUPLE_LEFT_BREAK);
                storeEvent(new CompareEvent(event, left, null, null));
                break;
            case RIGHT_BREAK:
                monitor.handleEvent(this, Monitor.TUPLE_RIGHT_BREAK);
                storeEvent(new CompareEvent(event, null, right, null));
                break;
        }

    }

    private void storeEvent (CompareEvent event) {
        if (event.leftTuple!=null) {
            event.leftTuple.fullyPopulate();
        }
        result.addComparisonEvent(event);
    }

    private List<String> compareRows(Tuple left, Tuple right) {
        return null;
    }

    static class StreamRunnable implements Runnable {
        TupleStream tupleStream;
        public StreamRunnable (TupleStream tupleStream) {
            this.tupleStream = tupleStream;
        }
        @Override
        public void run() {
            tupleStream.open();
        }
    }

    private void validateSchemas(TupleStream left, TupleStream right) {

    }

    @Override
    public void setMonitor(Monitor monitor) {
        this.monitor = monitor;
    }

    @Override
    public void stop() {
        shouldCompare = false;
    }

    public boolean isStopped() {
        return shouldCompare;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return this.name;
    }
}
