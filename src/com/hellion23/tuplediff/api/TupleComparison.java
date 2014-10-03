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
    TupleStreamKey key;
    CompareEventListener compareEventListener;
    Monitor monitor;
    boolean alreadyRun = false;
    boolean shouldCompare = true;
    ComparisonResult result;

    public TupleComparison (Config config) {
        key = config.tupleStreamKey;
        leftStream = config.leftStream;
        rightStream = config.rightStream;
        compareEventListener = config.compareEventListener;
        result = new ComparisonResult(compareEventListener);
        this.name = config.getName() + " TupleComparison";
    }


    public ComparisonResult compare () {
        assert(!alreadyRun);
        try {
            monitor.handleEvent(this, STATE.STARTING, null);
            leftStream.setTupleStreamKey(key);
            leftStream.setName("LEFT STREAM");
            leftStream.init();
            rightStream.setTupleStreamKey(key);
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
            monitor.handleEvent(this, STATE.TERMINATED, STOP_REASON.COMPLETED);
        }
        catch (Exception ex) {
            logger.severe(getName() + " encountered exception comparing " + ex.getMessage());
            if (!isStopped()) {
                // Exceptions that occur after comparison has already stopped are ignored.
                monitor.handleEvent(this, STATE.TERMINATED, STOP_REASON.FAILED, ex);
            }

        }
        finally {
            alreadyRun = true;
            result.setStats(monitor.getAllStats());
        }
        return result;
    }

    private void compareTuples() {
        Tuple left, right;
        while (shouldCompare) {
            if (leftStream.hasNext() && rightStream.hasNext()) {
                left = leftStream.getNext();
                comparisonEvent(CompareEvent.TYPE.DATA_LEFT, left, null, null);
                right = rightStream.getNext();
                comparisonEvent(CompareEvent.TYPE.DATA_RIGHT, null, right, null);
                while (shouldCompare) {
                    int comp = left.getKey().compareTo(right.getKey());
                    if (comp == 0) {
                        List <String> breakFields = compareRows (left, right);
                        if (breakFields != null) {
                            comparisonEvent(CompareEvent.TYPE.PAIR_BREAK, left, right, breakFields);
                        }
                        else {
                            comparisonEvent(CompareEvent.TYPE.PAIR_MATCHED, left, right, null);
                        }
                        if ( leftStream.hasNext() && rightStream.hasNext()) {
                            left = leftStream.getNext();
                            comparisonEvent(CompareEvent.TYPE.DATA_LEFT, left, null, null);
                            right = rightStream.getNext();
                            comparisonEvent(CompareEvent.TYPE.DATA_RIGHT, null, right, null);
                        }
                        else break;
                    }
                    else if (comp < 0) {
                        comparisonEvent(CompareEvent.TYPE.LEFT_BREAK, left, null, null);
                        if ( leftStream.hasNext()) {
                            left = leftStream.getNext();
                            comparisonEvent(CompareEvent.TYPE.DATA_LEFT, left, null, null);
                        }
                        else {
                            comparisonEvent(CompareEvent.TYPE.PAIR_BREAK, null, right, null);
                            break;
                        }
                    }
                    else {
                        comparisonEvent(CompareEvent.TYPE.RIGHT_BREAK, null, right, null);
                        if (rightStream.hasNext()) {
                            right = rightStream.getNext();
                            comparisonEvent(CompareEvent.TYPE.DATA_RIGHT, null, right, null);
                        }
                        else {
                            comparisonEvent(CompareEvent.TYPE.LEFT_BREAK, left, null, null);
                            break;
                        }
                    }
                }
            }

            while (shouldCompare && leftStream.hasNext()) {
                left = leftStream.getNext();
                comparisonEvent(CompareEvent.TYPE.DATA_LEFT, left, null, null);
                comparisonEvent(CompareEvent.TYPE.LEFT_BREAK, left, null, null);
            }

            while (shouldCompare && rightStream.hasNext()) {
                right = rightStream.getNext();
                comparisonEvent(CompareEvent.TYPE.DATA_RIGHT, null, right, null);
                comparisonEvent(CompareEvent.TYPE.RIGHT_BREAK, null, right, null);
            }
        }
    }

    protected void comparisonEvent (CompareEvent.TYPE event, Tuple left, Tuple right, List<String> breakFields) {
        monitor.handleEvent(this, STATE.RUNNING, event);
        switch (event) {
            case DATA_LEFT:
            case DATA_RIGHT:
            case PAIR_MATCHED:
                break;
            case PAIR_BREAK:
            case LEFT_BREAK:
            case RIGHT_BREAK:
                result.addComparisonEvent(new CompareEvent (event, left, right, breakFields));
                break;
        }

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

    public void cancel () {
        stop();
        monitor.handleEvent(this, STATE.TERMINATED, STOP_REASON.CANCEL);

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
