package com.hellion23.tuplediff.api;

import com.hellion23.tuplediff.api.db.SqlTupleStream;
import com.hellion23.tuplediff.api.monitor.Monitor;
import com.hellion23.tuplediff.api.monitor.Nameable;
import com.hellion23.tuplediff.api.monitor.TupleComparisonMonitor;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author: Hermann Leung
 * Date: 11/24/2014
 */
public class TupleComparison implements Nameable {
    TupleStream leftStream;
    TupleStream rightStream;
    Config config;
    private ThreadPoolExecutor ex = new ScheduledThreadPoolExecutor(2);
    private ComparisonResult result;
    private Monitor monitor;
    volatile boolean notCancelled = true;
    volatile boolean alreadyRan = false;
    String name;

    public TupleComparison (Config config) {
        assert (config != null);
        this.config = config;

        this.leftStream = config.getLeftStream();
        assert (this.leftStream !=null);
        this.rightStream = config.getRightStream();
        assert (this.rightStream != null);

        this.monitor = config.getMonitor();
        if (this.monitor == null) {
            monitor = new TupleComparisonMonitor(this, config);
        }
        this.result = new ComparisonResult(config);
        this.name = config.getName() + "-["+System.currentTimeMillis()+"]";
    }


    protected void initialize () throws TupleDiffException {
        // Setup & validate streams:
        validateSchemas(leftStream, rightStream);

        // initialize Comparators:
        initComparators();

        // Setup listeners:
        if (config.getCompareEventListener() != null) {
            config.getCompareEventListener().init(leftStream.getSchema(), rightStream.getSchema());
        }

        // open up streams for reading
        prepareStreamsForReading();
    }

    protected void initComparators () {
//    TODO Add initialization logic

    }

    protected void prepareStreamsForReading () {
        CountDownLatch doneSignal = new CountDownLatch(2);

        StreamRunnable leftRunnable = new StreamRunnable(monitor, leftStream, doneSignal);
        StreamRunnable rightRunnable = new StreamRunnable(monitor, rightStream, doneSignal);

        ex.execute(leftRunnable);
        ex.execute(rightRunnable);

        // Wait until both streams are ready to be read.
        try {doneSignal.await();} catch (InterruptedException e) {}

        if (leftRunnable.getException() != null) {
            rightRunnable.stop();
            throw new TupleDiffException("Error with opening " + leftStream.getName() + " stream ",
                    leftStream, leftRunnable.getException()) ;
        }

        if (rightRunnable.getException() != null) {
            leftRunnable.stop();
            throw new TupleDiffException("Error with opening " + rightStream.getName() + " stream ",
                    rightStream, rightRunnable.getException()) ;
        }
    }

    public synchronized ComparisonResult compare() throws TupleDiffException{
        assert (!alreadyRan);
        try {
            // Initialize everything:
            initialize();

            // do actual comparison of data
            compareTuples();

            monitor.reportEvent(this, Monitor.EVENT_STOP_NORMAL);
        }
        catch (Exception ex) {
            TupleDiffException tde = null;

            // Exception with identifiable source.
            if (ex instanceof TupleDiffException) {
                tde = (TupleDiffException) ex;
            }
            // Unknown source exception; wrap it and designate this class as the source
            else {
                tde = new TupleDiffException("Unexpected exception during TupleDiff comparison. "
                        + ex.getMessage(), this, ex);
            }

            // Report the event:
            monitor.reportEvent(tde.getSource(), Monitor.EVENT_STOP_ABNORMAL, tde);

            // Re-throw the exception if this was an un-expected exception. i.e. the comparison was not cancelled,
            // but was instead abnormally/unexpected cancelled.
            if (notCancelled) {
                throw tde;
            }
        }
        finally {
            alreadyRan= true;
            cleanup();
        }
        return result;
    }

    private void cleanup () {
        leftStream.close();
        rightStream.close();
        if (config.getCompareEventListener() != null) {
            config.getCompareEventListener().close();
        }
    }

    private void compareTuples() {
        Tuple left, right;
        while (notCancelled) {
            if (leftStream.hasNext() && rightStream.hasNext()) {
                left = leftStream.getNext();
                comparisonEvent(CompareEvent.TYPE.DATA_LEFT, left, null, null);
                right = rightStream.getNext();
                comparisonEvent(CompareEvent.TYPE.DATA_RIGHT, null, right, null);
                while (notCancelled) {
                    int comp = left.getKey().compareTo(right.getKey());
                    if (comp == 0) {
                        List<String> breakFields = getBreakFields(left, right);
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

            while (notCancelled && leftStream.hasNext()) {
                left = leftStream.getNext();
                comparisonEvent(CompareEvent.TYPE.DATA_LEFT, left, null, null);
                comparisonEvent(CompareEvent.TYPE.LEFT_BREAK, left, null, null);
            }

            while (notCancelled && rightStream.hasNext()) {
                right = rightStream.getNext();
                comparisonEvent(CompareEvent.TYPE.DATA_RIGHT, null, right, null);
                comparisonEvent(CompareEvent.TYPE.RIGHT_BREAK, null, right, null);
            }
        }
    }

    protected void comparisonEvent (CompareEvent.TYPE event, Tuple left, Tuple right, List<String> breakFields) {
        monitor.reportEvent(this, "COMPARE_EVENT", event);
        switch (event) {
            case DATA_LEFT:
            case DATA_RIGHT:
            case PAIR_MATCHED:
                break;
            case PAIR_BREAK:
            case LEFT_BREAK:
            case RIGHT_BREAK:
                result.handleCompareEvent(new CompareEvent(event, left, right, breakFields));
                break;
        }
    }

    public void cancel () {
        notCancelled = false;
        if (!alreadyRan) {
            cleanup();
        }
    }

    private List<String> getBreakFields(Tuple left, Tuple right) {
//        TODO: Do logic for calculating break fields.
        return null;
    }

    private void validateSchemas(TupleStream left, TupleStream right) {
        // TODO complete code for validating schemas.
        final List<Field> leftFields = left.getSchema().getAllFields();
        final List<Field> rightFields = right.getSchema().getAllFields();

        if (leftFields.size() != rightFields.size()) {
            throw new TupleDiffException(
                "Unexpected # of fields in Stream schemas. Left = <"+leftFields.size() + "> Right = <"
                    + rightFields.size() + "> ",
                 this
            );
        }


    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    static class StreamRunnable implements Runnable {
        Monitor monitor;
        TupleStream tupleStream;
        CountDownLatch doneSignal;
        TupleDiffException exception;
        Thread runningThread;

        public StreamRunnable (Monitor monitor, TupleStream tupleStream, CountDownLatch doneSignal) {
            this.monitor = monitor;
            this.tupleStream = tupleStream;
            this.doneSignal = doneSignal;
        }
        @Override
        public void run() {
            try {
                runningThread = Thread.currentThread();
                monitor.reportEvent(tupleStream, Monitor.EVENT_START);
                tupleStream.open();
                monitor.reportEvent(tupleStream, Monitor.EVENT_STOP_NORMAL);
            }
            catch (Exception ex) {
                this.exception = new TupleDiffException("Encountered error in " +
                        tupleStream.getName() + " : " + ex.getMessage(), tupleStream, ex);
                monitor.reportEvent(tupleStream, Monitor.EVENT_STOP_ABNORMAL, this.exception);
                // Force the other stream to stop.
                this.doneSignal.countDown();
            }
            finally {
                // This stream has completed.
                this.doneSignal.countDown();
            }
        }

        public void stop () {
                tupleStream.close();
        }

        public TupleDiffException getException () {
            return exception;
        }
    }

    public static void main (String args[]) throws Exception{
        Connection conn = null;
        SqlTupleStream leftStream = SqlTupleStream.create(conn);
        Schema leftSchema = leftStream.getSchema();
        Config config = new Config();
        config.setLeftStream(leftStream);
        config.setRightStream(SqlTupleStream.create(conn));
        TupleComparison comparison = new TupleComparison(config);
        ComparisonResult result = comparison.compare();
        List<CompareEvent> compareEvents = new ArrayList<CompareEvent>();

//        TODO: Make flow-y API work.
//        Config.configure()
//                .usingSql("select * from DIM_FUND")
//                .withLeftConnection(conn)
//                .withRightConnection(conn)
//                .withKey(new String[] {"ABC", "DEF"})
//                .drainResultsTo(compareEvents)
//        )  ;
        System.out.println ("DONE!");
    }
}
