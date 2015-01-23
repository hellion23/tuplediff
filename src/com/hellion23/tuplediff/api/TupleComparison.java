package com.hellion23.tuplediff.api;

import com.hellion23.tuplediff.api.comparator.FieldComparator;
import com.hellion23.tuplediff.api.comparator.FieldComparatorFactory;
import com.hellion23.tuplediff.api.db.SqlTupleStream;
import com.hellion23.tuplediff.api.listener.CompareEventListener;
import com.hellion23.tuplediff.api.listener.ListCompareEventListener;
import com.hellion23.tuplediff.api.monitor.Monitor;
import com.hellion23.tuplediff.api.monitor.Nameable;
import com.hellion23.tuplediff.api.monitor.TupleComparisonMonitor;

import java.sql.Connection;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.logging.Logger;

/**
 * @author: Hermann Leung
 * Date: 11/24/2014
 */
public class TupleComparison implements Nameable {
    public final static String COMPARE_EVENT = "COMPARE_EVENT";
    private static final Logger logger = Logger.getLogger(Field.class.getName());
    TupleStream leftStream;
    TupleStream rightStream;
    Config config;
    private ThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(2);
    private ComparisonResult result;
    volatile boolean notCancelled = true;
    volatile boolean alreadyRan = false;
    String name;
    Map<Field, FieldComparator> fieldComparators;
    protected Monitor monitor;
    protected CompareEventListener compareEventListener;

    public TupleComparison (Config config, Monitor monitor, CompareEventListener compareEventListener) {
        assert (config != null);
        this.config = config;
        this.leftStream = config.getLeftStream();
        assert (this.leftStream !=null);
        this.rightStream = config.getRightStream();
        assert (this.rightStream != null);
        this.name = config.getName();

        if (this.leftStream.getName() == null) {
            this.leftStream.setName( name + " - LEFT");
        }

        if (this.rightStream.getName() == null) {
            this.rightStream.setName( name + " - RIGHT");
        }

        // Setup default Event Listener if none provided
        if (compareEventListener == null)
            this.compareEventListener = new ListCompareEventListener();
        else
            this.compareEventListener = compareEventListener;

        // Setup default monitor if none provided.
        if (monitor == null)
            this.monitor = new TupleComparisonMonitor();
        else
            this.monitor = monitor;

        this.result = new ComparisonResult(this);
    }

    public TupleComparison (Config config) {
        this (config, null, null);
    }

    protected void initialize () throws TupleDiffException {
        // Setup Monitor.
        initMonitor();

        // Setup & validate streams:
        validateSchemas(leftStream, rightStream);

        // initialize Comparators:
        initComparators();

        // Setup listeners:
        initListener();

        // open up streams for reading
        prepareStreamsForReading();
    }

    private void initListener() {
        this.compareEventListener.init(config);
    }

    private void initMonitor() {
        monitor.setTupleComparison(this);
        monitor.setLeftStream(leftStream);
        monitor.setRightStream(rightStream);
        monitor.init();
    }

    protected void initComparators () {
        fieldComparators = new HashMap <Field, FieldComparator>();

        // schemas have already been checked, so whichever schema's fields we use, it should be identical to the other
        // so we'll arbitrarily use the LEFT stream's schema:
        for (Field field : (List <Field>) leftStream.getSchema().getCompareFields()) {
            FieldComparator fc = FieldComparatorFactory.Instance().resolveField(field, config.getComparatorOverrides());
            fieldComparators.put(field, fc);
        }
    }

    protected void prepareStreamsForReading () {
        CountDownLatch doneSignal = new CountDownLatch(2);

        StreamRunnable leftRunnable = new StreamRunnable(monitor, leftStream, doneSignal);
        StreamRunnable rightRunnable = new StreamRunnable(monitor, rightStream, doneSignal);

        executor.execute(leftRunnable);
        executor.execute(rightRunnable);

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

    public synchronized void compare () throws TupleDiffException{
        if (alreadyRan) {
            throw new TupleDiffException ("This TupleComparison has already ran. Create a new one to re-execute.", this);
        }
        try {

            // Initialize everything:
            initialize();
            monitor.reportEvent(this, Monitor.EVENT_START);

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
                tde = new TupleDiffException("Unexpected exception during TupleDiff comparison: "
                        + ex.getMessage(), this, ex);
            }

            // Report the event:
            if (monitor != null) {
                monitor.reportEvent(tde.getSource(), Monitor.EVENT_STOP_ABNORMAL, tde);
            }


            // Re-throw the exception if this was an un-expected exception. i.e. the comparison was not cancelled,
            // but was instead abnormally/unexpected cancelled.
            if (notCancelled) {
                throw tde;
            }

        }
        finally {
            alreadyRan= true;
//            result = new ComparisonResult(config);
            cleanup();
        }

    }

    public ComparisonResult getResult () {
        return result;
    }

    private void cleanup () {
        leftStream.close();
        rightStream.close();
        this.compareEventListener.close();
    }

    private void compareTuples() {
        Tuple left, right;
        int i = 0;
        while (notCancelled) {
            i++;
//            logger.info("Comparing tuples, loop# " + i);

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
            else {
                break;
            }
        }
        logger.info("One Stream has completed. ");
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

    protected void comparisonEvent (CompareEvent.TYPE event, Tuple left, Tuple right, List<String> breakFields) {
        monitor.reportEvent(this, COMPARE_EVENT, event, left, right, breakFields);
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

    public boolean isCancelled () {
        return !notCancelled;
    }

    public boolean isDone () {
        return alreadyRan;
    }

    private List<String> getBreakFields(Tuple left, Tuple right) {
        List <Field> compareFields = left.getSchema().getCompareFields();
        List<String> breakFields = null;
        for (Field f : compareFields) {
            int answer = fieldComparators.get(f).getComparator().compare(left.getValue(f.getName()), right.getValue(f.getName()));
            if (answer != 0) {
                if (breakFields == null) {
                    breakFields = new LinkedList<String>();
                }
                breakFields.add(f.getName());
            }
        }

        return breakFields;
    }

    protected void validateSchemas(TupleStream left, TupleStream right) throws TupleDiffException {
        List<String> errors = new LinkedList<String>();

        compareFields(
                left.getSchema().getCompareFields(), right.getSchema().getCompareFields(),
                "Stream Fields", errors);
        compareFields(
                left.getSchema().getTupleStreamKey().getFields(),
                right.getSchema().getTupleStreamKey().getFields(),
                "Primary Key Fields", errors);

        if (errors.size()!=0) {
            throw new TupleDiffException("Schema Validation Failed. \n" + errors.toString(),  this );
        }
    }

    private void compareFields (List<? extends Field> leftFields, List<? extends Field> rightFields,
                                String fieldType, List <String> errors) {
        if (leftFields.size() != rightFields.size()) {
            errors.add( "Mismatched # of " + fieldType + " in LEFT and RIGHT Stream schemas. Left = <"+
                   leftFields.size() + "> Right = <" + rightFields.size() + ">. \n");
        }
        List<Field> missingLeft = new ArrayList<Field> (leftFields);
        missingLeft.removeAll(rightFields);
        List<Field> missingRight = new ArrayList<Field>(rightFields);
        missingRight.removeAll(leftFields);
        if (missingLeft.size() > 0) {
            errors.add(" LEFT " + fieldType + " not found in RIGHT Stream: " + missingLeft + ". \n");
        }
        if (missingRight.size() > 0) {
            errors.add( " RIGHT " + fieldType + " not found in LEFT Stream: " + missingRight + ". \n");
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
        Connection lConn = null;
        Connection rConn = null;
        String sql = "select * from DIM_EXCHANGE";
        String [] primaryKey = new String [] {"HCM_EXCHANGE_ID"};
        Config config = new Config();
        config.setLeftStream(SqlTupleStream.create(lConn, sql, primaryKey));
        config.setRightStream(SqlTupleStream.create(rConn, sql, primaryKey));

        TupleComparison comparison = new TupleComparison(config);
        comparison.compare();
        ComparisonResult result = comparison.getResult();
        Collection<CompareEvent> breaks = result.getListener().getCompareEvents();

//        TODO: Make flow-y API work.
//        Config.configure()
//                .usingSql("select * from DIM_FUND")
//                .withLeftConnection(conn)
//                .withRightConnection(conn)
//                .withKey(new String[] {"ABC", "DEF"});
//        )  ;
        System.out.println ("DONE!");
    }
}
