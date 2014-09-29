package com.hellion23.tuplediff.api.monitor;

import com.hellion23.tuplediff.api.ComparisonResult;
import com.hellion23.tuplediff.api.TupleComparison;

import java.util.HashSet;
import java.util.logging.Logger;

/**
 * @author: Hermann Leung
 * Date: 9/29/2014
 */
public class DefaultMonitor implements Monitor {
    private static final Logger logger = Logger.getLogger(DefaultMonitor.class.getName());
    HashSet<Monitorable> monitored = new HashSet<Monitorable>();
    protected int totalOnlyLeft;
    protected int totalOnlyRight;
    protected int totalLeft;
    protected int totalRight;
    protected int totalBreaks;
    protected int totalMatched;
    protected long startTime;
    protected long endTime;
    protected Exception exception;
    protected STOP_TYPE stopType = STOP_TYPE.NORMAL;

    @Override
    public void registerMonitorable(Monitorable monitorable) {
        monitored.add(monitorable);
    }

    @Override
    public void handleEvent(Nameable source, String event, Object... params) {
        switch (event) {
            case TUPLE_LEFT_SEEN:
                totalLeft++;
                break;
            case TUPLE_RIGHT_SEEN:
                totalRight++;
                break;
            case TUPLE_LEFT_BREAK:
                totalOnlyLeft++;
                break;
            case TUPLE_RIGHT_BREAK:
                totalOnlyRight++;
                break;
            case TUPLE_PAIR_MATCHED:
                totalMatched++;
                break;
            case TUPLE_PAIR_BREAK:
                totalBreaks++;
                break;
            case FAILED:
                if (params != null && params.length > 0 && params[0] instanceof Exception) {
                    exception = (Exception) params[1];
                }
                stopComparison(STOP_TYPE.FATAL);
                break;
            case CANCEL:
                stopComparison(STOP_TYPE.CANCEL);
                break;
            case STARTED:
                if (source instanceof TupleComparison) {
                    startTime = System.currentTimeMillis();
                }
                break;
            case ENDED:
                if (source instanceof TupleComparison) {
                    endTime = System.currentTimeMillis();
                }
                break;
        }
    }

    private void stopComparison(STOP_TYPE stopType) {
        this.stopType = stopType;
        for (Monitorable monitorable : monitored) {
            try {
                if (!monitorable.isStopped()) {
                    monitorable.stop();
                }
            }
            catch (Exception ex) {
                logger.severe ("Saw exception closing monitorable: " + monitorable + " error: " + ex.getMessage());
            }
        }
    }

    @Override
    public void reportResults(ComparisonResult result) {
        result.setException(exception);
        result.setTotalLeft(totalLeft);
        result.setTotalRight(totalRight);
        result.setTotalBreaks(totalBreaks);
        result.setTotalMatched(totalMatched);
        result.setTotalOnlyLeft(totalOnlyLeft);
        result.setTotalOnlyRight(totalOnlyRight);
        result.setStartTime(startTime);
        result.setEndTime(endTime);
        result.setHowStopped(stopType);
    }
}
