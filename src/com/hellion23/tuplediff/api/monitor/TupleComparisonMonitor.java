package com.hellion23.tuplediff.api.monitor;

import com.hellion23.tuplediff.api.*;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * @author: Hermann Leung
 * Date: 9/29/2014
 */
public class TupleComparisonMonitor implements Monitor {
    private static final Logger logger = Logger.getLogger(TupleComparisonMonitor.class.getName());
    TupleStream leftStream, rightStream;
    TupleComparison tc;
    protected Map<Nameable, Stats> allStats = new HashMap<Nameable, Stats>();

    public void setTupleComparison (TupleComparison tc) {
        this.tc = tc;
    }

    @Override
    public void setLeftStream(TupleStream leftStream) {
        this.leftStream = leftStream;
    }

    @Override
    public void setRightStream(TupleStream rightStream) {
        this.rightStream = rightStream;
    }

    @Override
    public void init() {
        allStats.put(leftStream, new Stats(leftStream));
        allStats.put(rightStream, new Stats(rightStream));
        allStats.put(tc, new CompareStats(tc));
    }

    @Override
    public void reportEvent(Nameable source, String eventName, Object... params) throws TupleDiffException {
        allStats.get(source).event(eventName, params);
        validateHealth();
    }


    @Override
    public Map <Nameable, Stats> getAllStats() {
        return allStats;
    }

    @Override
    public CompareStats getCompareStats() {
        return (CompareStats) allStats.get(tc);
    }

    @Override
    public Stats getLeftStats() {
        return allStats.get(leftStream);
    }

    @Override
    public Stats getRightStats() {
        return allStats.get(rightStream);
    }


    /**
     * Sub-Classes can be overriden to re-implement health checks. By default, this method is called every time an event
     * is reported (as this assumes the reporting is frequent).
     * Alternatively the monitor class can be run in it's own separate thread and periodically poll this method
     * to verify the comparison is going well.
     *
     * @throws TupleDiffException
     */
    protected void validateHealth ()throws TupleDiffException {
//            TODO check whether there are too many breaks, etc...
    }


    protected void stopComparison() {
        this.tc.cancel();
    }

}
