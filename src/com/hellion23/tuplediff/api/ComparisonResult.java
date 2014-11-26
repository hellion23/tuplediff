package com.hellion23.tuplediff.api;

import com.hellion23.tuplediff.api.listener.CompareEventListener;
import com.hellion23.tuplediff.api.monitor.Monitorable;
import com.hellion23.tuplediff.api.monitor.Stats;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;

/**
 * @author: Hermann Leung
 * Date: 9/29/2014
 */
public class ComparisonResult {
    Collection<CompareEvent> compareEvents = new LinkedList<CompareEvent>();
    CompareEventListener listener;
    private Map<Monitorable, Stats> stats;

    public Collection<CompareEvent> getCompareEvents() {
        return compareEvents;
    }

    public void setCompareEvents(Collection<CompareEvent> compareEvents) {
        this.compareEvents = compareEvents;
    }

    public Map<Monitorable, Stats> getStats() {
        return stats;
    }

    public void setStats(Map<Monitorable, Stats> stats) {
        this.stats = stats;
    }

    public ComparisonResult (CompareEventListener listener) {
        this.listener = listener;
    }

    public void addComparisonEvent (CompareEvent tupleBreak) {
        if (compareEvents != null) {
            compareEvents.add(tupleBreak);
        }
        if (listener != null) {
            listener.handleTupleBreak(tupleBreak);
        }
    }

    public Collection<CompareEvent> getComparisonEvent() {
        return compareEvents;
    }

    public void setComparisonEvent(Collection<CompareEvent> compareEvent) {
        this.compareEvents = compareEvent;
    }

    public CompareEventListener getListener() {
        return listener;
    }

    public void setListener(CompareEventListener listener) {
        this.listener = listener;
    }

}
