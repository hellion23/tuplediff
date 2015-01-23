package com.hellion23.tuplediff.api;

import com.hellion23.tuplediff.api.listener.CompareEventListener;
import com.hellion23.tuplediff.api.listener.ListCompareEventListener;
import com.hellion23.tuplediff.api.monitor.CompareStats;
import com.hellion23.tuplediff.api.monitor.Monitor;
import com.hellion23.tuplediff.api.monitor.Nameable;
import com.hellion23.tuplediff.api.monitor.Stats;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * The primary purpose of ComparisonResult is for the TupleComparison to invoke compare() asynchronously.
 * The results of the comparison are drained into the CompareEventListener, which itself wraps a Collection
 * of breaks (CompareEvent). By default the CompareEventListener will drain to a LinkedList.
 * Other useful implementations of CompareEventListener include those backed by a BlockingQueue so that the breaks
 * can be quickly drained for asynchronous comparisons.
 *
 * @author: Hermann Leung
 * Date: 9/29/2014
 */
public class ComparisonResult {
    TupleComparison tupleComparison;
    CompareEventListener listener;
    private String name;
    Monitor monitor;

    public ComparisonResult (TupleComparison tupleComparison) {
        this.tupleComparison = tupleComparison;
        this.listener = tupleComparison.compareEventListener;
        this.monitor = tupleComparison.monitor;
        this.name = "ComparisonResult for <" + tupleComparison.getName() +">";
    }

    /**
     * Results from the TupleComparison.compare() method are drained into the listener.
     * @param compareEvent
     */
    protected void handleCompareEvent(CompareEvent compareEvent) {
        listener.handleCompareEvent(compareEvent);
    }

    public CompareEventListener getListener() {
        return listener;
    }

    public CompareStats getCompareStats() {
        return monitor.getCompareStats();
    }

    public Stats getLeftTupleStreamStats () {
        return monitor.getLeftStats();
    }

    public Stats getRightTupleStreamStats () {
        return monitor.getRightStats();
    }

    /**
     *  Invokes the TupleComparison's cancel() method.
     */
    public void cancel () {
        tupleComparison.cancel();
    }

    public boolean isCancelled () {
        return tupleComparison.isCancelled();
    }

    public boolean isDone() {
        return tupleComparison.isDone();
    }

    public String toString () {
        StringBuilder sb = new StringBuilder(name);
        sb.append(" Comparison Result: \n");
        if (monitor != null) {
            sb.append(monitor.getLeftStats().toString()).append('\n');
            sb.append(monitor.getRightStats().toString()).append('\n');
            sb.append(monitor.getCompareStats().toString()).append('\n');
        }
        else {
            sb.append("Uninitialized...\n");
        }
        return sb.toString();
    }

}
