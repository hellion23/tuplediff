package com.hellion23.tuplediff.api;

import com.hellion23.tuplediff.api.listener.CompareEventListener;
import com.hellion23.tuplediff.api.monitor.Nameable;
import com.hellion23.tuplediff.api.monitor.Stats;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;

/**
 * ComparisonResult wraps several components of the result of a TupleComparison:
 * 1) DrainTo queue - simple Collection (usually queue) where CompareEvents that result from the TupleComparison can
 *  be added to.
 * 2) CompareEventListener - fancier Interface that has an initialization/close methods that handle the situation
 *  where the schema is important to how CompareEvents are processed.
 * 3) Stats - statistics classes that has information on how long the component took to run, the exception
 *  if the component stopped abnormally, etc...
 *
 * @author: Hermann Leung
 * Date: 9/29/2014
 */
public class ComparisonResult {
    Collection<CompareEvent> drainTo;
    CompareEventListener listener;
    private Map<Nameable, Stats> stats;

    public Map<Nameable, Stats> getStats() {
        return stats;
    }

    public void setStats(Map<Nameable, Stats> stats) {
        this.stats = stats;
    }

    public ComparisonResult (Config config) {
        this.listener = config.getCompareEventListener();
        this.drainTo = config.getDrainTo();
        if (drainTo == null && listener == null) {
            this.drainTo =  new LinkedList<CompareEvent>();
        }
    }

    /**
     * If a Listener was defined, results go to the listener; if DrainTo was defined will go to the Collection.
     * If neither is defined results will go to a linkedList and results can be fetched by
     * @param compareEvent
     */
    public void handleCompareEvent(CompareEvent compareEvent) {
        if (drainTo != null) {
            drainTo.add(compareEvent);
        }
        if (listener != null) {
            listener.handleCompareEvent(compareEvent);
        }
    }

    public Collection<CompareEvent> getDrainTo() {
        return drainTo;
    }

    public void setDrainTo(Collection<CompareEvent> drainTo) {
        this.drainTo = drainTo;
    }

    public CompareEventListener getListener() {
        return listener;
    }

    public void setListener(CompareEventListener listener) {
        this.listener = listener;
    }

}
