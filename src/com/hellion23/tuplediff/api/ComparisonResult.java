package com.hellion23.tuplediff.api;

import com.hellion23.tuplediff.api.listener.CompareEventListener;
import com.hellion23.tuplediff.api.monitor.Monitor;

import java.util.Collection;
import java.util.LinkedList;

/**
 * @author: Hermann Leung
 * Date: 9/29/2014
 */
public class ComparisonResult {
    Collection<CompareEvent> compareEvents = new LinkedList<CompareEvent>();
    protected int totalOnlyLeft;
    protected int totalOnlyRight;
    protected int totalLeft;
    protected int totalRight;
    protected int totalBreaks;
    protected int totalMatched;
    protected long startTime;
    protected long endTime;
    protected Monitor.STOP_TYPE howStopped;
    Exception exception;
    CompareEventListener listener;

    public ComparisonResult (CompareEventListener listener) {
        this.listener = listener;
    }

    public void addComparisonEvent (CompareEvent tupleBreak) {
        if (listener == null) {
            compareEvents.add(tupleBreak);
        }
        else {
            listener.handleTupleBreak(tupleBreak);
        }
    }

    public Collection<CompareEvent> getComparisonEvent() {
        return compareEvents;
    }

    public void setComparisonEvent(Collection<CompareEvent> compareEvent) {
        this.compareEvents = compareEvent;
    }

    public int getTotalOnlyLeft() {
        return totalOnlyLeft;
    }

    public void setTotalOnlyLeft(int totalOnlyLeft) {
        this.totalOnlyLeft = totalOnlyLeft;
    }

    public int getTotalOnlyRight() {
        return totalOnlyRight;
    }

    public void setTotalOnlyRight(int totalOnlyRight) {
        this.totalOnlyRight = totalOnlyRight;
    }

    public int getTotalLeft() {
        return totalLeft;
    }

    public void setTotalLeft(int totalLeft) {
        this.totalLeft = totalLeft;
    }

    public int getTotalRight() {
        return totalRight;
    }

    public void setTotalRight(int totalRight) {
        this.totalRight = totalRight;
    }

    public int getTotalBreaks() {
        return totalBreaks;
    }

    public void setTotalBreaks(int totalBreaks) {
        this.totalBreaks = totalBreaks;
    }

    public int getTotalMatched() {
        return totalMatched;
    }

    public void setTotalMatched(int totalMatched) {
        this.totalMatched = totalMatched;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public Exception getException() {
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }

    public CompareEventListener getListener() {
        return listener;
    }

    public void setListener(CompareEventListener listener) {
        this.listener = listener;
    }

    public Monitor.STOP_TYPE getHowStopped() {
        return howStopped;
    }

    public void setHowStopped(Monitor.STOP_TYPE howStopped) {
        this.howStopped = howStopped;
    }
}
