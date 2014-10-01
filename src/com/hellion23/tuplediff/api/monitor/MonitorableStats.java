package com.hellion23.tuplediff.api.monitor;

import java.util.Map;

/**
 * Created by margaret on 9/30/2014.
 */
public abstract class MonitorableStats {
    protected long startTime;
    protected long endTime;
    protected Monitorable.STATE currentState = Monitorable.STATE.NEW;
    protected Monitorable.STOP_REASON stopReason = null;
    Exception exception;

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

    public Monitorable.STATE getCurrentState() {
        return currentState;
    }

    public void setCurrentState(Monitorable.STATE currentState) {
        this.currentState = currentState;
    }

    public Exception getException() {
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }

    public Monitorable.STOP_REASON getStopReason() {
        return stopReason;
    }

    public void setStopReason(Monitorable.STOP_REASON stopReason) {
        this.stopReason = stopReason;
    }

    public abstract void event (Object ... params);
}
