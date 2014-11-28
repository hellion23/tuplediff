package com.hellion23.tuplediff.api.monitor;

import java.util.Date;
import java.util.Map;

/**
 * Created by margaret on 9/30/2014.
 */
public class Stats implements Nameable {
    protected long startTime;
    protected long endTime;
    Exception exception;
    Nameable source;
    String name;

    public Stats (Nameable source) {
        this.source = source;
        this.name = "Stats For - ["+source.getName()+"]";
    }

    public Nameable getSource () { return source;}

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public Exception getException() {
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }

    public void event (String event, Object ...params) {
        switch (event) {
            case Monitor.EVENT_START:
                startTime = System.currentTimeMillis();
                break;
            case Monitor.EVENT_STOP_NORMAL:
                endTime = System.currentTimeMillis();
                break;
            case Monitor.EVENT_STOP_ABNORMAL:
            case Monitor.EVENT_STOP_CANCELLED:
            case Monitor.EVENT_STOP_BY_MONITOR:
                endTime = System.currentTimeMillis();
                if (params[0] instanceof Exception) {
                    this.exception = (Exception)params[0];
                }
                break;
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

    public String toString () {
        return this.name + "\n Total Runtime: " + (endTime - startTime) + " ms. " ;
    }
}
