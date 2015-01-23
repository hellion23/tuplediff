package com.hellion23.tuplediff.api.monitor;

import com.hellion23.tuplediff.api.Field;

import java.util.Date;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Created by margaret on 9/30/2014.
 */
public class Stats implements Nameable {
    private static final Logger logger = Logger.getLogger(Stats.class.getName());
    protected long startTime = 0;
    protected long endTime = 0;
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
                logger.info(source.getName() + " EVENT START ");
                startTime = System.currentTimeMillis();
                break;
            case Monitor.EVENT_STOP_NORMAL:
                logger.info(source.getName() + " EVENT END ");
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
        return this.name + " Total Runtime: " + (endTime - startTime) + " ms. \n" ;
    }
}
