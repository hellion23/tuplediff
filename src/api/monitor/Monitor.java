package com.hellion23.tuplediff.api.monitor;

/**
 * Created by Hermann on 9/28/2014.
 */
public interface Monitor {
    // Generic events:
    public static final String STARTED = "STARTED";
    public static final String STOPPED = "STOPPED";
    public static final String FAILED = "FAILED";
    public static final String FAILED_NON_FATAL = "FAILED_NON_FATAL";

    // Specific operator events:

    public void registerMonitorable (Monitorable monitorable);
    public void handleEvent (Nameable source, String event, Exception exception, Object ...params);
}
