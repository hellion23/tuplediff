package com.hellion23.tuplediff.api.monitor;

import com.hellion23.tuplediff.api.ComparisonResult;

/**
 * Created by Hermann on 9/28/2014.
 */
public interface Monitor {
    public static enum STOP_TYPE {
        FATAL, CANCEL, NORMAL, MONITOR
    }

    // Generic events:
    public static final String STARTED = "STARTED";
    public static final String ENDED = "ENDED";
    public static final String FAILED = "FAILED";
//    public static final String FAILED_NON_FATAL = "FAILED_NON_FATAL";
    public static final String CANCEL = "CANCEL";

    // Specific operator events:
    public static final String TUPLE_LEFT_SEEN = "TUPLE_LEFT_SEEN";
    public static final String TUPLE_RIGHT_SEEN = "TUPLE_RIGHT_SEEN";
    public static final String TUPLE_LEFT_BREAK = "TUPLE_LEFT_BREAK";
    public static final String TUPLE_RIGHT_BREAK = "TUPLE_RIGHT_BREAK";
    public static final String TUPLE_PAIR_BREAK = "TUPLE_PAIR_BREAK";
    public static final String TUPLE_PAIR_MATCHED = "TUPLE_PAIR_MATCHED";

    public void registerMonitorable (Monitorable monitorable);
    public void handleEvent (Nameable source, String event, Object ...params);
    public void reportResults (ComparisonResult result);
}
