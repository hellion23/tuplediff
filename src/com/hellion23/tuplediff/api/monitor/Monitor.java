package com.hellion23.tuplediff.api.monitor;

import com.hellion23.tuplediff.api.TupleDiffException;

import java.util.Map;

/**
 * A Monitor may treat events emitted by a Monitorable in any way appropriate, but should be populating
 * a MonitorableStats object for any or all of the Monitorable objects under its supervision.
 *
 * It should also at a minimum handle the TERMINATED event, and especially if the STOP_REASON is FAILED or CANCEL,
 * which are abnormal termination states.
 *
 * Created by Hermann on 9/28/2014.
 */
public interface Monitor {
    public static final String EVENT_START = "EVENT_START";
    public static final String EVENT_STOP_NORMAL = "EVENT_STOP_NORMAL";
    public static final String EVENT_STOP_ABNORMAL = "EVENT_STOP_ABNORMAL";

    // Don't know if these should be/can be used...
    public static final String EVENT_STOP_CANCELLED = "EVENT_STOP_CANCELLED";
    public static final String EVENT_STOP_BY_MONITOR = "EVENT_STOP_BY_MONITOR";

    public void reportEvent(Nameable source, String eventName, Object... params) throws TupleDiffException;
    public Map<Nameable, Stats> getStats();
}
