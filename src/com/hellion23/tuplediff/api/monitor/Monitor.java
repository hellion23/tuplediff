package com.hellion23.tuplediff.api.monitor;

import java.util.Map;

/**
 * Created by Hermann on 9/28/2014.
 */
public interface Monitor {

    // Stop type events:
//    public static final String FAILED = "FAILED";
//    public static final String COMPLETED = "COMPLETED";
//    public static final String CANCEL = "CANCEL";
//    public static final String MONITOR = "MONITOR";

    public void registerMonitorable (Monitorable monitorable);
    public void handleEvent (Nameable source, Monitorable.STATE state, Object ...params);
    public Map<Monitorable, MonitorableStats>  getAllStats();
}
