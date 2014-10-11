package com.hellion23.tuplediff.api.monitor;

import java.util.Map;

/**
 * A Monitor may treat events emitted by a Monitorable in any way appropriate, but should be populating
 * a MonitorableStats object.
 * It should also at a minimum handle the TERMINATED event, and especially if the STOP_REASON is FAILED or CANCEL,
 * which are abnormal termination states.
 *
 * Created by Hermann on 9/28/2014.
 */
public interface Monitor <T extends MonitorableStats> {
    public void registerMonitorable (Monitorable monitorable);
    public void handleEvent (Nameable source, Monitorable.STATE state, Object ...params);
    public Map<Monitorable, T>  getAllStats();
}
