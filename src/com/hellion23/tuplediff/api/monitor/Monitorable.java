package com.hellion23.tuplediff.api.monitor;

/**
 * A Monitorable reports it's status to a Monitor (via the Monitor's handleEvent method) and allows itself to be
 * stopped by the monitor.
 *
 * At a minimum the Monitorable should report it's STATE transition changes, and if reporting a TERMINATED state, should
 * also include a STOP_REASON. If the STOP_REASON is FAILED, the Exception should also be included. If a STOP_REASON
 * is not provided, it is assumed the TERMINATED reason is COMPLETED.
 *
 * It is dependent on the implementation of Monitor how events should be treated.
 *
 * Created by Hermann on 9/28/2014.
 */
public interface Monitorable {
    public static enum STATE {
        NEW, STARTING, RUNNING, STOPPING, TERMINATED
    }

    public static enum STOP_REASON {
        CANCEL, COMPLETED, FAILED, MONITOR
    }

    public void setMonitor(Monitor monitor);

    /**
     * Invocationns to stop should be idemmpotent and not throw any exceptions. 
     */
    public void stop ();
    public boolean isStopped();
}
