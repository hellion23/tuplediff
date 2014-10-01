package com.hellion23.tuplediff.api.monitor;

/**
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
    public void stop ();
    public boolean isStopped();
}
