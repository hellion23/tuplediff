package com.hellion23.tuplediff.api.monitor;

/**
 * Created by Hermann on 9/28/2014.
 */
public interface Monitorable {
    public void setMonitor(Monitor monitor);
    public void stop ();
    public boolean isStopped();
}
