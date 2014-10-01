package com.hellion23.tuplediff.api.monitor;

import com.hellion23.tuplediff.api.CompareEvent;
import com.hellion23.tuplediff.api.TupleComparison;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * @author: Hermann Leung
 * Date: 9/29/2014
 */
public class TupleComparisonMonitor implements Monitor {
    private static final Logger logger = Logger.getLogger(TupleComparisonMonitor.class.getName());

    TupleComparison tc;
    protected Stats tcStats = new Stats();
    protected Map<Monitorable, MonitorableStats> monitoredStats = new HashMap<Monitorable, MonitorableStats>();

    @Override
    public void registerMonitorable(Monitorable monitorable) {
        MonitorableStats s = null;
        if (monitorable instanceof TupleComparison) {
            tc = (TupleComparison) monitorable;
            s = tcStats;
        }
        monitoredStats.put(monitorable, s);
    }

    @Override
    public void handleEvent(Nameable source, Monitorable.STATE state, Object... params) {
        switch (state) {
            case RUNNING:
                if (source == tc) {
                    tcStats.setCurrentState(state);
                    tcStats.event(params);
                }
            case STARTING:
                if (source == tc) {
                    tcStats.setCurrentState(state);
                    tcStats.setStartTime(System.currentTimeMillis());
                }
                break;
            case TERMINATED:
                if (source == tc) {
                    tcStats.setCurrentState(state);
                    tcStats.setEndTime(System.currentTimeMillis());
                }

                final Monitorable.STOP_REASON stopReason = params[0] instanceof Monitorable.STOP_REASON ?
                        (Monitorable.STOP_REASON) params[0] : Monitorable.STOP_REASON.COMPLETED;

                if (tcStats.getStopReason() == null) {
                    tcStats.setStopReason(stopReason);
                }

                // Special handling if stopped abnormally
                if (stopReason == Monitorable.STOP_REASON.FAILED || stopReason == Monitorable.STOP_REASON.CANCEL) {
                    if (params != null && params.length >= 2 && params[1] instanceof Exception) {
                        tcStats.setException ((Exception)params[1]);
                    }
                    stopComparison();
                }
                break;
        }
    }

    @Override
    public Map<Monitorable, MonitorableStats> getAllStats() {
        return monitoredStats;
    }

    private void stopComparison() {
        for (Monitorable monitorable : monitoredStats.keySet()) {
            try {
                if (!monitorable.isStopped()) {
                    monitorable.stop();
                }
            }
            catch (Exception ex) {
                logger.severe ("Saw exception closing monitorable: " + monitorable + " error: " + ex.getMessage());
            }
        }
    }

    /**
     * Created by margaret on 9/30/2014.
     */
    public static class Stats extends MonitorableStats {

        protected int totalOnlyLeft;
        protected int totalOnlyRight;
        protected int totalLeft;
        protected int totalRight;
        protected int totalBreaks;
        protected int totalMatched;

        public int getTotalOnlyLeft() {
            return totalOnlyLeft;
        }

        public int getTotalOnlyRight() {
            return totalOnlyRight;
        }

        public int getTotalLeft() {
            return totalLeft;
        }

        public int getTotalRight() {
            return totalRight;
        }

        public int getTotalBreaks() {
            return totalBreaks;
        }


        public int getTotalMatched() {
            return totalMatched;
        }


        @Override
        public void event(Object... params) {
            if (params[0] instanceof CompareEvent.TYPE) {
                switch ((CompareEvent.TYPE ) params[0]) {
                    case DATA_LEFT:
                        totalLeft++;
                        break;
                    case DATA_RIGHT:
                        totalRight++;
                        break;
                    case LEFT_BREAK:
                        totalOnlyLeft++;
                        break;
                    case RIGHT_BREAK:
                        totalOnlyRight++;
                        break;
                    case PAIR_BREAK:
                        totalBreaks++;
                        break;
                    case PAIR_MATCHED:
                        totalMatched++;
                        break;
                }
            }
        }
    }
}
