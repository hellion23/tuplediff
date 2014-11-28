package com.hellion23.tuplediff.api.monitor;

import com.hellion23.tuplediff.api.*;

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
    protected Map<Nameable, Stats> allStats = new HashMap<Nameable, Stats>();

    public TupleComparisonMonitor (TupleComparison tc, Config config) {
        this.tc = tc;
        allStats.put(tc, new TCSTats(tc));
        allStats.put(config.getLeftStream(), new Stats(config.getLeftStream()));
        allStats.put(config.getRightStream(), new Stats(config.getRightStream()));
    }


    @Override
    public void reportEvent(Nameable source, String eventName, Object... params) throws TupleDiffException {
        allStats.get(source).event(eventName, params);
        validateHealth();
    }


    @Override
    public Map <Nameable, Stats> getStats() {
        return allStats;
    }


    /**
     * Sub-Classes can be overriden to re-implement health checks. By default, this method is called every time an event
     * is reported (as this assumes the reporting is frequent).
     * Alternatively the monitor class can be run in it's own separate thread and periodically poll this method
     * to verify the comparison is going well.
     *
     * @throws TupleDiffException
     */
    protected void validateHealth ()throws TupleDiffException {
//            TODO check whether there are too many breaks, etc...
    }


    protected void stopComparison() {
        this.tc.cancel();
    }

    /**
     * Created by margaret on 9/30/2014.
     */
    public static class TCSTats extends Stats {

        protected int totalOnlyLeft;
        protected int totalOnlyRight;
        protected int totalLeft;
        protected int totalRight;
        protected int totalBreaks;
        protected int totalMatched;

        public TCSTats (Nameable source) {
            super(source);
        }

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

        public void event(String event, Object ... params) {
            switch (event) {
                case "COMPARE_EVENT":
                    switch ((CompareEvent.TYPE)params[0]) {
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
                    break;
                default:
                    super.event(event, params);
                    break;
            }
        }

    }
}
