package com.hellion23.tuplediff.api.monitor;

import com.hellion23.tuplediff.api.CompareEvent;
import com.hellion23.tuplediff.api.TupleComparison;
import com.hellion23.tuplediff.api.TupleDiffException;
import com.hellion23.tuplediff.api.TupleStream;

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

    public TupleComparisonMonitor (TupleComparison tc, TupleStream leftStream, TupleStream rightStream) {
        this.tc = tc;
        allStats.put(tc, new TCSTats());
        allStats.put(leftStream, new Stats());
        allStats.put(rightStream, new Stats());
    }


    @Override
    public void reportEvent(Nameable source, String eventName, Object... params) throws TupleDiffException {
        allStats.get(source).event(eventName, params);
    }


    @Override
    public Map <Nameable, Stats> getStats() {
        return allStats;
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

                    validateHealth();

                    break;
                default:
                    super.event(event, params);
                    break;
            }
        }

        public void validateHealth () {
//            TODO check whether there are too many breaks, etc...
        }
    }
}
