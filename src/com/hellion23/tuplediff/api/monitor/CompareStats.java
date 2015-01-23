package com.hellion23.tuplediff.api.monitor;

import com.hellion23.tuplediff.api.CompareEvent;
import com.hellion23.tuplediff.api.TupleComparison;

import java.util.logging.Logger;

/**
 * Created by margaret on 9/30/2014.
 */
public class CompareStats extends Stats {
    private static final Logger logger = Logger.getLogger(CompareStats.class.getName());
    protected int totalOnlyLeft;
    protected int totalOnlyRight;
    protected int totalLeft;
    protected int totalRight;
    protected int totalBreaks;
    protected int totalMatched;

    public CompareStats(Nameable source) {
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
            case TupleComparison.COMPARE_EVENT:
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
                logger.info("Unknown event, passing to super: " + event);
                super.event(event, params);
                break;
        }
    }

    public String toString () {
        return super.toString()+
            " totalLeft: " + totalLeft + ", totalRight: " + totalRight + "\n" +
            " totalOnlyLeft: " + totalOnlyLeft + ", totalOnlyRight: " + totalOnlyRight + "\n" +
            " totalBreaks: " + totalBreaks + ", totalMatched: " + totalMatched + "\n"
        ;
    }
}
