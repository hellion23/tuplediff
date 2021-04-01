package com.hellion23.tuplediff.api.handler;

import com.hellion23.tuplediff.api.TDCompare;
import com.hellion23.tuplediff.api.compare.CompareEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Created by hleung on 5/30/2017.
 */
public class StatsEventHandler implements CompareEventHandler {
    private final static Logger LOG = LoggerFactory.getLogger(StatsEventHandler.class);
    long numLeft =0,            numRight = 0;
    long numLeftOnly = 0,       numRightOnly = 0;
    long numMatched = 0,        numBreaks = 0;
    long startTime = 0,         endTime = 0;

    boolean printStatsAtEnd;
    TDCompare comparison;
    Map<String, Integer> fieldBreakCounts = new HashMap<>();

    public StatsEventHandler () {
        this(false);
    }

    public StatsEventHandler (boolean printStatsAtEnd) {
        this.printStatsAtEnd = printStatsAtEnd;
    }

    @Override
    public void init(TDCompare comparison) {
        this.comparison = comparison;
        startTime = System.currentTimeMillis();
    }

    public long getNumLeft() {
        return numLeft;
    }

    public long getNumRight() {
        return numRight;
    }

    public long getNumLeftOnly() {
        return numLeftOnly;
    }

    public long getNumRightOnly() {
        return numRightOnly;
    }

    public long getNumMatched() {
        return numMatched;
    }

    public long getNumBreaks() {
        return numBreaks;
    }

    public Duration getCompareTime () {
        if (startTime == 0 && endTime == 0) {
            return Duration.ofMillis(0);
        }
        if (startTime !=0 && endTime != 0) {
            return Duration.ofMillis(endTime - startTime);
        }
        else { // endTime not set.
            return Duration.ofMillis(System.currentTimeMillis() - startTime);
        }
    }

    @Override
    public void close() throws Exception {
        endTime = System.currentTimeMillis();
        if (printStatsAtEnd) {
            LOG.info("Stats for comparison <" +comparison.getName() + ">: \n " + toString() + "\n Field Break Counts: " + fieldBreakCounts);
        }
    }

    @Override
    public void accept(CompareEvent compareEvent) {
        endTime = System.currentTimeMillis();
        switch (compareEvent.getType()) {
            case FOUND_BREAK:
                tallyBreakFields(compareEvent.getFieldBreaks());
                numBreaks++;
                break;
            case FOUND_ONLY_LEFT:
                numLeftOnly ++;
                break;
            case FOUND_ONLY_RIGHT:
                numRightOnly++;
                break;
            case DATA_LEFT:
                numLeft++;
                break;
            case DATA_RIGHT:
                numRight++;
                break;
            case FOUND_MATCHED:
                numMatched++;
                break;
        }
    }

    private void tallyBreakFields(List<String> fieldBreaks) {
        for (String field : fieldBreaks) {
            Integer count = Optional.ofNullable(fieldBreakCounts.get(field)).orElse(0);
            this.fieldBreakCounts.put(field, count+1);
        }
    }

    public Map<String, Integer> getFieldBreakCounts () {
        return new HashMap<>(fieldBreakCounts);
    }

    @Override
    public String toString() {
        return "StatsEventHandler{" +
                "numBreaks=" + numBreaks +
                ", numMatched=" + numMatched +
                ", numLeft=" + numLeft +
                ", numRight=" + numRight +
                ", numLeftOnly=" + numLeftOnly +
                ", numRightOnly=" + numRightOnly +
                ", compareTime=" + getCompareTime().toMillis() + " ms" +
                '}';
    }
}
