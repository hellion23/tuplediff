package com.hellion23.tuplediff.api.compare;

import java.util.Date;

/**
 * Millis threshold comparator. Default to 1 millis
 * Created by hleung on 7/27/2017.
 */
public class ThresholdDateComparator implements TypedComparator<Date> {
    public long threshold;

    public ThresholdDateComparator () {
        this(1);
    }

    public ThresholdDateComparator (long threshold) {
        this.threshold= threshold;
    }

    @Override
    public int compare(Date date1, Date date2) {
        long difference = date1.getTime() - date2.getTime();
        if (Math.abs(difference) < this.threshold) {
            return 0;
        }
        else if (difference > 0){
            return 1;
        }
        else if (difference < 0){
            return -1;
        }
        return 0;
    }

    public long getThreshold() {
        return threshold;
    }

    public void setThreshold(long threshold) {
        this.threshold = threshold;
    }

    @Override
    public String toString() {
        return "ThresholdDate{" + threshold +'}';
    }
}
