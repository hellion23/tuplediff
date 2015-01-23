package com.hellion23.tuplediff.api.comparator;

import java.util.Comparator;

/**
 * @author: Hermann Leung
 * Date: 12/12/2014
 */
public class ThresholdNumberComparator implements Comparator <Number> {
    public static final ThresholdNumberComparator DEFAULT = new ThresholdNumberComparator(.00001);
    double threshold;

    protected ThresholdNumberComparator(double threshold) {
        this.threshold = threshold;
    }

    @Override
    public int compare(Number num1, Number num2) {
        double difference =
                (num1 == null ? 0 : num1.doubleValue()) -
                (num2 == null ? 0 : num2.doubleValue());
        double absDiff = difference < 0 ? difference * -1 : difference;
        if (absDiff < this.threshold) {
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

    public String toString() {
        return "ThresholdNumberComparator["+threshold+"]";
    }

}
