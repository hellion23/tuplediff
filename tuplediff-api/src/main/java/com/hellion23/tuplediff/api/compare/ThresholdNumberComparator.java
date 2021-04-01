package com.hellion23.tuplediff.api.compare;

/**
 * Defaults to a threshold of decimal = .00001.
 *
* Created by hleung on 5/30/2017.
*/
public class ThresholdNumberComparator implements TypedComparator<Number> {
    double threshold;

    public ThresholdNumberComparator () {
        this(.00001d);
    }

    public ThresholdNumberComparator(double threshold) {
        this.threshold = threshold;
    }

    public int compare(Number num1, Number num2) {
        double difference = num1.doubleValue() - num2.doubleValue();

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

    public double getThreshold() {
        return threshold;
    }

    public void setThreshold(double threshold) {
        this.threshold = threshold;
    }

    @Override
    public String toString() {
        return "ThresholdNumber{" + threshold +'}';
    }
}
