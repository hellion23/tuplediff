package com.hellion23.tuplediff.api.compare;

import java.time.Duration;
import java.time.LocalDateTime;

import static java.time.temporal.ChronoUnit.MILLIS;

/**
 * Threshold for local date time. Threshold is expressed in Millis.
 *
 * Created by hleung on 7/27/2017.
 */
public class ThresholdLocalDateTimeComparator  implements TypedComparator<LocalDateTime> {
    public Duration threshold;

    public ThresholdLocalDateTimeComparator () {this (null);}

    public ThresholdLocalDateTimeComparator (long threshold) {
        this (Duration.of(threshold, MILLIS));
    }

    public ThresholdLocalDateTimeComparator (Duration duration) {
        setDuration(duration);
    }

    @Override
    public int compare(LocalDateTime dt1, LocalDateTime dt2) {
        if (threshold.compareTo(Duration.between(dt1, dt2).abs()) >= 0) {
            return 0;
        }
        else {
            return dt1.compareTo(dt2);
        }
    }

    public long getThreshold() {
        return threshold.toMillis();
    }

    public void setThreshold (long millisThreshold) {
        setDuration(Duration.of(millisThreshold, MILLIS));
    }

    public void setDuration(Duration threshold) {
        this.threshold = threshold;
        if (this.threshold == null) {
            this.threshold = Duration.of(1, MILLIS);
        }
    }

    public Duration getDuration () {
        return this.threshold;
    }

    @Override
    public String toString() {
        return "ThresholdLocalDateTime{" + threshold +'}';
    }
}
