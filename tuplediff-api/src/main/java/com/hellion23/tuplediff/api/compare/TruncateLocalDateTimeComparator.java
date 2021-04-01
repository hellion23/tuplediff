package com.hellion23.tuplediff.api.compare;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import static java.time.temporal.ChronoUnit.*;

/**
 * Truncate local date time comparator. Defaults to truncated to day (i.e. turns LocalDateTime into a date comparator.
 * Created by hleung on 7/28/2017.
 */
public class TruncateLocalDateTimeComparator implements TypedComparator<LocalDateTime> {
    ChronoUnit truncateTo;

    public TruncateLocalDateTimeComparator () {
        this (null);
    }

    public TruncateLocalDateTimeComparator (ChronoUnit truncateTo) {
        setTruncateTo(truncateTo);
    }

    @Override
    public int compare(LocalDateTime dt1, LocalDateTime dt2) {
        LocalDateTime dateTime1 = dt1.truncatedTo(truncateTo);
        LocalDateTime dateTime2 = dt2.truncatedTo(truncateTo);
        return dateTime1.compareTo(dateTime2);
    }

    public ChronoUnit getTruncateTo() {
        return truncateTo;
    }

    public void setTruncateTo(ChronoUnit truncateTo) {
        this.truncateTo = truncateTo == null ? DAYS : truncateTo;
    }

    @Override
    public String toString() {
        return "TruncateLocalDateTime{" + truncateTo +'}';
    }
}
