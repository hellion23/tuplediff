package com.hellion23.tuplediff.api.compare;

import static java.time.temporal.ChronoUnit.*;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Date;

/**
 * Created by hleung on 7/30/2017.
 */
public class TruncateDateComparator implements TypedComparator<Date> {
    ChronoUnit truncateTo;
    TruncateLocalDateTimeComparator comparator;

    public TruncateDateComparator() {
        this(null);
    }

    public TruncateDateComparator(ChronoUnit truncateTo) {
        setTruncateTo(truncateTo);
    }

    @Override
    public int compare(Date o1, Date o2) {
        return comparator.compare(convert(o1), convert(o2));
    }

    private LocalDateTime convert(Date dt) {
        return LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
    }

    public ChronoUnit getTruncateTo() {
        return truncateTo;
    }

    public void setTruncateTo(ChronoUnit truncateTo) {
        this.truncateTo = truncateTo == null ? DAYS : truncateTo;
        this.comparator = new TruncateLocalDateTimeComparator(this.truncateTo);
    }

    @Override
    public String toString() {
        return "TruncateDate{" + truncateTo +'}';
    }
}
