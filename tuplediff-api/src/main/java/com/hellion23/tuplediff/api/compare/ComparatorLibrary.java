package com.hellion23.tuplediff.api.compare;

import com.hellion23.tuplediff.api.config.ComparatorConfig;
import com.hellion23.tuplediff.api.config.MarshalUtils;
import com.hellion23.tuplediff.api.format.MatchCriteria;
import com.hellion23.tuplediff.api.model.TDColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 *
 * Created by hleung on 5/26/2017.
 */
public class ComparatorLibrary {
    private final static Logger LOG = LoggerFactory.getLogger(ComparatorLibrary.class);

    public static TypedComparator <Object>  DEFAULT_COMPARABLE_COMPARATOR =
//            new ComparableComparator();
            new ObjectComparator();

    public static TypedComparator <Object> ALWAYS_EQUAL_COMPARATOR = new AlwaysEqualComparator();

    public static Comparator NULL_SAFE_COMPARATOR = Comparator.nullsFirst(Comparator.naturalOrder());

    public static FieldComparator dontCompareComparator (String colName) {
        return new FieldComparator(ALWAYS_EQUAL_COMPARATOR, colName );
    }

    public static FieldComparator resolveComparator(TDColumn col, List<FieldComparator> overrides) {
        FieldComparator fc = null;
        if (overrides != null) {
            fc = overrides.stream().filter(co -> co.getMatchCriteria().apply(col)).findFirst().orElse(null);
        }
        if (fc == null) {
            fc =  new FieldComparator(DEFAULT_COMPARABLE_COMPARATOR, col.getName());
        }
        return fc;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        String [] fieldNames;
        Class [] fieldClasses;
        TypedComparator comparator;

        public Builder names(String... names) {
            this.fieldNames = names;
            return this;
        }

        public Builder classes(Class... classes) {
            this.fieldClasses = classes;
            return this;
        }

        public Builder comparator (TypedComparator comparator) {
            this.comparator = comparator;
            return this;
        }

        public Builder sameDate () {
            return truncateDateTo (ChronoUnit.DAYS);
        }

        public Builder sameLocalDateTime () {
            return truncateLocalDateTimeTo(ChronoUnit.DAYS);
        }

        public Builder truncateDateTo (ChronoUnit truncateTo) {
            comparator = new TruncateDateComparator (truncateTo);
            return this;
        }

        public Builder truncateLocalDateTimeTo (ChronoUnit truncateTo) {
            comparator = new TruncateLocalDateTimeComparator (truncateTo);
            return this;
        }

        public Builder thresholdNumber () {
            comparator = new ThresholdNumberComparator();
            return this;
        }

        public Builder thresholdNumber (Double threshold) {
            comparator = new ThresholdNumberComparator(threshold);
            return this;
        }

        public Builder thresholdDate (long millis) {
            comparator = new ThresholdDateComparator(millis);
            return this;
        }

        public Builder thresholdLocalDateTime (long millis) {
            comparator = new ThresholdLocalDateTimeComparator(millis);
            return this;
        }

        public FieldComparator buildComparator() {
            return new FieldComparator(comparator, new MatchCriteria(fieldNames, fieldClasses));
        }

        public ComparatorConfig buildConfig () {
            return MarshalUtils.toConfig(buildComparator());
        }
    }


}
