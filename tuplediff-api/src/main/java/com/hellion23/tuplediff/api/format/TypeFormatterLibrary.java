package com.hellion23.tuplediff.api.format;

import com.hellion23.tuplediff.api.compare.FieldComparator;
import com.hellion23.tuplediff.api.config.FormatterConfig;
import com.hellion23.tuplediff.api.model.TDColumn;
import com.hellion23.tuplediff.api.model.TDException;
import com.hellion23.tuplediff.api.model.TDSide;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Created by hleung on 5/24/2017.
 */
public class TypeFormatterLibrary {
    private final static Logger LOG = LoggerFactory.getLogger(TypeFormatterLibrary.class);
    static ZoneId DEFAULT_ZONE_ID = ZoneId.systemDefault();

    public static final TypeFormatter<Object, String> TO_STRING_FORMATTER = new TypeFormatter<Object, String>() {
        @Override
        public String apply(Object o) {
            return o == null ? null : o.toString();
        }
    };

    /**
     * Converts any Date class (including java.sql.Time, java.sql.Timestamp) into a LocalDateTime
     */
    //TODO: Assumes all times/dates from any source is going to be local. If this isn't then the TDStreams must return dates/times that are localized. In the future, may support multiple timezones.
    public static final TypeFormatter<? extends Date, LocalDateTime> DATE_TO_LOCALDATETIME_FORMATTER = new TypeFormatter<Date, LocalDateTime>() {
        @Override
        public LocalDateTime apply(Date date) {
            return date == null ? null : LocalDateTime.ofInstant(date.toInstant(), DEFAULT_ZONE_ID);
        }
    };

    /**
     * Convert any Number to BigDecimal
     */
    public static final TypeFormatter<? extends Number, BigDecimal> NUMBER_TO_BIGDECIMAL_FORMATTER = new TypeFormatter<Number, BigDecimal>() {
        @Override
        public BigDecimal apply(Number number) {
            BigDecimal result;
            if (number == null || "".equals(number)) {
                result = null;
            } else if (number instanceof Double) {
                result = BigDecimal.valueOf(number.doubleValue());
            } else if (number instanceof Integer) {
                result = BigDecimal.valueOf(number.intValue());
            } else if (number instanceof Short) {
                result = BigDecimal.valueOf(number.shortValue());
            } else if (number instanceof Long) {
                result = BigDecimal.valueOf(number.longValue());
            } else if (number instanceof Float) {
                result = BigDecimal.valueOf(number.floatValue());
            } else {
                throw new IllegalArgumentException("unsupported Number subtype: " + number.getClass().getName());
            }
            return result;
        }
    };


    /**
     * Convert String to BigDecimal
     */
    public static final TypeFormatter<String, BigDecimal> STRING_TO_BIGDECIMAL_FORMATTER = new TypeFormatter<String, BigDecimal>() {
        @Override
        public BigDecimal apply(String s) {
            return (s == null || "".equals(s)) ? null : new BigDecimal(s);
        }
    };

    /**
     * Convert String to Boolean
     */
    public static final TypeFormatter<String, Boolean> STRING_TO_BOOLEAN_FORMATTER = new TypeFormatter<String, Boolean>() {
        @Override
        public Boolean apply(String s) {
            if (s == null) return Boolean.FALSE;
            final String b = s.toLowerCase().trim();
            switch (b) {
                case "true":
                case "1":
                case "yes":
                case "y":
                    return Boolean.TRUE;
                case "false":
                case "0":
                case "no":
                case "n":
                default:
                    return Boolean.FALSE;
            }
        }
    };

    /**
     * Convert to LocalDateTime from String. If pattern provided will use that, if none provided, will attempt to
     * detect once parsing occurs.
     */
    public static class LocalDateTimeFormatter extends TypeFormatter <String, LocalDateTime> {
        DateTimeFormatter formatter = null;
        String pattern;
        static Map<String, DateTimeFormatter> defaultDateFormatters;
        static Map<String, Consumer<DateTimeFormatterBuilder>> customDTF;
        static String regex;

        static {
            String o = "~";
            String c = "~";
            customDTF = new HashMap<>();
            customDTF.put(o+"FRACTION_OF_MILLI"+c, (fb -> fb.appendFraction(ChronoField.MILLI_OF_SECOND, 0, 3, true)));
            customDTF.put(o+"FRACTION_OF_MICRO"+c, (fb -> fb.appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)));
            customDTF.put(o+"FRACTION_OF_NANO"+c, (fb -> fb.appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)));
            String r = customDTF.keySet().stream().collect(Collectors.joining("|"));
            regex = "((?<=" + r + ")|(?=" + r + "))";

            // Create Default DateTimeFormatters
            defaultDateFormatters = new LinkedHashMap<>();

            // Excel style
            defaultDateFormatters.put("d-MMM-yy", new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .appendPattern("d-MMM-yy")
                .optionalStart()
                .appendPattern(" HH:mm:ss")
                .optionalEnd()
                .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
                .toFormatter(Locale.ENGLISH));

            // Traditional MM/dd/yyyy format with optional HH:mm:sss.SSSSS, used by Oracle and others:
            defaultDateFormatters.put("M/d/yyyy",  new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .append(DateTimeFormatter.ofPattern("M/d/yyyy"))
                .optionalStart()
                .appendPattern(" HH:mm:ss")
                .optionalEnd()
                .optionalStart()
                .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                .optionalEnd()
                .optionalStart()
                .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
                .optionalEnd()
                .optionalStart()
                .appendFraction(ChronoField.MILLI_OF_SECOND, 0, 3, true)
                .optionalEnd()
                .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
                .toFormatter()
            );

            // Traditional MM/dd/yyyy format with optional HH:mm:sss.SSSSS, used by Oracle and others:
            defaultDateFormatters.put("OracleTimestampExport",  new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .append(DateTimeFormatter.ofPattern("d-MMM-yy hh.mm.ss"))
                .optionalStart()
                .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                .optionalEnd()
                .appendPattern(" a")
                .toFormatter()
            );
            defaultDateFormatters.put("[BASIC_ISO_DATE]", DateTimeFormatter.BASIC_ISO_DATE);
            defaultDateFormatters.put("[ISO_DATE]", DateTimeFormatter.ISO_DATE);
            defaultDateFormatters.put("[ISO_LOCAL_DATE_TIME]", DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            defaultDateFormatters.put("[ISO_INSTANT]", DateTimeFormatter.ISO_INSTANT);
            String customJavaFmt = "yyyy-MM-dd HH:mm:ss~FRACTION_OF_MILLI~";
            defaultDateFormatters.put(customJavaFmt, buildFormatter(customJavaFmt));
        }

        public LocalDateTimeFormatter() {
            this (null);
        }

        public LocalDateTimeFormatter (String pattern) {
            this.pattern = pattern;
            if (pattern!=null) {
                this.formatter = buildFormatter(pattern);
            }
        }

        @Override
        public Class getType() {
            return LocalDateTime.class;
        }

        @Override
        public LocalDateTime apply(String s) {
            LocalDateTime date;
            if (s == null || "".equals(s)) {
                date = null;
            }
            else if (formatter == null) {
                date = tryFormatters(s);
            }
            else {
                date = LocalDateTime.parse(s, formatter);
            }
            return date;
        }

        private static DateTimeFormatter buildFormatter (String pattern) {
            DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();
            String str [] = pattern.split(regex);
            for (String s : str) {
                Consumer<DateTimeFormatterBuilder> c = customDTF.get(s);
                if (c == null) {
                    // Append standard pattern
                    builder.appendPattern(s);
                }
                else {
                    // Append customized pattern.
                    c.accept(builder);
                }
            }
            return builder.toFormatter();
        }

        private LocalDateTime tryFormatters(String s) throws TDException {
            for (Map.Entry<String, DateTimeFormatter> dtf : defaultDateFormatters.entrySet()) {
                try {
                    LocalDateTime date = LocalDateTime.parse(s, dtf.getValue());
                    // successful format.
                    formatter = dtf.getValue();
                    pattern = dtf.getKey();
                    LOG.debug(String.format("Successfully chosen pattern '%s', for String '%s'.", pattern, s));
                    return date;
                }
                catch (Exception ex) {
                    LOG.debug(String.format("Tried datePattern <%s> against date String: <%s> but did not work for reason <%s>. Will try more patterns if available",
                            dtf.getKey(), s, ex.getMessage()));
                }
            }
            throw new TDException("Unable to format to LocalDateTime using default formatters. Value: " + s);
        }

        @Override
        public String toString() {
            return "TypeFormatter{"+LocalDateTime.class+"} pattern{"+ (pattern==null ? "UNKNOWN" : pattern) +"}";
        }

        public String getPattern() {
            return pattern;
        }
    }

    /**
     * This method resolves the most appropriate TypeFormatter for the given column. Both left and right columns should
     * resolve into a common Class such that a Comparator can be used to compare them correctly.
     * 1) If left and right columns are of the same type, and no comparator overrides are defined, no TypeFormatter is needed.
     * 2) If left and right columns are of a different type, a common class will be determined for both and
     *  both will be cast into the common class. For example, if the left column type is String and right column is an
     *  Integer, then the left column will be assigned a typeFormatter that transforms a String to a BigDecimal and the
     *  right will be assigned a typeFormatter converting an Integer to a BigDecimal.
     * 3) If left and right columns are of a different type, and no common class can be found, an exception is thrown.
     * 4) If there is a Comparator override provided for the column name (not class comparator override since that
     *  matching is resolved after typeFormatters are determined), then this Comparator override's class is used
     *  as the Class to which the column's type formatter must match to. If a type formatter cannot be found for the
     *  columns that match the Comparator override of that class an exception wil be thrown. For example if left and
     *  right column returns an Integer class and the Comparator compares LocalDateTime, there are no TypeFormatters
     *  available (by default) that converts the Integer to a LocalDateTime and the exception will be thrown.
     * 5) A final check is made to determine that the TypeFormatters resolved for both the left and right column
     *  yields the same formatted class. The Comparator class is also checked against the left & right classes to ensure
     *  that the the formatted classes can be compared by the Comparator
     *
     * @param left
     * @param right
     * @param typeOverrides
     * @param comparatorOverrides
     * @param strongType
     * @return
     */
    public static Map<TDColumn, TypeFormatter> resolveFormatters (TDColumn left, TDColumn right,
                                                                  List<FieldTypeFormatter> typeOverrides, List<FieldComparator> comparatorOverrides,boolean strongType) {

        // Figure out if there is a field name comparator appropriate for these columns; if there is, the Comparator's
        // expected class is what both columns need to be formatted into.
        Class comparatorClass = null;
        for (FieldComparator co : comparatorOverrides == null ? Collections.<FieldComparator> emptyList() : comparatorOverrides) {
//            if (co.getMatchingNames().contains(left.getName())) {
            MatchCriteria match = co.getMatchCriteria();
            if (match.getBy() == MatchCriteria.BY.NAME && match.apply(left)) {
                comparatorClass = co.getType();
//                LOG.info("Comparator " + co + " Expected class " + comparatorClass + " Column " + left.getName());
                break;
            }
        }

        Map<TDColumn, TypeFormatter> formatterMap = getFormatters(comparatorClass, strongType, Arrays.asList(left, right));

        if (typeOverrides != null) {
            for (FieldTypeFormatter ftt : typeOverrides) {
                if (matches (ftt, left, TDSide.LEFT)) {
                    if (comparatorClass != null && ! comparatorClass.isAssignableFrom(ftt.getType())) {
                        throw new TDException(String.format("TypeForamtter override <%ts> cannot be assigned to " +
                                "LEFT column <%s>. Comparator Class expected class <%s>, but TypeFormatter will " +
                                "format into class <%s>.", ftt.getMatchCriteria(), left, comparatorClass, ftt.getType()));
                    }
                    else {
                        formatterMap.put (left, ftt);
                    }
                }
                if (matches (ftt, right, TDSide.RIGHT)) {
                    if (comparatorClass != null && ! comparatorClass.isAssignableFrom(ftt.getType())) {
                        throw new TDException(String.format("TypeFormatter override <%s> cannot be assigned to " +
                                "RIGHT column <%s>. Comparator Class expected class <%s>, but TypeFormatter will " +
                                "format into class <%s>.", ftt.getMatchCriteria(), right, comparatorClass, ftt.getType()));
                    }
                    else {
                        formatterMap.put( right, ftt);
                    }

                }
            }
        }

        Class leftClass = resolveClass(Optional.ofNullable(formatterMap.get(left)).map(TypeFormatter::getType).orElse(left.getColumnClass()));
        Class rightClass = resolveClass(Optional.ofNullable(formatterMap.get(right)).map(TypeFormatter::getType).orElse(right.getColumnClass()));

        if (leftClass != rightClass) {
            throw new TDException (String.format("Unable to format into a common class to do a comparison. Either a TypeFormatter override " +
                            "has changed the class of one column into a format that doesn't match the other column or no common " +
                            "can be found. LeftClass<%s>, RightClass<%s>, Left <%s>, Right <%s>, Formatters: <%s>",
                    leftClass, rightClass, left, right, formatterMap));
        }

        if (comparatorClass != null && !comparatorClass.isAssignableFrom(leftClass)) {
            throw new TDException (String.format("Cannot resolve Formatter; Comparator class is not assignable from the resolved column classes. " +
                            "Comparator class: <%s>, LeftClass<%s>, RightClass<%s>, Left <%s>, Right <%s>, Formatters: <%s>",
                    comparatorClass, leftClass, rightClass, left, right, formatterMap));
        }

        return formatterMap;
    }

    protected static boolean matches (FieldTypeFormatter tf, TDColumn column, TDSide side) {
//        LOG.info("Checking matching logic... " + column.getName() + " Side: " + side  + " tf " + tf + " matched? " + tf.getMatchCriteria().apply(column));
        return  tf.getMatchCriteria().apply(column)&& (tf.getSide() == TDSide.BOTH || tf.getSide() == side);
    }


    /**
     * If column class and the expected class is the same, a null will be returned (no formatting necessary.
     * If the classes are different a valid TypeFormatter will be returned. If none found, will throw an Exception.
     *
     * @param col
     * @param outClass
     * @return
     * @throws TDException
     */
    public static TypeFormatter getTypeFormatter (TDColumn col, Class outClass) throws TDException {
        Class inClass = resolveClass(col.getColumnClass());
        outClass = resolveClass(outClass);
        TypeFormatter tf = null;

        if (inClass != outClass) {
            tf = getTypeFormatter(inClass, outClass);
            if (tf == null) {
                throw new TDException(String.format ("No valid formatter found for column <%s> converting from class <%s> to class<%s>.",col, inClass, outClass));
            }
        }
        LOG.debug(String.format("TypeFormatter %s chosen for outClass %s, for column %s", tf, outClass, col));
        return tf;
    }

    public static TypeFormatter getTypeFormatter (Class fromClass, Class toClass) throws TDException {
        return getTypeFormatter(fromClass, toClass, null);
    }

    public static TypeFormatter getTypeFormatter (Class fromClass, Class toClass, String pattern) throws TDException {
        TypeFormatter tf = null;
        if (Comparable.class == toClass) {
            // Both Strings, both weak types, with a strong-type strategy... both qualify for Parsing at compare-time.
            // TODO: Implement compare-time Type Formatter for now, convert to String.
            tf = TO_STRING_FORMATTER;
        } else if (LocalDateTime.class == toClass && isDate(fromClass)) {
            tf = DATE_TO_LOCALDATETIME_FORMATTER;
        } else if (LocalDateTime.class == toClass && isStr(fromClass)) {
            tf = new LocalDateTimeFormatter(pattern);
        } else if (BigDecimal.class == toClass && isNum(fromClass)) {
            tf = NUMBER_TO_BIGDECIMAL_FORMATTER;
        } else if (BigDecimal.class == toClass && isStr(fromClass)) {
            tf = STRING_TO_BIGDECIMAL_FORMATTER;
        } else if (Boolean.class == toClass && isBool(fromClass)) {
            tf = STRING_TO_BOOLEAN_FORMATTER;
        } else if (String.class == toClass) {
            tf = TO_STRING_FORMATTER;
        }
        return tf;
    }

    final static Comparator <TDColumn> COLUMN_COMPARATOR =
            Comparator.comparing(TDColumn::getName)
            .thenComparing(TDColumn::getLabel)
            .thenComparing(TDColumn::isStrongType)
            .thenComparing(c -> c.getColumnClass().getName());

    public static Map<TDColumn, TypeFormatter> getFormatters(Class comparatorClass, boolean useStrongTypeRsolution, List<TDColumn> tdColumns) {
        Map<TDColumn, TypeFormatter> formatters = new TreeMap<>(COLUMN_COMPARATOR);
        Class convertIntoClass = null;
        TDColumn currentColumn  = null;
        // Figure out which class we should be converting into. This class should be one in which both columns
        // can be converted into successfully.
        for (TDColumn nextColumn : tdColumns) {
            if (currentColumn!=null) {
                convertIntoClass = resolveConvertIntoClass(comparatorClass, currentColumn, nextColumn, useStrongTypeRsolution);
            }
            currentColumn = nextColumn;
        }
        // Get the appropriate formatter
        for (TDColumn col : tdColumns) {
            // Find appropriate formatter if the column's class doesn't match the convert-into class.
            TypeFormatter tf = getTypeFormatter(col, convertIntoClass);
            if (tf != null) {
                formatters.put(col, tf);
            }
//            if (col.getColumnClass() != convertIntoClass) {
//                formatters.put(col, getTypeFormatter(col, convertIntoClass));
//            }
        }
        return formatters;
    }

    static Class resolveConvertIntoClass(Class comparatorClass, TDColumn aC, TDColumn bC, boolean useStrongTypeResolution) throws TDException {
        Class a = resolveClass(aC.getColumnClass());
        Class b = resolveClass(bC.getColumnClass());
//        LOG.info(String.format("Strong type resolution? <%s>; assignable Class <%s>", useStrongTypeResolution, comparatorClass));
        LOG.debug(String.format("Column A <%s>, class=<%s>, Column B <%s>, class=<%s>", aC.getLabel(), a.getName(), bC.getLabel(), b.getName()));

        if (a == b && isAssignable (comparatorClass, a )) {
            return a;
        }
        // The comparing class isn't a String, but is a number or a something else. If both columns are also Strings,
        // then it is expected that the Strings should be parsed into the assignableClass.
        else if (comparatorClass != null && !isStr(comparatorClass) && a.equals(b) && isStr(a)) {
            LOG.info(String.format("Force conversion of String into %s.", comparatorClass));
            if (isDate(comparatorClass)) {
                return LocalDateTime.class;
            }
            else if (isNum(comparatorClass)) {
                return BigDecimal.class;
            }
            else if (isBool(comparatorClass)) {
                return Boolean.class;
            }
            else {
                throw new TDException(String.format("Do not know how to select a conversion class for columsn <%s> and " +
                        "<%s> with comparing class %s", aC, bC, comparatorClass ));
            }

        }
        // Force all Dates into LocalDateTime's.
        else if (isDate(a) && isDate(b)) {
            if (isAssignable(comparatorClass, LocalDateTime.class)) {
                return LocalDateTime.class;
            }
            else {
                throw new TDException(String.format("Columns <%s>, <%s> are Date classes, but doesn't match " +
                        "comparing class <%s>", aC, bC, comparatorClass ));
            }
        }
        // Force all numbers into BigDecimals
        else if (isNum(a) && isNum(b)) {
            if (isAssignable(comparatorClass, Number.class)) {
                return BigDecimal.class;
            }
            else {
                throw new TDException(String.format("Columns <%s>, <%s> are Numeric classes, but doesn't match " +
                        "comparing class <%s>", aC, bC, comparatorClass ));
            }

        }
        // Both are Strings
        else if (isStr(a) && isStr(b)) {
            if (isAssignable(comparatorClass, String.class)) {
                return String.class;
            }
            else {
                throw new TDException(String.format("Columns <%s>, <%s> are String classes, but doesn't match " +
                        "comparing class <%s>", aC, bC, comparatorClass ));
            }
        }
        // If StrongType, then force all Strings into the "expected" class. Assumes the other non-String class is
        // the expected class.
        else if (useStrongTypeResolution) {
            if ((isDate(a) && isStr(b)) || (isStr(a) && isDate(b))) {
                if (isAssignable(comparatorClass, LocalDateTime.class) || isAssignable(comparatorClass, Date.class)) {
                    return LocalDateTime.class;
                }
                else if (isAssignable(comparatorClass, String.class)) {
                    return String.class;
                }
                else {
                    throw new TDException(String.format("Columns <%s>, <%s> are String/Date classes, but doesn't match " +
                            "comparing class <%s>", aC, bC, comparatorClass ));
                }
            }
            else if ((isNum(a) && isStr(b)) || (isStr(a) && isNum(b))) {
                if (isAssignable(comparatorClass, BigDecimal.class)) {
                    return BigDecimal.class;
                }
                else if (isAssignable(comparatorClass, String.class)) {
                    return String.class;
                }
                else {
                    throw new TDException(String.format("Columns <%s>, <%s> are String/Number classes, but doesn't match " +
                            "comparing class <%s>", aC, bC, comparatorClass ));
                }
            }
            else if ((isBool(a) && isStr(b)) || (isStr(a) && isBool(b))) {
                if (isAssignable(comparatorClass, Boolean.class)) {
                    return BigDecimal.class;
                }
                else if (isAssignable(comparatorClass, String.class)) {
                    return String.class;
                }
                else {
                    throw new TDException(String.format("Columns <%s>, <%s> are String/Boolean classes, but doesn't match " +
                            "comparing class <%s>", aC, bC, comparatorClass ));
                }
            }
            else {
                if (isAssignable(comparatorClass, String.class)) {
                    return String.class;
                }
                else {
                    throw new TDException(String.format("Columns <%s>, <%s> are String classes, but doesn't match " +
                            "comparing class <%s>", aC, bC, comparatorClass ));
                }
            }
        }
        else {
            if (isAssignable(comparatorClass, String.class)) {
                return String.class;
            }
            else {
                throw new TDException(String.format("Columns <%s>, <%s> would be converted to String classes, but doesn't match " +
                        "comparing class <%s>", aC, bC, comparatorClass ));
            }
        }
    }

    /**
     * If no Class is passed in, this will default to true because the class passed in here is the Comarator class;
     * if none passed in, this means no Comparator would be defined.
     *
     * @param assignableClass
     * @param a
     * @return
     */
    private static boolean isAssignable(Class assignableClass, Class a) {
        return assignableClass == null ? true : assignableClass.isAssignableFrom(a);
    }

    public static void main (String args[]) {
        System.out.println(isAssignable(BigDecimal.class, Number.class));
    }

    private static boolean isBool (Class c) {
        return Boolean.class.isAssignableFrom(c);
    }

    private static boolean isDate (Class c) {
        return Date.class.isAssignableFrom(c) || LocalDateTime.class.isAssignableFrom(c);
    }

    private static boolean isNum (Class c) {
        return Number.class.isAssignableFrom(c);
    }

    private static boolean isStr (Class c) {
        return String.class.isAssignableFrom(c);
    }

    /**
     * https://stackoverflow.com/questions/5032898/how-to-instantiate-class-class-for-a-primitive-type
     *
     * @param c
     * @return
     */
    private static Class resolveClass (Class c) {
        String className = c.getName();
        switch (className) {
            case "boolean": return Boolean.class;
            case "byte": return Byte.class;
            case "short": return Short.class;
            case "int": return Integer.class;
            case "long": return Long.class;
            case "float": return Float.class;
            case "double": return Double.class;
            case "char": return String.class;
            default: return c;
        }
    }
    /**
     * Finds the Lowest Common Denominator class that both Class a and Class for which this is true:
     *
     * returnedClass.isAssignableFrom(a;
     * returnedClass.isAssignableFrom(b);
     *
     * returnedClass must be either the exact Class that  both A and B are; or one of:
     * Boolean, LocalDateTime, String, Number. Otherwise a TDException will be thrown.
     *
     * @param a
     * @param b
     * @return
     */
    protected static Class lowestCommonAssignableClass (Class a, Class b) {
        if (a==b) return a;
        if(isStr(a) && isStr(b)) return String.class;
        if(isNum(a) && isNum(b)) return Number.class;
        if(Date.class.isAssignableFrom(a) && Date.class.isAssignableFrom(b)) return Date.class;
        if(LocalDateTime.class.isAssignableFrom(a) && LocalDateTime.class.isAssignableFrom(b)) return LocalDateTime.class;
        throw new TDException ("Cannot find lowest common assignable class between " + a + " and " + b +
                "returnedClass must be either the exact Class that  both A and B are; or one of: \n" +
                "     * Boolean, LocalDateTime, String, Number. Otherwise a TDException will be thrown.");
    }

    protected static Class lowestCommonAssignableClass (Class [] classes) {
        Class lcd = classes[0];
        for (int i=1; i<classes.length; i++) {
            lcd = lowestCommonAssignableClass(lcd, classes[i]);
        }
        return lcd;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends FormatterConfig{
        public Builder names(String... names) {
            this.fieldNames = names;
            return this;
        }

        public Builder parse (Class to) {
            return formatter (String.class, to);
        }

        public Builder parse (String pattern, Class to) {
            return formatter (pattern, new Class[] {String.class}, to);
        }

        public Builder formatter (Class from, Class to) {
            return formatter (new Class[] {from}, to);
        }

        public Builder formatter (Class from[], Class to) {
            return formatter(null, from, to);
        }

        public Builder formatter (String pattern, Class from[],  Class to) {
            this.from = from;
            this.to = to;
            this.pattern = pattern;
            return this;
        }

        public Builder side (TDSide side) {
            this.side = side;
            return this;
        }

        public FieldTypeFormatter buildFormatter() {
            return fromConfig(this);
        }

        public static FieldTypeFormatter fromConfig (FormatterConfig config) {
            Class from [] = config.getFrom();
            if (from == null) from = new Class[]{String.class};
            Class lcd = lowestCommonAssignableClass(from);
            TypeFormatter formatter = TypeFormatterLibrary.getTypeFormatter(lcd, config.getTo(), config.getPattern());
            return new FieldTypeFormatter(formatter, config.getSide(), new MatchCriteria(config.getFieldNames(), from));
        }

    }
}

