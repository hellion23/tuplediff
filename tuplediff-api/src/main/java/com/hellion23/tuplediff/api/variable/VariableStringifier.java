package com.hellion23.tuplediff.api.variable;

import com.hellion23.tuplediff.api.model.TDException;
import com.hellion23.tuplediff.api.stream.json.JSONValue;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Logic to transform a Variable resolved value into a String. This can convert the value into a single String using
 * the intoString() method if only a single String is expected or can create multiple Strings if intoStrings() is
 * called. Note that intoStrings() has no effect if:
 *  a) joinCollections is set to true
 *  b) value is not a Collection object.
 *
 */
@Slf4j
@ToString
public class VariableStringifier {
    public static VariableStringifier DEFAULT = new VariableStringifier();

    /**
     * These values are used for creating a VariableStringifier from a String definition.
     */
    public static final char SEPARATOR = ';';
    public static final String JOIN = "JOIN";
    public static final String FORMAT = "FORMAT";
    public static final String QUOTE_CHAR = "QUOTE_CHAR";
    public static final String QUOTE_LOGIC = "QUOTE_LOGIC";

    public  enum QUOTELOGIC {
        ALWAYS, // Will always apply logic to quote
        NEVER,  // Will never apply quote character to String.
        AUTO    // Will make the best determination when to quote:
                //    a) If Number, will never quote.
                //    b) If non-Number, will quote only if joinCollections is set to true AND the Object to be
                //          stringified is a Collection to be joined.
    }

    public static final String defaultDateTimeFormat = "yyyy-MM-dd HH:mm:ss.SSS";
    public static final String defaultDateFormat = "yyyyMMdd";
    public static final QUOTELOGIC defaultQuoteLogic = QUOTELOGIC.AUTO;
    public static final String defaultQuoteChar = "'";
    public static final boolean defaultJoinCollection = true;

    // Used for LocalDateTime and Date object types:
    String dateTimeFormat = null;
    // Used for LocalDate object types.
    String dateFormat = null;     // for formatting LocalDates
    DateTimeFormatter df = null;  // LocalDates
    DateTimeFormatter dtf = null; //LocalDateTimes
    SimpleDateFormat sdf = null;
    //        DateTimeFormatter LOCAL_DATE_FORMATTER = DateTimeFormatter.BASIC_ISO_DATE; // For LocalDates
    String quoteChar;

    String joinDelim = ",";
    boolean joinCollections;
    QUOTELOGIC quoteLogic;

    public VariableStringifier () {
        this(defaultJoinCollection, defaultDateTimeFormat, defaultDateFormat, defaultQuoteLogic, defaultQuoteChar);
    }

    /**
     * Create a custom Stringifier for a calculated Variable value.
     *
     * @param joinCollections If the calculated value is a Collection, setting this to false will result in
     *                        multiple Strings being created. See intoStrings() method. Default is true, which would
     *                        join all the Objects within the Collection with a comma (and optionally quoted).
     * @param dateTimeFormat  For Date, LocalDateTime objects, use this format.
     * @param dateFormat      For LocalDate objects, use this format.
     * @param quoteLogic      AUTO, ALWAYS, NEVER. See the enum for more details.
     * @param quoteChar       Character used for quoting.
     */
    public VariableStringifier (boolean joinCollections, String dateTimeFormat, String dateFormat,
                                QUOTELOGIC quoteLogic, String quoteChar) {
        this.joinCollections = joinCollections;
        this.dateFormat = dateFormat == null ? defaultDateFormat : dateFormat;
        this.dateTimeFormat = dateTimeFormat == null ? defaultDateTimeFormat : dateTimeFormat;
        this.quoteLogic = quoteLogic  == null ? QUOTELOGIC.AUTO : quoteLogic;
        this.quoteChar = quoteChar == null ? defaultQuoteChar : quoteChar;
    }

    /**
     * For a given variable definition, e.g. "The Date is ${DATE;FORMAT=YYMMDD;QUOTE_CHAR="}", this method will
     * create a VariableStringifier using the value = "FORMAT=YYMMDD;QUOTE_CHAR="". The available settings are:
     *
     * JOIN = If Collection, then if set to false, will result in multiple Strings being created.
     * FORMAT = Format the date to specified format.
     * QUOTE_CHAR = The quote character to use when quoting.
     * QUOTE_LOGIC = NEVER, ALWAYS, AUTO. Let the VariableStringifier determine when best to add quotes. It will
     *  for example not quote numbers ever, but will quote Strings if it is inside a Collection.
     *
     * @param settings SETTING_1=VALUE1;SETTING_2=VALUE2; etc...
     * @return
     */
    public static VariableStringifier create (String settings) {
        if (settings == null || "".equals(settings.trim())) {
            return DEFAULT;
        }
        else {
            String dateTimeFormat = defaultDateTimeFormat;
            String dateFormat = defaultDateFormat;
            boolean joinCollections = defaultJoinCollection;
            QUOTELOGIC quoteLogic = defaultQuoteLogic;
            String quoteChar = defaultQuoteChar;

            for (String definition : settings.split(""+SEPARATOR)) {
                String def [] = definition.split("=");
                if (def.length!=2) {
                    throw new TDException(" VariableStringifier logic must be in a format where: " +
                            " options ar separated by semicolon ';' and setting name and value separated by an equal '=' " +
                            " e.g. ${TODAYS_DATE;FORMAT=YYYYMMDD;QUOTE='");
                }
                String setting = def[0].toUpperCase();
                switch (setting) {
                    case JOIN:
                        joinCollections = "TRUE".equalsIgnoreCase(def[1]);
                        break;
                    case FORMAT:
                        dateFormat = def[1];
                        dateTimeFormat = def[1];
                        break;
                    case QUOTE_CHAR:
                        quoteChar = def[1];
                        break;
                    case QUOTE_LOGIC:
                        quoteLogic = QUOTELOGIC.valueOf(def[1].toUpperCase());
                        break;
                    default:
                        throw new TDException("No support for custom logic <"+setting+">. Only supports JOIN, FORMAT, QUOTE");
                }
            }
            return new VariableStringifier( joinCollections, dateTimeFormat, dateFormat, quoteLogic, quoteChar);
        }
    }

    public String intoString (Object value) {
        Boolean shouldQuote = shouldQuote(value, false);
        return asString(value, shouldQuote);
    }

    public List<String> intoStrings (Object value) {
        List<String> resultList;
        if (joinCollections || !(value instanceof Collection)) {
            resultList = Collections.singletonList(intoString(value));
        }
        else {
            resultList = new LinkedList<>();
            Collection values = (Collection) value;
            Boolean shouldQuote = null;
            for (Object o : values) {
                if (shouldQuote == null) {
                    shouldQuote = shouldQuote(o, true);
                }
                resultList.add(asString(o, shouldQuote));
            }
        }
        return resultList;
    }

    /**
     * Pretty print the Object into a String. null's are rendered as empty String
     * LocalDate is rendered as YYYYMMdd
     * LocalDateTime or java.util.Date is rendered as YYYYMMdd HH:mm:SS.sss
     * List objects are joined by a comma and wrapped with a quote (') if they are non numeric.
     * Everything else is .toString()'ed
     *
     * @param value Object to be rendered as a String
     * @param useQuote Quote if necessary
     * @return
     */

    protected String asString(Object value, Boolean useQuote) {
        String string;
        if (value == null) {
            string = "";
        }
        else if (value instanceof String) {
            string = (String)value;
        }
        else if (value instanceof LocalDate) {
            string = df().format((LocalDate)value);
        }
        else if (value instanceof LocalDateTime) {
            string = dtf().format((LocalDateTime)value);
        }
        else if (value instanceof Date) {
            string = sdf().format((Date)value);
        }
        else if (value instanceof Collection) {
            string = stringifyCollection ((Collection)value, useQuote);
        }
        else if (value instanceof JSONValue) {
            // unwrap the underlying Object and stringify it.
            JSONValue jsonValue = (JSONValue) value;
            return asString(jsonValue.getComparable(), useQuote);
        }
        else {
            string = value.toString();
        }
        if (Boolean.TRUE == useQuote && !(value instanceof Collection)) {
            string = this.quoteChar + string + quoteChar;
        }
        return string;
    }

    protected Boolean shouldQuote (Object value, boolean isInsideCollection) {
        Boolean shouldQuote;
        switch (quoteLogic) {
            case NEVER:
                shouldQuote = false;
                break;
            case ALWAYS:
                shouldQuote = true;
                break;
            case AUTO:
                if (value == null) {
                    shouldQuote = false;
                } else if (value instanceof Number) {
                    shouldQuote = false;
                } else if (value instanceof Collection) {
                    // Don't know whether to quote yet because we don't know the contents of the Collection.
                    shouldQuote = null;
                } else {
                    // If this is a String and is inside a collection, then quote.
                    shouldQuote = isInsideCollection;
                }
                break;
            default:
                shouldQuote=false;
        }
        return shouldQuote;
    }

    protected String stringifyCollection(Collection collection, Boolean useQuote) {
        StringBuilder sb = new StringBuilder();
        Iterator it = collection.iterator();
        if (!it.hasNext()) {
            return "";
        }
        Object value = it.next();
        if (useQuote == null) {
            useQuote = shouldQuote(value, true);
        }
        sb.append(asString(value, useQuote));
        while (it.hasNext()) {
            sb.append(joinDelim);
            sb.append(asString(it.next(), useQuote));
        }
        return sb.toString();
    }

    protected SimpleDateFormat sdf () {
        if (sdf == null) {
            sdf = new SimpleDateFormat(dateTimeFormat);
        }
        return sdf;
    }

    protected DateTimeFormatter dtf () {
        if (dtf == null) {
            dtf = DateTimeFormatter.ofPattern(dateTimeFormat);
        }
        return dtf;
    }

    protected DateTimeFormatter df () {
        if (df == null) {
            df = DateTimeFormatter.ofPattern(dateFormat);
        }
        return df;
    }
}
