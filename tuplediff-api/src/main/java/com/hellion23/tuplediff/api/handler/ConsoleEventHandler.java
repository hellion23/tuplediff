package com.hellion23.tuplediff.api.handler;

import com.hellion23.tuplediff.api.TDCompare;
import com.hellion23.tuplediff.api.model.TDColumn;
import com.hellion23.tuplediff.api.model.TDStreamMetaData;
import com.hellion23.tuplediff.api.model.TDTuple;
import com.hellion23.tuplediff.api.compare.CompareEvent;
import lombok.extern.slf4j.Slf4j;

import java.io.PrintStream;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Pretty prints breaks to a PrintStream (default= console).
 *
 * Created by hleung on 5/31/2017.
 */
@Slf4j
public class ConsoleEventHandler extends AbstractFlatMapEventHandler {
    private static final String datePattern = "MM/dd/yyyy HH:mm:ss.SSS";
    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern(datePattern);
    private static final SimpleDateFormat sdf = new SimpleDateFormat(datePattern);

    String lineFmt;
    int colLength = 20; // stupidly make every column the same length;
    char [] border;
    LinkedHashMap<String, Integer> paddings;
    boolean headersPrinted = false;
    long breakCount = 0;
    long maxToPrint;
    PrintStream printStream;

    public ConsoleEventHandler () {
        this (null);
    }

    public ConsoleEventHandler (Long maxToPrint) {
        this (maxToPrint, null);
    }

    public ConsoleEventHandler (Long maxToPrint, PrintStream printStream) {
        this.maxToPrint = maxToPrint == null ? 50 : maxToPrint;
        this.printStream = printStream == null ? System.out : printStream;
    }

    @Override
    public void init(TDCompare comparison) {
        super.init(comparison);
        paddings = new LinkedHashMap<>();
        for (String each : getColumnNames()) {
            int length = getLength(each, comparison.getNormalizedLeft().getMetaData());

            if (compareColumns.contains(each)) {
                paddings.put("L_" + each, length);
                paddings.put("R_" + each, length);
            }
            else {
                paddings.put(each, length);
            }
        }
        border = new char[1 + paddings.size() + paddings.values().stream().mapToInt(Integer::intValue).sum()];
        Arrays.fill(border, '-');
        // Create row format based on the above defined paddings.
        lineFmt = "|" + paddings.values().stream().map(s -> "%"+s+"s").collect(Collectors.joining("|")) + "|";
//        LOG.info("Paddings " + paddings + "\n line format: " + lineFmt + "\n"+ compareColumns + "\n" + compareColumnsOrdered);
    }

    private int getLength(String colName, TDStreamMetaData metaData) {
        if (BREAK_SUMMARY_FIELD == colName) {
            return 30;
        }
        else if (EVENT_FIELD == colName) {
            return 20;
        }
        else {
            TDColumn col = metaData.getColumn(colName);
            Class clazz = col.getColumnClass();
            int length = colName.length()+ (compareColumns.contains(col.getName()) ? 3 : 1);
            int suggestedLength = colLength;
            if (Date.class.isAssignableFrom(clazz))             suggestedLength = 25;
            if (LocalDateTime.class.isAssignableFrom(clazz))    suggestedLength = 25;
            if (Number.class.isAssignableFrom(clazz))           suggestedLength = 15;
            if (BigDecimal.class.isAssignableFrom(clazz))       suggestedLength = 15;
            if (String.class.isAssignableFrom(clazz))           suggestedLength = 23;
            log.debug("Column " + colName + " col " + col.getName() + " length " + length + " Suggested length " + suggestedLength);
            return length > suggestedLength ? length : suggestedLength;
        }
    }

    @Override
    void accept(LinkedHashMap<String, Object> breakRow) {
        breakCount++;
        if (breakCount > maxToPrint) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        Formatter fmt = new Formatter(sb);
        if (breakCount == maxToPrint) {
            sb
            .append(border).append('\n')
            .append("Breaks exceed max # to print <").append(maxToPrint).append("> Will not print anymore...\n")
            ;
            printString(sb.toString());
        }
        else {
            if (!headersPrinted) {
                headersPrinted = true;
                printHeaders (sb, fmt);
            }
            List<String> formatted = formatStrings(breakRow);
            fmt.format(lineFmt, formatted.toArray());
            printString(sb.toString());
        }
    }

    private void printHeaders (StringBuilder sb, Formatter fmt) {
        sb.append('\n').append(border).append('\n');
        fmt.format(lineFmt, new ArrayList(paddings.keySet()).toArray());
        sb.append('\n').append(border).append('\n');
    }

    private ArrayList<String> formatStrings (LinkedHashMap<String, Object> breakRow) {
        ArrayList<String> formatted = new ArrayList<>();
        for (Map.Entry<String, Object> me : breakRow.entrySet()) {
            String col = me.getKey();
            Object o = me.getValue();
            if (o == null) {
                formatted.add("");
            }
            else {
                if (compareColumns.contains(col)) {
                    Object [] pair = (Object[]) o;
                    formatted.add(formatString(col, pair[0]));
                    formatted.add(formatString(col, pair[1]));
                }
                else {
                    formatted.add(formatString(col, o));
                }
            }
        }
        return formatted;
    }

    private String formatString (String col, Object o) {
        String s = "";
        if (o == null) {
            return "";
        }
        int length = compareColumns.contains(col) ? paddings.get("L_"+col) : paddings.get(col);
        if (o instanceof LocalDateTime) {
            s = ((LocalDateTime)o).format(dtf);
        }
        else if (o instanceof Date) {
            s = sdf.format((Date)o);
        }
        else {
            s = o.toString();
        }
        if (s.length()> length) {
            s = s.substring(0, length-1);
        }
        return s;
    }

    @Override
    public void close() throws Exception {
        StringBuilder sb = new StringBuilder();
        Formatter fmt = new Formatter(sb);
        if (!headersPrinted) {
            headersPrinted = true;
            printHeaders (sb, fmt);
            sb.append("| FOUND 0 BREAKS! Congratulations!!! \n");
        }
        sb.append(border).append('\n');
        printString (sb.toString());
    }

    private void printString (String s) {
        printStream.println(s);
    }

    private void printRecord(CompareEvent.TYPE type, List<String> breakFields, TDTuple left, TDTuple right) {
        log.info(String.format("  EVENT: %s, BreakFields: %s, \nLEFT: <%s>\nRIGHT: <%s> ",
                type, breakFields, left, right));
    }
}
