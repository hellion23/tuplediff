package com.hellion23.tuplediff.api;

import com.hellion23.tuplediff.api.compare.ComparatorLibrary;
import com.hellion23.tuplediff.api.config.MarshalUtils;
import com.hellion23.tuplediff.api.format.FieldTypeFormatter;
import com.hellion23.tuplediff.api.format.TypeFormatter;
import com.hellion23.tuplediff.api.format.TypeFormatterLibrary;
import com.hellion23.tuplediff.api.model.*;
import com.hellion23.tuplediff.api.stream.FormattedTDStream;
import com.hellion23.tuplediff.api.stream.InMemorySortedTDStream;
import com.hellion23.tuplediff.api.compare.CompareEvent;
import com.hellion23.tuplediff.api.compare.FieldComparator;
import com.hellion23.tuplediff.api.compare.PrimaryKeyTupleComparator;
import com.hellion23.tuplediff.api.handler.CompareEventHandler;
import com.hellion23.tuplediff.api.handler.ConsoleEventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

/**
 * Created by hleung on 5/24/2017.
 */
public class TDCompare implements AutoCloseable{
    private final static Logger LOG = LoggerFactory.getLogger(TDCompare.class);

    /**
     * Settable variables:
     */
    String name;
    List<String> primaryKey;
    List<String> primaryKeyLabel;
    TDStream left;
    TDStream right;
    List<String> excludeFields;
    boolean isStrongType = true;
    List<FieldComparator> comparators;
    List<FieldTypeFormatter> formatters;

    public TDStream normalizedLeftStream;
    public TDStream normalizedRightStream;
    CompareEventHandler eventHandler;
    ExecutorService executor;
    /**
     * Names of columns to compare. Excludes primary key & columns that appear in the left query but not in the right or
     * vice versa..
     */
    Map<String, FieldComparator> columnComparators = new HashMap<>();
    PrimaryKeyTupleComparator <TDTuple> primaryKeyComparator;

    volatile boolean cancelled = false;
    volatile boolean started = false;
    volatile boolean initialized = false;

    public TDCompare(){
        this (null, null, null, null);
    }

    public TDCompare (List<String> primaryKey, TDStream left, TDStream right, CompareEventHandler eventHandler ) {
        this(null, eventHandler, null, primaryKey, left, right, null, true, null, null);
    }

    public TDCompare (String name, CompareEventHandler eventHandler, ExecutorService executor,
                      List<String> primaryKey,
                      TDStream left, TDStream right, List<String> excludeFields,
                      boolean isStrongType, List<FieldComparator> comparators,
                      List<FieldTypeFormatter> formatters) {
        this.name = name == null ? "TDCompare <" + System.currentTimeMillis() + ">" : name;
        setPrimaryKeyLabel(primaryKey);
        this.left = left;
        this.right = right;
        this.excludeFields = excludeFields;
        this.isStrongType = isStrongType;
        this.comparators = comparators;
        this.formatters = formatters;
        this.eventHandler = eventHandler == null ? new ConsoleEventHandler() : eventHandler;
        this.executor = executor == null ? TupleDiff.defaultExecutor() : executor;
    }

    public void initialize() throws TDException {
        // Normalize column names.
        List<String> normalizedPrimaryKey = TDUtils.normalizedColumnNames(primaryKey);
        if (!normalizedPrimaryKey.equals(primaryKey)) {
            LOG.warn("Primary key columns (and all other column names are normalized, (upper-casing, removing" +
                    " special characters that may not port across different systems, etc...) and will not match" +
                    " the primary key column names passed in. Original: \n" + primaryKey + "\n Normalized " + normalizedPrimaryKey);
        }
        this.primaryKey = normalizedPrimaryKey;
        // normalize exclude fields too.
        this.excludeFields = TDUtils.normalizedColumnNames(excludeFields);

        try {
            left.getMetaData();
        } catch (TDException e) {
            LOG.error("Could not open LEFT stream... ", e);
            throw e;
        }
        try {
            right.getMetaData();
        } catch (TDException e) {
            LOG.error("Could not open RIGHT stream... ", e);
            throw e;
        }

        Set<String> leftCompareCols = getMetaDataColumnNames(left.getMetaData());
        Set<String> rightCompareCols = getMetaDataColumnNames(right.getMetaData());

        List<String> compareColumns = leftCompareCols.stream()
                // Shared column in both left & right streams.
                .filter(c -> rightCompareCols.contains(c))
                .collect(toList())
                ;

        Map<TDColumn, TypeFormatter> leftFormatters = new HashMap<>();
        Map<TDColumn, TypeFormatter> rightFormatters = new HashMap<>();

        this.columnComparators = new HashMap<>();

        FieldComparator pkComp [] = new FieldComparator[primaryKey.size()];
        Set<String> dontCompareFields = new HashSet<>( excludeFields == null ? Collections.emptySet() : excludeFields);
        // Normalize TDStreams to FormattedTDStreams.
        for (String col : compareColumns) {
            TDColumn lCol = left.getMetaData().getColumn(col);
            TDColumn rCol = right.getMetaData().getColumn(col);
            Map<TDColumn, TypeFormatter> xforms = TypeFormatterLibrary.resolveFormatters(lCol, rCol, formatters, comparators, isStrongType);
            leftFormatters.put(lCol, xforms.get(lCol));
            rightFormatters.put(rCol, xforms.get(rCol));

            Class clazz = xforms.containsKey(lCol) ? xforms.get(lCol).getType() : lCol.getColumnClass();
            FieldComparator columnComparator =
                    dontCompareFields.contains(col) ? ComparatorLibrary.dontCompareComparator(col) :
                    ComparatorLibrary.resolveComparator(new TDColumn(col, clazz), comparators);

            int pkIndex = primaryKey.indexOf(col);

            if (pkIndex < 0) {
                // put row comparators into it's map.
                columnComparators.put(col, columnComparator);
            }
            else {
                // put primary key comparator in the list, in the same order as the primary key.
                pkComp[pkIndex] = columnComparator;
            }
        }
        primaryKeyComparator = new PrimaryKeyTupleComparator<>(primaryKey, Arrays.asList(pkComp));

        // If either stream needs formatting to re-cast the type, wrap it in a FormattedTDStream.
        this.normalizedLeftStream = leftFormatters.size() > 0 ? new FormattedTDStream(left, leftFormatters) : left;
        this.normalizedRightStream = rightFormatters.size() > 0 ?  new FormattedTDStream(right, rightFormatters) : right;

        // Wrap these Streams in a sorter steam (RESULTing in IN MEMORY sorting! Very bad!!!!
        // Instead should provide streams that are already is better.
        if (!left.isSorted()) {
            LOG.info(left.getName() + " STREAM is not sorted. Will do an in memory sort! ");
            this.normalizedLeftStream = new InMemorySortedTDStream<>(primaryKey, primaryKeyComparator, this.normalizedLeftStream);
        }
        if (!right.isSorted())  {
            LOG.info(right.getName() + " STREAM is not sorted. Will do an in memory sort! ");
            this.normalizedRightStream = new InMemorySortedTDStream<>(primaryKey, primaryKeyComparator, this.normalizedRightStream);
        }

        // Validate that the primary key is present in both left & right streams:
        for (String pk : primaryKey) {
            if (this.normalizedLeftStream.getMetaData().getColumn(pk) == null ) {
                throw new TDException(this.normalizedLeftStream.getName() + " STREAM is missing primary key field " + pk + " MetaData: " + this.normalizedLeftStream.getMetaData());
            }
            if (this.normalizedRightStream.getMetaData().getColumn(pk) == null ) {
                throw new TDException(this.normalizedRightStream.getName() + " STREAM is missing primary key field " + pk + " MetaData: " + this.normalizedRightStream.getMetaData());
            }
        }

        LOG.info ("Initializing event handlers...");
        initEventHandlers();
        initialized =  true;
        LOG.info("TDCompare schema:\n" + schemaString());
    }

    private boolean isSortedByPrimaryKey(TDStream stream) {
        return stream.isSorted();
    }

    public void compare () throws TDException {
        if (started) {
            throw new TDException("This comparison "+name+" has already been STARTED. Cannot start a second time.");
        }
        else if (cancelled) {
            throw new TDException("This comparison "+name+" has already been CANCELLED. Create a new comparison.");
        }
        if (!initialized) {
            initialize();
        }
        long start = System.currentTimeMillis();
        started = true;
        long comparisonTime = 0;
        long totalTime = 0;

        try {
            LOG.info ("Opening streams (i.e. querying)...");
            openingStreams();

            LOG.info ("==== Begin comparison...");
            long startCompare = System.currentTimeMillis();
            compareStreams();
            comparisonTime = System.currentTimeMillis() - startCompare;
        } catch (InterruptedException e) {
            LOG.info("Comparison was interrupted: " + e.getMessage());
            throw new TDException("Comparison was interrupted: " + e.getMessage());
        } catch (TDException ex) {
            LOG.info("TupleDiff comparison error: " + ex.getMessage());
            throw ex;
        }
        finally {
            try {
                close();
            } catch (Exception e) {
                LOG.error("Error cleaning up ", e);
            }
        }
        LOG.info ("==== Comparison Time: " + comparisonTime + " ms");
        totalTime = System.currentTimeMillis() - start;
        LOG.info ("==== Total Time : " + totalTime + " ms");
        LOG.info("Comparison complete. ");
    }

    /**
     * Core logic to compare 2 streams. Since both streams are sorted by primary key, we can iterate over
     * the stream to match.
     */
    protected void compareStreams() {
        TDTuple left=null, right=null;
        final TDStream lrs = this.normalizedLeftStream;
        final TDStream rrs = this.normalizedRightStream;

        if (lrs.hasNext() && rrs.hasNext()) {
            left = lrs.next();
            right = rrs.next();
            eventHandler.accept(new CompareEvent(CompareEvent.TYPE.DATA_LEFT, left, null, null));
            eventHandler.accept(new CompareEvent(CompareEvent.TYPE.DATA_RIGHT, null, right, null));
            while (!isCancelled()) {
                int comp = primaryKeyComparator.compare (left, right);
                if (comp == 0) {
                    List<String> breakFields = compareRows (left, right);
                    if (breakFields != null && breakFields.size() > 0) {
                        eventHandler.accept(new CompareEvent(CompareEvent.TYPE.FOUND_BREAK, left, right, breakFields));
                    } else {
                        eventHandler.accept(new CompareEvent(CompareEvent.TYPE.FOUND_MATCHED, left, right, null));
                    }
                    if (lrs.hasNext() && rrs.hasNext()) {
                        left = lrs.next();
                        eventHandler.accept(new CompareEvent(CompareEvent.TYPE.DATA_LEFT, left, null, null));
                        right = rrs.next();
                        eventHandler.accept(new CompareEvent(CompareEvent.TYPE.DATA_RIGHT, null, right, null));
                    } else {
                        // If both side's don't have a tuple, then greedy read the other stream as left or right only tuples.
                        break;
                    }
                } else if (comp < 0) {
                    eventHandler.accept(new CompareEvent(CompareEvent.TYPE.FOUND_ONLY_LEFT, left, null, null));
                    if (lrs.hasNext()) {
                        left = lrs.next();
                        eventHandler.accept(new CompareEvent(CompareEvent.TYPE.DATA_LEFT, left, null, null));
                    } else {
                        eventHandler.accept(new CompareEvent(CompareEvent.TYPE.FOUND_ONLY_RIGHT, null, right, null));
                        break;
                    }
                } else { // comp > 0
                    eventHandler.accept(new CompareEvent(CompareEvent.TYPE.FOUND_ONLY_RIGHT, null, right, null));
                    if (rrs.hasNext()) {
                        right = rrs.next();
                        eventHandler.accept(new CompareEvent(CompareEvent.TYPE.DATA_RIGHT, null, right, null));
                    } else {
                        eventHandler.accept(new CompareEvent(CompareEvent.TYPE.FOUND_ONLY_LEFT, left, null, null));
                        break;
                    }
                }
            }
        }
        if (!isCancelled ()) {
            // Any uncompared items on either side is either left-only or right-only
            while (lrs.hasNext()) {
                left = lrs.next();
                eventHandler.accept(new CompareEvent(CompareEvent.TYPE.DATA_LEFT, left, null, null));
                eventHandler.accept(new CompareEvent(CompareEvent.TYPE.FOUND_ONLY_LEFT, left, null, null));
            }
            while (rrs.hasNext()) {
                right = rrs.next();
                eventHandler.accept(new CompareEvent(CompareEvent.TYPE.DATA_RIGHT, null, right, null));
                eventHandler.accept(new CompareEvent(CompareEvent.TYPE.FOUND_ONLY_RIGHT, null, right, null));
            }
        }
    }

    /**
     * Return any fields whose values don't match.
     * @param left
     * @param right
     * @return
     */
    protected List<String> compareRows(TDTuple left, TDTuple right) {
        final List <String> breakFields = new LinkedList <> ();

        columnComparators.entrySet().stream().forEach(me -> {
            final String columnName = me.getKey();
            FieldComparator comparator = me.getValue();
            Object lObj = left.getValue(columnName);
            Object rObj = right.getValue(columnName);

            if (comparator.getUnderlying() == ComparatorLibrary.ALWAYS_EQUAL_COMPARATOR) {
                // always equal.
            }
            else if (lObj == null && rObj == null) {
                // both equal
            }
            else if (lObj == null || rObj == null) {
                // one side is null
//                breakFields.add(columnName);
                breakFields.add(left.getMetaData().getColumn(columnName).getLabel());
            }
            else {
                try {
                    if (comparator.compare(lObj, rObj) != 0) {
//                        breakFields.add(columnName);
                        breakFields.add(left.getMetaData().getColumn(columnName).getLabel());
                    }
                } catch (Exception ex) {
                    throw new TDException (String.format("Could not compare column <%s>, leftValue=<%s>, rightValue=<%s>," +
                            "\nleftTuple: <%s>\nrightTuple: <%s>\n" +ex.getMessage(),
                            columnName, lObj, rObj, left, right
                            ));
                }
            }
        });
        return breakFields;
    }

    public boolean isCancelled() {
        return this.cancelled;
    }

    public void cancel () {
        this.cancelled = true;
        try {
            close();
        } catch (Exception e) {
            LOG.error("Error cancelling comparison", e);
        }
    }

    protected void initEventHandlers() {
        this.eventHandler.init(this);
    }


    protected void openingStreams() throws InterruptedException {
        CountDownLatch cdl = new CountDownLatch(2);
        StreamRunnable lsr = new StreamRunnable(TDSide.LEFT, normalizedLeftStream, cdl);
        StreamRunnable rsr = new StreamRunnable(TDSide.RIGHT, normalizedRightStream, cdl);
        executor.execute(lsr);
        executor.execute(rsr);
        cdl.await();
        if (lsr.ex != null) {
            throw new TDException("Could not open left stream: "+lsr.ex.getMessage());
        } else
        if (rsr.ex != null) {
            throw new TDException("Could not open right stream: "+rsr.ex.getMessage());
        }

    }


    protected Set<String> getMetaDataColumnNames (TDStreamMetaData metaData) {
        Set<String> compareColumns = new HashSet<String>(metaData.getColumnNames());
//        Optional.ofNullable(excludeFields).ifPresent(x -> compareColumns.removeAll(x));
        return compareColumns;
    }

    static class StreamRunnable implements Runnable {
        TDStream stream;
        CountDownLatch countDownLatch;
        Exception ex;
        TDSide side;

        public StreamRunnable (TDSide side, TDStream stream, CountDownLatch countDownLatch) {
            this.side = side;
            this.stream = stream;
            this.countDownLatch = countDownLatch;
        }

        @Override
        public void run() {
            final long startTime = System.currentTimeMillis();
            try {
                LOG.error(" Opening stream " + side + "...");
                stream.open();
                final long queryRunTime = System.currentTimeMillis() - startTime;
                LOG.info ("==== " + side + " stream opened. Runtime: " + queryRunTime + " ms");
            }
            catch (Exception ex) {
                this.ex = ex;
                LOG.error("Error opening stream " + side, ex);
                // If this Stream exception'ed, force all other streams on this CDL to stop also
                while (countDownLatch.getCount()> 0 ) {
                    countDownLatch.countDown();
                }
            }
            finally {
                this.countDownLatch.countDown();
            }
        }
    }

    public TDStream getNormalizedLeft() {
        return normalizedLeftStream;
    }

    public TDStream getNormalizedRight() {
        return normalizedRightStream;
    }

    public Set <String> getCompareColumns () {
        return Collections.unmodifiableSet(columnComparators.keySet());
    }

    public Map <String, FieldComparator> getColumnComparators() {
        return columnComparators;
    }

    @Override
    public void close() {
        try { if (eventHandler != null) eventHandler.close();
        }catch (Exception ex) {
            LOG.error("Could not close EventHandler: ", ex);
        }
        try { if (normalizedLeftStream != null) normalizedLeftStream.close(); }
        catch (Exception ex) {
            LOG.error("Could not close LEFT Stream: ", ex);
        }
        try { if (normalizedRightStream != null) normalizedRightStream.close(); }
        catch (Exception ex) {
            LOG.error("Could not close RIGHT Stream: ", ex);
        }
    }

    public String getName() {
        return name;
    }

    public List<String> getPrimaryKey() {
        return primaryKey;
    }

    public List<String> getPrimaryKeyLabel() {
        return primaryKeyLabel;
    }

    public TDStream getLeft() {
        return left;
    }

    public TDStream getRight() {
        return right;
    }

    public List<String> getExcludeFields() {
        return excludeFields;
    }

    public boolean isStrongType() {
        return isStrongType;
    }

    public List<FieldComparator> getComparators() {
        return comparators;
    }

    public List<FieldTypeFormatter> getFormatters() {
        return formatters;
    }

    public void setExcludeFields(List<String> excludeFields) {
        this.excludeFields = excludeFields;
    }

    public void setComparators(List<FieldComparator> comparators) {
        this.comparators = comparators;
    }

    public void setFormatters(List<FieldTypeFormatter> formatters) {
        this.formatters = formatters;
    }

    public void setPrimaryKeyLabel(List<String> primaryKeyLabel) {
        this.primaryKeyLabel = primaryKeyLabel;
        this.primaryKey = TDUtils.normalizedColumnNames(primaryKeyLabel);
    }

    public void setLeft(TDStream left) {
        this.left = left;
    }

    public void setRight(TDStream right) {
        this.right = right;
    }

    public void setName(String name) {
        this.name = name;
    }
    
    public ExecutorService getExecutor() {
        return executor;
    }

    public void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    public CompareEventHandler getEventHandler() {
        return eventHandler;
    }

    public void setEventHandler(CompareEventHandler eventHandler) {
        this.eventHandler = eventHandler;
    }

    public String schemaString() {
        StringBuilder sb = new StringBuilder();
        if (!initialized) {
            return sb.append("TDCompare Schema NOT_INITIALIZED \n").toString();
        }
        Formatter fmt = new Formatter(sb);
        LinkedHashMap<String, Integer> paddings = new LinkedHashMap<>();
        paddings.put("FIELD NAME",          35);
        paddings.put("PK INDEX",            10);
        paddings.put("COL INDEX",           10);
        paddings.put("COMPARATOR",          35);
        paddings.put("LEFT FMT CLASS",      20);
        paddings.put("RIGHT FMT CLASS",     20);
        paddings.put("LEFT ORIG CLASS",     20);
        paddings.put("RIGHT ORIG CLASS",    20);

        // Create top/bottom table border
        char[] border = new char[1 + paddings.size() + paddings.values().stream().mapToInt(Integer::intValue).sum()];
        Arrays.fill(border, '-');
        sb.append(border).append('\n');
        // Create row format based on the above defined paddings.
        String lineFmt = "|" + paddings.values().stream().map(s -> "%"+s+"s").collect(Collectors.joining("|")) + "|\n";
        // Write column header.
        fmt.format(lineFmt, new ArrayList(paddings.keySet()).toArray());
        sb.append(border).append('\n');

        List <String>allColumns = new ArrayList<>(primaryKey);
        allColumns.addAll(columnComparators.keySet());

        for (String col : allColumns) {
            String lColClass = left.getMetaData().getColumn(col).getColumnClass().getSimpleName();
            String rColClass = right.getMetaData().getColumn(col).getColumnClass().getSimpleName();
            String lColClassNormalized = normalizedLeftStream.getMetaData().getColumn(col).getColumnClass().getSimpleName();
            String rColClassNormalized = normalizedRightStream.getMetaData().getColumn(col).getColumnClass().getSimpleName();
            int pkInd = primaryKeyComparator.indexOf(col);
            Integer colIndLeft =  normalizedLeftStream.getMetaData().getIndex(col);
            Integer colIndRight =  normalizedRightStream.getMetaData().getIndex(col);
            Comparator comp = pkInd >=0 ?
                    primaryKeyComparator.map().get(col).getUnderlying() : // is Primary Key comparator
                    columnComparators.get(col).getUnderlying();           // is compare column comparator.
            String compName = comp.getClass().getSimpleName();
            if (comp.getClass().getPackage().getName().equals(MarshalUtils.comparePkg)) {
                compName = comp.toString(); // Comparators in comparePkg have pretty print.
            }
            fmt.format(lineFmt, col,
                    (pkInd < 0 ? "" : pkInd+1),
                    colIndLeft + "/" + colIndRight,
                    compName, lColClassNormalized, rColClassNormalized, lColClass, rColClass);
        }
        sb.append(border).append('\n');
        return sb.toString();
    }

}
