package com.hellion23.tuplediff.api.handler;

import com.hellion23.tuplediff.api.TDCompare;
import com.hellion23.tuplediff.api.model.TDStreamMetaData;
import com.hellion23.tuplediff.api.model.TDTuple;
import com.hellion23.tuplediff.api.compare.CompareEvent;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

/**
 * EventHandler that "flattens" a CompareEvent into a LinkedHashMap. Useful for processing the data in tabular format.
 * The columns created are:
 * EVENT: FOUND_ONLY_LEFT, FOUND_ONLY_RIGHT, BREAK
 * BREAK_SUMMARY: List<String> </String> break fields (List of fields that don't match). Only populated if event is BREAK.
 * PRIMARY KEYS COLUMNS: 1 value for each primary key, in order of Primary Key index.
 * COMPARE COLUMNS: 2 values for each compare columns Object[0] = Left Tuple Value, Object[1] = Right Tuple value.
 *
 * Created by hleung on 7/28/2017.
 */
@Slf4j
public abstract class AbstractFlatMapEventHandler implements CompareEventHandler {
    public final static String EVENT_FIELD = "EVENT";
    public final static String BREAK_SUMMARY_FIELD = "BREAK_SUMMARY";

    protected List<String> primaryKeys;
    protected List<String> columnNames;
    protected Set<String> compareColumns;
    protected TDStreamMetaData leftMetaData;
    protected TDStreamMetaData rightMetaData;

    @Override
    public void init(TDCompare comparison) {
        primaryKeys = new LinkedList<> (comparison.getPrimaryKey());
        compareColumns = comparison.getCompareColumns();
        this.leftMetaData = comparison.getNormalizedLeft().getMetaData();
        this.rightMetaData = comparison.getNormalizedRight().getMetaData();
        columnNames = new LinkedList<> ();
        columnNames.add(EVENT_FIELD);
        columnNames.add(BREAK_SUMMARY_FIELD);
        columnNames.addAll(primaryKeys);
        columnNames.addAll(compareColumns);
    }

    @Override
    public void accept(CompareEvent compareEvent) {
        LinkedHashMap<String, Object> row = null;
        switch (compareEvent.getType()) {
            case FOUND_BREAK:
                row = toMap(compareEvent.getType(), compareEvent.getFieldBreaks(), compareEvent.getLeft(), compareEvent.getRight());
                break;
            case FOUND_ONLY_LEFT:
                row = toMap(compareEvent.getType(), null, compareEvent.getLeft(), null);
                break;
            case FOUND_ONLY_RIGHT:
                row = toMap(compareEvent.getType(), null, null, compareEvent.getRight());
                break;
        }
        if (row != null) {
            accept(row);
        }
    }

    abstract void accept (LinkedHashMap<String, Object> breakRow);

    private LinkedHashMap<String, Object> toMap(CompareEvent.TYPE type, List<String> fieldBreaks, TDTuple left, TDTuple right) {
        LinkedHashMap<String, Object> row = new LinkedHashMap<>();
        row.put(EVENT_FIELD, type);
        row.put(BREAK_SUMMARY_FIELD, fieldBreaks);
        for (String pk : primaryKeys) {
            row.put(pk, Optional.ofNullable(left).orElse(right).getValue(pk));
        }
        for (String cc : compareColumns) {
            row.put (cc, new Object [] {
                Optional.ofNullable(left).map(t -> t.getValue(cc)).orElse(null),
                Optional.ofNullable(right).map(t -> t.getValue(cc)).orElse(null)
            });
        }
        return row;
    }


    @Override
    public void close() throws Exception {
        //Nothing to do.
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

}
