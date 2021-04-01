package com.hellion23.tuplediff.api.model;

import com.hellion23.tuplediff.api.TDUtils;

import java.util.*;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.joining;

/**
 * Represents a "row" emitted from TDStream.
 *
 * Created by hleung on 5/23/2017.
 */
public class TDTuple <M extends TDStreamMetaData> {
    List<Object> values;
    M metaData;

    public TDTuple (final M metaData, List<Object> values  ) {
        this.metaData = metaData;
        this.values = Collections.unmodifiableList(new ArrayList<>(values));
    }

    public List<TDColumn> getColumns () {
        return metaData.getColumns();
    }

    public List<Object> getValues () {
        return values;
    }

    public Object getValue (TDColumn column) {
        Integer index = metaData.getIndex(column);
        if (index == null) {
            return null;
        }
        return values.get(metaData.getIndex(column));
    }

    public Object getValue (Integer columnIndex) {
        if (columnIndex == null) {
            return null;
        }
        return values.get(columnIndex);
    }

    /**
     *
     * @param columnName
     * @return
     */
    public Object getValue (String columnName) {
        Integer index = metaData.getIndex(columnName);
        if (index == null) {
            return null;
        }
        return this.values.get(index);
    }

    /**
     * This is the same as getValue except that the column is a label. This is slow because it
     * first calls a method to normalize the column name before doing a getValue()
     *
     * @param columnLabel
     * @return
     */
    public Object getValueForLabel (String columnLabel) {
        return getValue(TDUtils.normalizeColumnName(columnLabel));
    }

    public void setMetaData(M metaData) {
        this.metaData = metaData;
    }

    public M getMetaData() {
        return this.metaData;
    }

    public void setValues (List<Object>  values) {
        this.values = values;
    }

    public String toString () {
        StringBuilder sb = new StringBuilder("TDTuple {");
        return "TDTuple {" +
           IntStream.range(0, metaData.getColumnCount())
                   .mapToObj(i -> metaData.getColumn(i).getName() + "=" + values.get(i)).collect(joining (", ")) +
        "}";
    }
}
