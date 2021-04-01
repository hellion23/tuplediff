package com.hellion23.tuplediff.api.model;

import com.hellion23.tuplediff.api.TDUtils;

import java.util.*;
import java.util.stream.IntStream;

/**
 * Describes the columns of the TDStream
 * Created by hleung on 5/23/2017.
 */
public class TDStreamMetaData <C extends TDColumn> {
    protected List<C> columns;

    /**
     * Convenience maps for fast lookups.
     */
    Map<C, Integer> indexMap;
    Map<String, Integer> colNameIndexMap;
    Map<String, C> colNameMap;
    List<String> columnNames;
    List<String> columnLabels;

    public TDStreamMetaData () {
        this(Collections.EMPTY_LIST);
    }

    public TDStreamMetaData(List<C> columns) {
        setColumns(columns);
    }

    /**
     * Returns all columns for a TDStream.
     * @return
     */
    public List<C> getColumns() {
        return columns;
    }

    public void setColumns (List<C> columns) {
        // Back by ArrayList for faster index lookups.
        this.columns = Collections.unmodifiableList(new ArrayList<> (columns));

        /**
         * Create multiple lookup maps for convenience methods.
         */
        this.indexMap = new HashMap<>();
        this.colNameIndexMap = new HashMap<>();
        this.colNameMap = new HashMap<>();
        this.columnNames = new LinkedList<>();
        this.columnLabels = new LinkedList<>();

        IntStream.range(0, columns.size()).forEach(n -> {
                final C col = this.columns.get(n);
                this.indexMap.put( col, n);
                this.colNameIndexMap.put (col.getName(), n);
                this.colNameMap.put(col.getName(), col);
                this.columnNames.add(col.getName());
                this.columnLabels.add(col.getLabel());
            }
        );

        this.columnNames = Collections.unmodifiableList(columnNames);
        this.columnLabels = Collections.unmodifiableList(columnLabels);
    }


    /**
     * Convenience method to get a column by name.
     * @param columnName
     * @return
     */
    public C getColumn (String columnName) {
        return this.colNameMap.get(columnName);
    }

    public C getColumnByLabel (String columnLabel) {
        return this.colNameMap.get(TDUtils.normalizeColumnName(columnLabel));
    }

    public C getColumn (Integer index) {
        return columns.get(index);
    }

    public List<String> getColumnNames () {
        return this.columnNames;
    }

    public List<String> getColumnLabels () {
        return this.columnLabels;
    }
    /**
     * Convenience method for getting column size.
     *
     * @return
     */

    public int getColumnCount () {
        return columns.size();
    }

    public Integer getIndex (C column) {
        return this.indexMap.get(column);
    }

    public Integer getIndex (String columnName) {
        return this.colNameIndexMap.get(columnName);
    }

    @Override
    public String toString() {
        return "TDStreamMetaData{" +
                "columns=" + columns +
                '}';
    }
}
