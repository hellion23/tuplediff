package com.hellion23.tuplediff.api.db;

import com.hellion23.tuplediff.api.Field;

/**
 * Created by margaret on 9/30/2014.
 */
public class SqlField extends Field {

    String columnClassName;
    String columnName;
    int columnType;
    int columnIndex;

    public SqlField(String name, int columnType, String columnClassName, String columnName,  int columnIndex) {
        this.name = name;
        this.columnIndex = columnIndex;
        this.columnClassName = columnClassName;
        this.columnName = columnName;
        this.columnType = columnType;
        try {
            this.fieldClass = Class.forName(columnClassName);
        }
        catch (ClassNotFoundException ex) {
            throw new RuntimeException("Could not instantiate field name " + name + " Class not found " + columnClassName);
        }
    }

    public String getColumnClassName() {
        return columnClassName;
    }

    public void setColumnClassName(String columnClassName) {
        this.columnClassName = columnClassName;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public int getColumnType() {
        return columnType;
    }

    public void setColumnType(int columnType) {
        this.columnType = columnType;
    }

    public int getColumnIndex() {
        return columnIndex;
    }

    public void setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
    }
}
