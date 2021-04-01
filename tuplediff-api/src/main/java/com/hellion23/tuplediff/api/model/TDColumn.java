package com.hellion23.tuplediff.api.model;

import com.hellion23.tuplediff.api.TDUtils;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Created by hleung on 5/23/2017.
 * A TDColumn is equal iff the name is equal. The name is equal if the
 */
@ToString
@EqualsAndHashCode
public class TDColumn {
    protected String name;
    protected String label;
    protected Class <?> columnClass;
    boolean isStrongType;

    public TDColumn () {
        this(null, null);
    }

    public TDColumn (String label, Class<?> clazz) {
        this (label, clazz, true);
    }

    public TDColumn(String label, Class<?> clazz, boolean isStrongType) {
        setLabel(label);
        this.columnClass = clazz;
        this.isStrongType = isStrongType;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
        this.name = TDUtils.normalizeColumnName(label);
    }

    public Class<?> getColumnClass() {
        return columnClass;
    }

    public void setColumnClass(Class<?> columnClass) {
        this.columnClass = columnClass;
    }

    public boolean isStrongType() {
        return isStrongType;
    }

    public void setStrongType(boolean isStrongType) {
        this.isStrongType = isStrongType;
    }

}
