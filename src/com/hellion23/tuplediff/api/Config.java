package com.hellion23.tuplediff.api;

import com.hellion23.tuplediff.api.comparator.FieldComparator;
import com.hellion23.tuplediff.api.listener.CompareEventListener;
import com.hellion23.tuplediff.api.monitor.Monitor;
import com.hellion23.tuplediff.api.monitor.Nameable;

import java.util.*;

/**
 * Created by Hermann on 9/28/2014.
 */
public class Config implements Nameable {
    TupleStream leftStream;
    TupleStream rightStream;
    String name;
    List<FieldComparator> comparatorOverrides;

    public Config () {
        this (null);
    }

    public Config (String name) {
        if (name == null) {
            this.name = "Unnamed TupleComparison # " + System.currentTimeMillis();
        }
        else {
            this.name = name;
        }
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return this.name;
    }

    public TupleStream getLeftStream() {
        return leftStream;
    }

    public void setLeftStream(TupleStream leftStream) {
        this.leftStream = leftStream;
    }

    public TupleStream getRightStream() {
        return rightStream;
    }

    public void setRightStream(TupleStream rightStream) {
        this.rightStream = rightStream;
    }

    public List<FieldComparator> getComparatorOverrides() {
        return comparatorOverrides;
    }

    /**
     * FieldComparator overrides are evaluated in the order that they are defined in the list; i.e. the first
     * FieldComparator.compareWith(field) that returns true in the list will be the one that will be used for this field.
     * That means the most restrictive Comparators should be defined first, and the least restrictive, last.
     * For example, all Field Name specific comparators should be first in the list and Field Class based
     * comparators should go last (ordered by instanceof class hierarchy).
     *
     * Use FieldComparatorFactory's createByFieldName or createByBaseClass to create a FieldComparator that associates
     * a field name or class to a particular Comparator.
     *
     * @param comparatorOverrides
     */
    public void setComparatorOverrides(List<FieldComparator> comparatorOverrides) {
        this.comparatorOverrides = comparatorOverrides;
    }

}
