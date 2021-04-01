package com.hellion23.tuplediff.api.compare;

import com.hellion23.tuplediff.api.format.MatchCriteria;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Comparator that compare Field values, This class accepts fieldNames and fieldClasses that this is
 * Comparator should be used for.
 *
 * Created by hleung on 6/2/2017.
 */
public class FieldComparator <T> implements TypedComparator <T> {
    private final static Logger LOG = LoggerFactory.getLogger(ComparatorLibrary.class);
    Comparator <T> comparator;
    TypedComparator <T> underlyingComp;
    MatchCriteria criteria;
    Class comparingClass;

    public FieldComparator ( TypedComparator <T> comparator, Class ... matchingClasses) {
        this (comparator, MatchCriteria.forClasses(matchingClasses));
    }

    public FieldComparator (TypedComparator <T> comparator, String... matchingNames) {
        this (comparator, MatchCriteria.forLabels(matchingNames));
    }

//    public FieldComparator (TypedComparator <T> comparator, Set<String> matchingNames, Set<Class> matchingClasses) {
    public FieldComparator (TypedComparator <T> comparator, MatchCriteria criteria) {
        this.underlyingComp = comparator;
        this.comparator = Comparator.nullsFirst(comparator);
        this.criteria = criteria;
        this.comparingClass = comparator.getType();
    }

    public MatchCriteria getMatchCriteria () {
        return criteria;
    }

    public TypedComparator getUnderlying () {
        return underlyingComp;
    }

    @Override
    public Class getType () {
        return comparingClass;
    }

    @Override
    public int compare(T o1, T o2) {
        return comparator.compare(o1, o2);
    }

    @Override
    public String toString() {
        return "FieldComparator{" +
                "comparator=" + underlyingComp +
                ", matchingCriteria=" + criteria +
                '}';
    }
}
