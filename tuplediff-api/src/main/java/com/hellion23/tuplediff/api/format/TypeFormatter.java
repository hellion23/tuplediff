package com.hellion23.tuplediff.api.format;

import java.util.function.Function;

/**
 * This formats values returned from a TDTuple into a Comparable object. Because some field=value attributes of the
 * tuple form the primary key, which must be sortable, the value must be formatted into a Comparable. This is not
 * necessary for non-primary key fields where only equals need to be properly implemented, but an overly
 * conservative approach is beneficial as tuple values that are Comparable simplifies the development of easily
 * extensible Comparator objects used to compare the actual values when it can be assumed that all compared objects
 * are already Comparable (i.e. can be compared simply using a.compareTo(b).
 *
 * Created by hleung on 5/24/2017.
 */
public abstract class TypeFormatter <T, R extends Comparable> implements Function<T, R>, Typed<R> {

    @Override
    public String toString(){
        return "TypeFormatter{"+ getType().getSimpleName()+"}";
    }
}
