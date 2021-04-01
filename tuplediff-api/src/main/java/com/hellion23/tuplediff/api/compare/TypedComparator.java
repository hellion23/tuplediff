package com.hellion23.tuplediff.api.compare;

import com.hellion23.tuplediff.api.format.Typed;

import java.util.Comparator;

/**
 * Created by hleung on 7/30/2017.
 */
public interface TypedComparator <T> extends Comparator<T>, Typed<T> {

}
