package com.hellion23.tuplediff.api.comparator;

import java.util.Comparator;

/**
 * @author: Hermann Leung
 * Date: 9/29/2014
 */
public interface FieldComparator extends Comparator {
    public boolean canCompareWith (String className);
    public String getDesiredClassCast ();
}
