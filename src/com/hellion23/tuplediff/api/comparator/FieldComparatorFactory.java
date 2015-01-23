package com.hellion23.tuplediff.api.comparator;


import com.hellion23.tuplediff.api.Field;
import com.hellion23.tuplediff.api.TupleDiffException;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import static com.hellion23.tuplediff.api.comparator.FieldComparator.*;
/**
 * @author: Hermann Leung
 * Date: 12/12/2014
 */
public class FieldComparatorFactory {
    static final FieldComparator ComparableFC = new ByBaseClass(Comparable.class, new ComparableComparator());
    static FieldComparatorFactory instance;
    List<FieldComparator> defaultFieldComparators;

    public FieldComparatorFactory () {
        defaultFieldComparators = new LinkedList<FieldComparator>();
        defaultFieldComparators.add(new ByBaseClass(Number.class, new ThresholdNumberComparator(.00001)));
    }

    public static FieldComparatorFactory Instance () {
        if (instance == null) {
            instance = new FieldComparatorFactory();
        }
        return instance;
    }

    public FieldComparator resolveField (Field field, List<FieldComparator> fco) {
        if (fco != null && fco.size() > 0) {
            for (FieldComparator fc : fco) {
                if (fc.canCompareWith(field)) {
                    return fc;
                }
            }
        }
       return getDefault(field);
    }

    public FieldComparator getDefault (Field field) {
        if (defaultFieldComparators != null && defaultFieldComparators.size() > 0) {
            for (FieldComparator dFC : defaultFieldComparators) {
                if (dFC.canCompareWith(field))
                    return dFC;
            }
        }

        if (ComparableFC.canCompareWith(field)) {
            return ComparableFC;
        }
        else {
            throw new TupleDiffException("No default FieldComparator could be found for the field "
            + field.getName() + " For field class " + field.getFieldClass().getName(), null);
        }
    }

    public FieldComparator createByFieldName (String fieldName, Comparator comparator) {
        return new ByName(fieldName, comparator);
    }

    public FieldComparator createByBaseClass (Class baseClass, Comparator comparator) {
        return new ByBaseClass(baseClass, comparator);
    }

    public List<FieldComparator> getDefaultFieldComparators() {
        return defaultFieldComparators;
    }

    public void setDefaultFieldComparators(List<FieldComparator> defaultFieldComparators) {
        this.defaultFieldComparators = defaultFieldComparators;
    }
}
