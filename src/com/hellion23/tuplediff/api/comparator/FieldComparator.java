package com.hellion23.tuplediff.api.comparator;

import com.hellion23.tuplediff.api.Field;
import com.hellion23.tuplediff.api.monitor.Nameable;

import java.util.Comparator;

/**
 * This class associates a Field with a Comparator.
 * See {@link com.hellion23.tuplediff.api.Config#setComparatorOverrides(java.util.List)}for more details on
 * how TupleComparison uses these Comparators.
 *
 * for
 *
 * @author: Hermann Leung
 * Date: 9/29/2014
 */
public abstract class FieldComparator implements Nameable {
    protected Comparator comparator;

    protected FieldComparator (Comparator comparator) {
        this.comparator = comparator;
    }

    public abstract boolean canCompareWith (Field field);

    public Comparator getComparator () {
        return comparator;
    }

    public void setName(String name) {}

    public String getName () {
        return
                this.getClass().getName() + "[" +
                this.comparator.getClass().getName() + "]";
    }

    public static class ByBaseClass extends FieldComparator {
        protected Class baseClass;
        protected ByBaseClass(Class baseClass, Comparator comp) {
            super (comp);
            this.baseClass = baseClass;
        }
        public boolean canCompareWith (Field field) {
            return baseClass.isAssignableFrom(field.getFieldClass());
        }

        @Override
        public String getName() {
            return super.getName()+" ["+baseClass.getName()+"]";
        }
        public Class getBaseClass () {
            return baseClass;
        }
    }

    public static class ByName extends FieldComparator {
        protected String fieldName;

        protected ByName(String fieldName, Comparator comp) {
            super (comp);
            this.fieldName = fieldName;
        }
        public boolean canCompareWith (Field field) {
            return this.fieldName.equals(field.getName());
        }

        @Override
        public String getName() {
            return super.getName()+ " ["+fieldName+"]";
        }

        public String getFieldName () {
            return fieldName;
        }
    }

    public static class ComparableComparator implements Comparator {

        @Override
        public int compare(Object o1, Object o2) {
            if (o1 == null && o2 == null) {
                return 0;
            }
            if (o1 == null) {
                return -1;
            }
            else if (o2 == null) {
                return 1;
            }
            else {
                return ((java.lang.Comparable)o1).compareTo(o2);
            }
        }
    }

}
