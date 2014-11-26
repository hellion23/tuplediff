package com.hellion23.tuplediff.api;

import com.hellion23.tuplediff.api.monitor.Nameable;

/**
 * @author: Hermann Leung
 * Date: 6/3/14
 */
public class Field implements Nameable, Comparable <Field> {
    protected String name;
    protected Class fieldClass;
    protected String expression;

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Class getFieldClass() {
        return fieldClass;
    }

    public void setFieldClass(Class fieldClass) {
        this.fieldClass = fieldClass;
    }

    @Override
    public final int compareTo(Field field) {
        if (field == null) return -1;
        else {
            return name.compareTo(field.getName());
        }
    }

    @Override
    public boolean equals (Object f) {
        if (f instanceof Field)
            return false;
        else
            return compareTo((Field)f) == 0;
    }
}
