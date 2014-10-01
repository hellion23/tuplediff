package com.hellion23.tuplediff.api;

/**
 * @author: Hermann Leung
 * Date: 6/3/14
 */
public class Field {
    String name;
    Class fieldClass;

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
}
