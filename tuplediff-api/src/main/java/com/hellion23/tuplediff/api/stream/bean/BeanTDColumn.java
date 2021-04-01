package com.hellion23.tuplediff.api.stream.bean;

import com.hellion23.tuplediff.api.model.TDColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

public class BeanTDColumn extends TDColumn {
    private static final Logger LOG  = LoggerFactory.getLogger(BeanTDColumn.class);
    Method getter;

    public BeanTDColumn (String label, Method getter) {
        super (label, getter.getReturnType());
        this.getter = getter;
    }

    public Object resolveValue (Object obj) {
        Object result = null;
        try {
            result = getter.invoke(obj);
        } catch (Exception e) {
            LOG.error("Could not invoke for column " + name, e);
        }
        return result;
    }
}
