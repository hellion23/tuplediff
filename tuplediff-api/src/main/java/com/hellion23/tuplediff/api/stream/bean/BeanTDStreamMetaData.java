package com.hellion23.tuplediff.api.stream.bean;

import com.hellion23.tuplediff.api.model.TDStreamMetaData;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.LinkedList;
import java.util.List;

@Slf4j
public class BeanTDStreamMetaData extends TDStreamMetaData<BeanTDColumn> {
    Class beanClass;

    public BeanTDStreamMetaData (Class beanClass) {
        this.beanClass = beanClass;
        init();
    }

    protected void init() {
        List<BeanTDColumn> columns = new LinkedList<>();
        log.info("Instantiating BeanTDStreamMetaData for class: " + beanClass );
        try {
            PropertyDescriptor[] pds = Introspector.getBeanInfo(beanClass, Object.class).getPropertyDescriptors();
            for (PropertyDescriptor pd : pds) {
                Method reader = pd.getReadMethod();
                if(!reader.isAccessible()) {
                    reader.setAccessible(true);
                }
                log.debug("For property: [{}], normalized column name: [{}], reader method: [{}]", pd.getName(), pd.getName(), reader.getName() );
                columns.add(new BeanTDColumn(pd.getName(), reader));
            }
        } catch (IntrospectionException e) {
            log.error("Could not create BeanTDStreamMetaData for bean of type class "+beanClass, e);
        }

        setColumns(columns);
    }
}
