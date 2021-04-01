package com.hellion23.tuplediff.api.stream.bean;

import com.hellion23.tuplediff.api.format.Typed;
import com.hellion23.tuplediff.api.model.TDTuple;
import com.hellion23.tuplediff.api.stream.AbstractBufferingTDStream;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Stream of Beans. Metadata is extracted from the Class object.
 *
 * @param <R> Bean Class type
 */
public class BeanTDStream<R> extends AbstractBufferingTDStream<BeanTDStreamMetaData, TDTuple<BeanTDStreamMetaData>, R> implements Typed<R> {
    Iterator<R> iterator;
    Class<R> beanClass;
    List<String> sortKey;

    public BeanTDStream(String name, Class beanClass, Iterator<R> iterator) {
        this(name, beanClass, iterator, null);
    }

    public BeanTDStream(String name, Class beanClass, Iterator<R> iterator, List<String> sortKey) {
        super(name);
        this.beanClass = beanClass;
        this.iterator = iterator;
        this.sortKey = sortKey;
    }

    @Override
    protected BeanTDStreamMetaData constructMetaData() {
        return new BeanTDStreamMetaData(this.getType());
    }

    @Override
    protected TDTuple constructTuple(R bean) {
        List<Object> values = new LinkedList<>();
        List<BeanTDColumn> columns = metadata.getColumns();
        for (BeanTDColumn column : columns) {
            values.add(column.resolveValue(bean));
        }
        return new TDTuple(metadata, values);
    }

    @Override
    protected Iterator<R> iterator() {
        return this.iterator;
    }

    @Override
    public  Class getType() {
        return beanClass;
    }

    @Override
    protected void openStream() throws Exception {
        //NOOP
    }

    @Override
    protected void closeStream() throws Exception {
        //NOOP
    }

    public Iterator<R> getIterator() {
        return iterator;
    }

    public void setIterator(Iterator<R> iterator) {
        this.iterator = iterator;
    }
}
