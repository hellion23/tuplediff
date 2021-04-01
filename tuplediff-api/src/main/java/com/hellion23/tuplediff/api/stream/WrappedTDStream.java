package com.hellion23.tuplediff.api.stream;

import com.hellion23.tuplediff.api.model.TDException;
import com.hellion23.tuplediff.api.model.TDStream;
import com.hellion23.tuplediff.api.model.TDStreamMetaData;
import com.hellion23.tuplediff.api.model.TDTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by hleung on 5/24/2017.
 */
public abstract class WrappedTDStream  <M extends TDStreamMetaData, T extends TDTuple> implements TDStream <M, T> {
    private final static Logger LOG = LoggerFactory.getLogger(WrappedTDStream.class);
    protected TDStream <M, T> stream;

    public WrappedTDStream (TDStream <M, T> stream) {
        this.stream = stream;
    }

    @Override
    public M getMetaData() {
        return this.stream.getMetaData();
    }

    @Override
    public void open() throws TDException {
        this.stream.open();
    }

    @Override
    public boolean hasNext() {
        return this.stream.hasNext();
    }

    @Override
    public T next() {
        return this.stream.next();
    }

    @Override
    public boolean isOpen() {
        return this.stream.isOpen();
    }

    @Override
    public void close() throws Exception {
        this.stream.close();
    }

    @Override
    public String getName() {
        return this.stream.getName();
    }

    @Override
    public boolean isSorted () {
        return this.stream.isSorted();
    }

    public TDStream <M, T> getWrapped() {
        return this.stream;
    }

    @Override
    public String toString() {
        return "WrappedTDStream{" +
                "stream=" + stream +
                '}';
    }
}
