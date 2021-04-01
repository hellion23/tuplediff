package com.hellion23.tuplediff.api.stream;

import com.hellion23.tuplediff.api.model.TDException;
import com.hellion23.tuplediff.api.model.TDStream;
import com.hellion23.tuplediff.api.model.TDStreamMetaData;
import com.hellion23.tuplediff.api.model.TDTuple;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * This Abstract class buffers X number of tuples to ascertain MetaData by exposing the peek()
 * methods to introspect the Tuples that have been generated without actually traversing the stream. This
 * class is used to generate TDStreamMetaData after opening the Stream, in cases where the schema is
 * unknown without evaluating the Tuples that have been traversed. The buffer in this class contains the raw
 * object produced from the Stream (Strings, CSVRecords, etc...)
 * The class also keeps state of open/closed.
 * It will call the constructMetaData after the openStream() method has been called and several O objects
 * have been buffered.
 * This is an open one-time stream. Once closed, cannot be re-opened. Once opened cannot be re-opened.
 *
 * @param <M>
 * @param <T>
 * @param <O> This is the object to be fetched from the underlying stream, raw and un-transformed into a tuple.
 */
public abstract class AbstractBufferingTDStream<M extends TDStreamMetaData, T extends TDTuple, O> implements TDStream <M, T> {
    protected int maxBuffer;
    protected String name;
    protected List<O> buffer;
    protected boolean opened = false;
    protected boolean closed = false;
    protected M metadata = null;

    protected AbstractBufferingTDStream(String name) {
        this(name, 1);
    }

    protected AbstractBufferingTDStream(String name, int buffer) {
        this.name = name;
        this.maxBuffer = buffer;
        this.buffer = new LinkedList<>();
    }

    /**
     * 1) Calls openStream()
     * 2) fills buffer with up to maximum size
     * 3) Calls the constructMetaData() method to construct metadata if it hasn't already
     * @throws TDException
     */
    @Override
    public void open() throws TDException {
        if (opened) {
            // If already opened, do nothing.
            return;
        }
        if (closed) {
            throw new TDException(name + " STREAM cannot re-opened a closed stream. Open attempt already made or open failed.");
        }

        try {
            openStream();
            // Pre-fetch the maxBuffer tuples
            fillBuffer();
            opened = true;
            if (metadata == null) {
                metadata = constructMetaData();
            }
        }
        catch (Exception ex) {
            closed = true;
            throw new TDException(name + " STREAM cannot open stream. " + ex.getMessage(), ex);
        }
    }

    protected void fillBuffer() {
        while (iterator().hasNext() && this.buffer.size() < this.maxBuffer) {
            O tuple = iterator().next();
            if (tuple != null) {
                this.buffer.add(tuple);
            }
        }
    }

    /**
     * Return the metadata. If not opened, will open stream. After opening metadata must be defined.
     * @return
     */
    @Override
    public M getMetaData() {
        if (metadata == null) {
            open();
        }
        return metadata;
    }

    @Override
    public boolean hasNext() {
        return opened && !closed && this.buffer.size()>0;
    }

    @Override
    public T next() {
        T next;
        if (hasNext() ) {
            next = constructTuple(this.buffer.remove(0));
            fillBuffer();
            if (this.buffer.size() == 0) {
                this.closed = true;
            }
        }
        else {
            throw new TDException(name + " STREAM cannot fetch next as there is no more to fetch!" );
        }
        return next;
    }

    @Override
    public boolean isOpen() {
        return opened && !closed;
    }

    @Override
    public void close() throws Exception {
        try {
            closeStream();
        }
        finally {
            closed = true;
        }
    }

    @Override
    public String getName() {
        return name;
    }

    /**
     * Helper method to get all available tuples that we can possibly look ahead
     * @return
     */
    protected List<O> peekAll() {
        return peek(maxBuffer);
    }

    /**
     * Helper method. Looks at the first Tuple. Returns null if none exist.
     *
     * @return
     */
    protected O peek () {
        List<O> values = peek(1);
        if (values.size()> 0)
            return values.get(0);
        else
            return null;
    }

    /**
     * Look ahead X number of objects. This is used to create metadata information without
     * actually traversing the stream.
     * This method can return fewer than the requested number of records
     *
     * @return
     */
    protected List<O> peek (int i) {
        if (buffer.size() < maxBuffer) {
            fillBuffer();
        }
        if (buffer.size() < 1) {
            return Collections.EMPTY_LIST;
        }
        else {
            return this.buffer.subList(0, Math.min(i, buffer.size()));
        }
    }


    /**
     * Open the stream. The open() method invokes this method and it is expected that the metaData should be created
     *
     *
     * @throws TDException
     */
    protected abstract void openStream () throws Exception;

    /**
     * This method is called *after* openStream() has been called and a number of items have been buffered. These
     * objects are made available for introspection using the peek() methods.
     * @return
     */
    protected abstract M constructMetaData();

    /**
     * Close the stream. Called by the close() method; implementers should keep track of whether
     * the underlying stream is closed, but should not overrride the open/close variables in this
     * abstract class since the underlying stream can be closed but there can still be items
     * remaining in the buffer. After this method is called, the method underlyingHasNext()
     * should always return false.
     *
     * @throws Exception
     */
    protected abstract void closeStream() throws Exception;

    /**
     * Construct the Tuple from the fetched object. At this point the metadata should have been ascertained
     * from the tuples (which is accessible using the peek methods).
     *
     * @param object
     * @return
     */
    protected abstract T constructTuple(O object);

    /**
     * This method should return the iterator() of the underlying Stream. It returns Objects that are passed to the
     * constructTuple method. Immediately after opening the stream, Objects are buffered to it's maximum buffered size.
     * Objects retrieved from the iterator's next() method are buffered and made available
     * to peek()
     *
     * @return an Iterator of objects of the underlying stream.
     */
    protected abstract Iterator<O> iterator();
}
