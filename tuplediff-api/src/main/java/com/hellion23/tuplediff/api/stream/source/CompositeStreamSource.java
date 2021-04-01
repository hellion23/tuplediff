package com.hellion23.tuplediff.api.stream.source;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Enumeration;
import java.util.Iterator;

/**
 * Basic implementatation that links the sequential StreamSources to sequentiial inputStreams
 * @param <S>
 */
@Slf4j
public abstract class CompositeStreamSource <S extends StreamSource> extends StreamSource implements Iterator<S>, Enumeration<InputStream> {
    private boolean didNext;
    protected S next;

    @Override
    protected InputStream openSource() throws IOException {
        if(!hasNext()) {
            throw new IOException("No valid Steams to iterate over!");
        }
        return new SequenceInputStream(this);
    }

    @Override
    public boolean hasNext() {
        if (!didNext) {
            next = doNext();
            this.didNext = true;
        }
        return next != null;
    }

    @Override
    public S next() {
        if (!didNext) {
            doNext();
        }
        didNext = false;
        return next;
    }

    @Override
    public boolean hasMoreElements() {
        return !isClosed() && hasNext();
    }

    @Override
    public InputStream nextElement() {
        return next().inputStream();
    }

    /**
     * Implementers must calculate the next Stream with an open and non-null InputStream. A null signifies there
     * are no more to read.
     *
     * @return
     */
    public abstract S doNext();
}
