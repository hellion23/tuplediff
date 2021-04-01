package com.hellion23.tuplediff.api.stream.source;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

/**
 * Wrapper around an open-only-once InputStream/Reader. Explore potentially replacing with Spring's InputStreamSource
 * as it serves essentially the same prupose:
 *
 * https://docs.spring.io/spring-framework/docs/current/javadoc-api/org/springframework/core/io/InputStreamSource.html
 *
 */
@Slf4j
@EqualsAndHashCode
@ToString
public abstract class StreamSource implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(StreamSource.class);
    boolean opened = false;
    boolean closed = false;
    InputStream inputStream= null;
    Reader reader = null;

    /**
     * Multiple calls to open() is idempotent. A call to open() after a Stream has already been opened will
     * have no effect and return immediately.
     * A call to open() after the stream has been closed will throw an Exception.
     *
     * @throws IOException
     */
    public void open () throws IOException {
        if (opened) {
            return;
        }
        if (closed) {
            throw new IOException("Stream source was already closed. Cannot re-open! ");
        }
        inputStream = openSource();
        reader = new InputStreamReader(inputStream);
        opened = true;
    }

    protected abstract InputStream openSource() throws IOException;

    /**
     * This method by default just calls the close() method on the InputStream.
     * @throws Exception
     */
    protected void closeSource() throws Exception {
        if (inputStream != null)
            inputStream.close();
    };

    public InputStream inputStream() {
        return inputStream;
    }

    public Reader getReader () {
        return reader;
    }

    public boolean isOpen () {
        return opened;
    }

    public boolean isClosed () { return closed; }

    @Override
    public void close () throws Exception {
        try {
            closed = true;
            opened = false;
            closeSource();
        }
        catch (Exception ex) {
            LOG.error("Could not close stream {}", toString());
        }
        finally {
            inputStream = null;
            reader = null;
        }
    }

}
