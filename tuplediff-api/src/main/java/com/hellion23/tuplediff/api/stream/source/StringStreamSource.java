package com.hellion23.tuplediff.api.stream.source;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * It is highly discouraged to use a StringStreamSource unless the source string is very small. This can easily
 * cause the
 */
public class StringStreamSource extends StreamSource {
    String string;

    public StringStreamSource (String string) {
        this.string = string;
    }

    @Override
    protected InputStream openSource() throws IOException {
        return  new ByteArrayInputStream(string.getBytes());
    }
}
