package com.hellion23.tuplediff.api;

import com.hellion23.tuplediff.api.monitor.Nameable;

/**
 * @author: Hermann Leung
 * Date: 11/25/2014
 */
public class TupleDiffException extends RuntimeException {
    Nameable source;
    Throwable cause;

    public TupleDiffException(String msg, Nameable source) {
        this (msg, source, null);
    }

    public TupleDiffException(String msg, Nameable source, Throwable cause) {
        super(msg, cause);
        this.source = source;
        this.cause = cause;
    }
    public Nameable getSource() {
        return source;
    }

    public Throwable getCause() {
        return cause;
    }
}
