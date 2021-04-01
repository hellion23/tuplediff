package com.hellion23.tuplediff.api.model;

/**
 * TDException is Runtime because lambdas suck w/ checked Exceptions.
 *
 * Created by hleung on 5/23/2017.
 */
public class TDException extends RuntimeException {
    public TDException (String msg, Throwable ex) {
        super (msg, ex);
    }

    public TDException (String msg) {
        super (msg);
    }
}
