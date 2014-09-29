package com.hellion23.tuplediff.api;

import java.util.List;

/**
 * Created by Hermann on 9/28/2014.
 */
public class TupleBreak {
    public enum TYPE {
        LEFT, RIGHT, PAIR
    }
    TYPE type;
    Tuple leftTuple;
    Tuple rightTuple;
    protected List<String> fields;
}
