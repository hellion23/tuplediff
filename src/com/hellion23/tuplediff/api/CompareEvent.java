package com.hellion23.tuplediff.api;

import java.util.List;

/**
 * Created by Hermann on 9/28/2014.
 */
public class CompareEvent {
    public enum TYPE {
        LEFT_BREAK, RIGHT_BREAK, PAIR_BREAK, PAIR_MATCHED, DATA_LEFT, DATA_RIGHT
    }

    TYPE type;
    Tuple leftTuple;
    Tuple rightTuple;
    protected List<String> breakFields;

    public CompareEvent(TYPE type, Tuple leftTuple, Tuple rightTuple, List<String> breakFields) {
        this.type = type;
        this.leftTuple = leftTuple;
        this.rightTuple = rightTuple;
        this.breakFields = breakFields;
    }
}
