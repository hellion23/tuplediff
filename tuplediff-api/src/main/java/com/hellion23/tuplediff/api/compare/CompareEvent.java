package com.hellion23.tuplediff.api.compare;

import com.hellion23.tuplediff.api.model.TDTuple;

import java.util.List;

/**
 * Created by hleung on 5/24/2017.
 */
public class CompareEvent {
    public enum TYPE {
        FOUND_ONLY_LEFT, FOUND_ONLY_RIGHT, // Data appears only on one side
        DATA_LEFT, DATA_RIGHT, // Incoming data on left or right data set
        FOUND_BREAK, // Data is paired (i.e. Primary key matches), but some data is unmatched
        FOUND_MATCHED, // Data is paired (i.e. Primary Key matches), and all data matches
    };

    TYPE type;
    TDTuple left;
    TDTuple right;
    List<String> fieldBreaks;

    public CompareEvent(TYPE type, TDTuple left, TDTuple right, List<String> fieldBreaks) {
        this.type = type;
        this.left = left;
        this.right = right;
        this.fieldBreaks = fieldBreaks;
    }

    public List<String> getFieldBreaks() {
        return fieldBreaks;
    }

    public TYPE getType() {
        return type;
    }

    public TDTuple getLeft() {
        return left;
    }

    public TDTuple getRight() {
        return right;
    }

    @Override
    public String toString() {
        return "CompareEvent{" +
                "type=" + type +
                ", left=" + left +
                "\n, right=" + right +
                "\n, fieldBreaks=" + fieldBreaks +
                '}';
    }
}
