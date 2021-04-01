package com.hellion23.tuplediff.api.president;


import com.hellion23.tuplediff.api.TupleDiff;
import com.hellion23.tuplediff.api.TupleDiffContext;
import com.hellion23.tuplediff.api.config.JSONStreamConfig;
import com.hellion23.tuplediff.api.model.TDStream;
import com.hellion23.tuplediff.api.model.TDTuple;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Create an Iterator over a TDStream. This sources the beans from a JSON file, however in reality, the Iterator
 * should be iterating over in-memory objects like List or Set or another Stream, not constructing the Bean from
 *
 */
public class PresidentIterator implements Iterator<President> {
    TDStream stream = null;
    JSONStreamConfig jsonStreamConfig = null;

    public PresidentIterator(JSONStreamConfig jsonStreamConfig) {
        this.jsonStreamConfig = jsonStreamConfig;
    }

    protected void init () {
        this.stream = TupleDiff.constructJSONStream(
                "SRC",
                jsonStreamConfig,
                Arrays.asList(PresidentTest.presidentPrimaryKey),
                new TupleDiffContext());
        stream.getMetaData(); // Force the stream to open and figure out metadata.
    }
    @Override
    public boolean hasNext() {
        if (this.stream == null) {
            init();
        }
        return this.stream.hasNext();
    }

    @Override
    public President next() {
        TDTuple tuple = this.stream.next();
        President president = new President();
        president.dob = asInt("DOB", tuple);
        president.firstName = (String) tuple.getValueForLabel("FIRST_NAME");
        president.lastName = (String) tuple.getValueForLabel("LAST_NAME");
        president.presidentId = asInt("PRESIDENT_ID", tuple);
        president.presidentNo = asInt("PRESIDENT_NO", tuple);

        return president;
    }

    private int asInt (String columnName, TDTuple tuple) {
        return ((BigDecimal)tuple.getValueForLabel(columnName)).intValue();
    }
}
