package com.hellion23.tuplediff.api.handler;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Drains break row HashMaps into provided Collection. If none supplied, will create a new one.
 *
* Created by hleung on 7/29/2017.
*/
public class CollectionDrainEventHandler extends AbstractFlatMapEventHandler {

    private final CopyOnWriteArrayList<LinkedHashMap<String, Object>> drainTo;

    public CollectionDrainEventHandler () {
        this(null);
    }

    public CollectionDrainEventHandler (CopyOnWriteArrayList<LinkedHashMap<String, Object>> drainTo) {
        this.drainTo = drainTo == null ? new CopyOnWriteArrayList<>() : drainTo;
    }

    /**
     * The mappings are:
     * EVENT: FOUND_ONLY_LEFT, FOUND_ONLY_RIGHT, BREAK
     * BREAK_SUMMARY: List<String> </String> break fields (List of fields that don't match). Only populated if event is BREAK.
     * PRIMARY KEYS COLUMNS: 1 value for each primary key, in order of Primary Key index.
     * COMPARE COLUMNS: 2 values for each compare column. Object[0] = Left Tuple Value, Object[1] = Right Tuple value.
     *
     * @param breakRow
     */
    @Override
    void accept(LinkedHashMap<String, Object> breakRow) {
        this.drainTo.add(breakRow);
    }

    public Collection<LinkedHashMap<String, Object>> getBreaks() {
        return drainTo;
    }
}
