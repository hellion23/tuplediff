package com.hellion23.tuplediff.api.stream;

import com.hellion23.tuplediff.api.compare.PrimaryKeyTupleComparator;
import com.hellion23.tuplediff.api.model.TDException;
import com.hellion23.tuplediff.api.model.TDStream;
import com.hellion23.tuplediff.api.model.TDStreamMetaData;
import com.hellion23.tuplediff.api.model.TDTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Guarantees that the TDTuples emitted by this Stream will be the same as the wrapped stream, but sorted in
 * primaryKey order. The sorting will be done in memory; when open() is called, it will consume all the TDTuples
 * from the wrapped Stream and sort the data into a new List.
 *
 * Created by hleung on 6/8/2017.
 */
public class InMemorySortedTDStream  <M extends TDStreamMetaData, T extends TDTuple> extends WrappedTDStream <M ,T> {
    private final static Logger LOG = LoggerFactory.getLogger(InMemorySortedTDStream.class);
    List<String> primaryKey;
    List <T> sortedTuples = null;
    PrimaryKeyTupleComparator <T> pkComparator;

    public InMemorySortedTDStream (List<String> primaryKey, PrimaryKeyTupleComparator <T>pkComparator, TDStream <M, T> stream) {
        super (stream);
        this.primaryKey = primaryKey;
        this.pkComparator = pkComparator;
    }

    @Override
    public void open() throws TDException {
        // Open the underlying stream.
        LOG.info("Opening underlying unsorted stream: " + getWrapped().getName());
        super.open();
//        getWrapped().open();
        sortedTuples = new ArrayList<>();
        while (super.hasNext()) {
            sortedTuples.add(super.next());
        }
        LOG.info("Sorting tuples from stream: " + getWrapped().getName());
        Collections.sort(sortedTuples, pkComparator);

        LOG.info("Tuples sorted & ready for reading from stream: " + getWrapped().getName());
        try {
            super.close();
        } catch (Exception e) {
            LOG.error ("Could not close underlying stream " + getWrapped().getName(), e);
        }
    }

    @Override
    public boolean hasNext() {
        return isOpen () && sortedTuples.size() > 0;
    }

    @Override
    public T next() {
        if (!isOpen()) {
            return null;
        }
        else {
            return sortedTuples.remove(0);
        }
    }

    @Override
    public boolean isOpen() {
        return sortedTuples != null;
    }

    @Override
    public String getName() {
        return "InMemorySortedOf{"+stream.getName()+"}";
    }

//    @Override
//    public List<String> getSortKey() {
//        return this.primaryKey;
//    }

    @Override
    public boolean isSorted () {
        return true;
    }

    @Override
    public void close() throws Exception {
        sortedTuples = null;
    }

    @Override
    public String toString() {
        return "InMemorySortedTDStream{" +
                "underlying=" + stream +"} ";
    }
}
