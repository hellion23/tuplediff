package com.hellion23.tuplediff.api.stream;

import com.hellion23.tuplediff.api.model.TDColumn;
import com.hellion23.tuplediff.api.model.TDStreamMetaData;
import com.hellion23.tuplediff.api.model.TDTuple;
import com.hellion23.tuplediff.api.stream.source.FileStreamSource;
import com.hellion23.tuplediff.api.stream.source.StreamSource;
import org.apache.commons.collections4.iterators.PeekingIterator;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Stream of CSV rows.
 */
public class CSVTDStream extends AbstractBufferingTDStream <TDStreamMetaData, TDTuple, CSVRecord> {
    private final static Logger LOG = LoggerFactory.getLogger(CSVTDStream.class);
    StreamSource streamSource;
    CSVParser csvParser = null;
    Iterator<CSVRecord> iterator = null;
    CSVRecord headers = null;

    public CSVTDStream(String name, String filePath) {
        this (name, new FileStreamSource(filePath));
    }

    public CSVTDStream(String name, StreamSource streamSource) {
        super(name, 1);
        this.streamSource = streamSource;
    }

    @Override
    protected void openStream() throws Exception {
        streamSource.open();
        csvParser = CSVFormat.EXCEL.parse(streamSource.getReader());
        iterator = new SkipEmptyLinesIterator(csvParser.iterator()); // csvParser.iterator();
        headers = iterator.next();
    }

    @Override
    protected TDStreamMetaData constructMetaData() {
        ArrayList <TDColumn> columns = new ArrayList<>(headers.size());
        headers.forEach( h -> columns.add(new TDColumn(h, String.class, false )));
        return new TDStreamMetaData (columns);
    }

    @Override
    protected TDTuple constructTuple(CSVRecord record) {
        List<String> values = new LinkedList<>();
        IntStream.range(0, record.size()).forEach(x -> values.add(record.get(x)));
        TDTuple tuple = new TDTuple(metadata, values);
        return tuple;
    }

    @Override
    protected Iterator<CSVRecord> iterator() {
        return this.iterator;
    }

    @Override
    protected void closeStream() throws Exception {
        csvParser.close();
    }

    @Override
    public String toString() {
        return "CSVTDStream{" +
                "streamSource=" + streamSource +
                "} " + super.toString();
    }

    static class SkipEmptyLinesIterator implements Iterator<CSVRecord> {
        PeekingIterator<CSVRecord> iterator;

        public SkipEmptyLinesIterator(Iterator<CSVRecord> iterator) {
            this.iterator = new PeekingIterator<>(iterator);
        }

        @Override
        public boolean hasNext() {
            CSVRecord record = iterator.peek();
            if (record == null) {
                return false;
            }
            // validate the CSVRecord is a valid one (i.e. not an empty line. Otherwise skip it.
            while (record != null && record.size() == 1 && "".equals(record.get(0).trim())) {
                iterator.next();
                record = iterator.peek();
            }
            return record != null;
        }

        @Override
        public CSVRecord next() {
            return iterator.next();
        }
    }
}
