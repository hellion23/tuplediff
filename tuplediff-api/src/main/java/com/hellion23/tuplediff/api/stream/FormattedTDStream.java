package com.hellion23.tuplediff.api.stream;

import com.hellion23.tuplediff.api.format.TypeFormatter;
import com.hellion23.tuplediff.api.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/**
 *
 * Created by hleung on 5/24/2017.
 */
public class FormattedTDStream extends WrappedTDStream <TDStreamMetaData, TDTuple> {
    private final static Logger LOG = LoggerFactory.getLogger(FormattedTDStream.class);
    Map<TDColumn, ? extends TypeFormatter> overrides;
    TDStreamMetaData metaData = null;
    List<TypeFormatter> formatters;

    public FormattedTDStream (TDStream stream, Map<TDColumn, ? extends TypeFormatter> overrides) throws TDException {
        super(stream);
        this.overrides = overrides;

        // Because this TDStream wraps the underlying Stream and then formats the column to something else,
        // We need to change the meta data's TDColumn information to reflect the new types.
        List<TDColumn> columns = new ArrayList<>();
        formatters = new ArrayList<>();
        getWrapped().getMetaData().getColumns().stream().forEach(c -> {
            TDColumn col = (TDColumn) c;
            final TypeFormatter tf = overrides.get(col);
            TDColumn fmtColumn = tf == null ? col : new TDColumn(col.getLabel(), tf.getType(), true);
            columns.add(fmtColumn);
            formatters.add(overrides.get(col));
        });
        this.metaData = new TDStreamMetaData(columns);
    }

    @Override
    public TDStreamMetaData getMetaData() {
        return this.metaData;
    }

    @Override
    public TDTuple next() {
        TDTuple next = super.next();
        return format(next, this.formatters, this.metaData);
    }

    /**
     * Formats a tuple with provided formatters. Also accepts the new TDTuple's formatted Meta Data in order to create
     * the formatted tuple.
     *
     * @param tuple
     * @param formatters
     * @param formattedMetaData
     * @return
     */
    public static TDTuple format (TDTuple tuple, List<TypeFormatter> formatters, TDStreamMetaData formattedMetaData) {
        List<Object> values = new ArrayList<>(formatters.size());
        IntStream.range(0, formatters.size()).forEach(i -> {
            TypeFormatter tf = formatters.get(i);
            if (tf == null) {
                values.add(tuple.getValue(i));
            }
            else {
                Object formatted = null;
                Object value = tuple.getValue(i);

                try {
                    formatted = tf.apply(value);
                }
                catch (Exception ex) {
                    LOG.error(String.format("Could not format column <%s>, value<%s>, using formatter <%s>",
                            tuple.getColumns().get(i), tuple.getValue(i), tf), ex);
                    throw new TDException(String.format("Could not format column <%s>, value<%s>, using formatter <%s>",
                            tuple.getColumns().get(i), tuple.getValue(i), tf));
                }
                values.add(formatted);
            }
        });
        return new TDTuple(formattedMetaData, values);
    }

    public Map<TDColumn, TypeFormatter> getColumnFormatters () {
        Map<TDColumn, TypeFormatter> columnFormatters = new LinkedHashMap<>();
        IntStream.range(0, formatters.size()).forEach(i -> columnFormatters.put(metaData.getColumn(i), formatters.get(i)));
        return columnFormatters;
    }

    @Override
    public String getName () {
        return "FORMATTED "+super.getName();
    }

    @Override
    public String toString() {
        return "FormattedTDStream{" +
                "metaData=" + metaData +
                ", formatters=" + formatters +
                "} " + super.toString();
    }
}
