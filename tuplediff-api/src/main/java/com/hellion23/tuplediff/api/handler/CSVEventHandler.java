package com.hellion23.tuplediff.api.handler;

import com.hellion23.tuplediff.api.TDCompare;
import com.hellion23.tuplediff.api.model.TDException;
import com.hellion23.tuplediff.api.model.TDStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by hleung on 5/29/2017.
 */
@Slf4j
public class CSVEventHandler  extends AbstractFlatMapEventHandler {
    String filePath;
    CSVPrinter printer;
    Appendable out;

    public CSVEventHandler (String filePath)  {
        this.filePath = filePath;
    }

    public CSVEventHandler (Appendable out) {
       this.out = out;
    }

    @Override
    public void init(TDCompare comparison) {
        super.init(comparison);
        try {
            if (out == null) {
                out = new FileWriter(filePath);
            }
            printer = new CSVPrinter(out, CSVFormat.EXCEL);
            TDStream lStream = comparison.getLeft();
            TDStream rStream = comparison.getRight();
            String lPrefix = lStream.getName() == null || "LEFT".equals(lStream.getName())? "L_" : lStream.getName() + "_";
            String rPrefix = rStream.getName() == null || "RIGHT".equals(rStream.getName())? "R_" : rStream.getName() + "_";

            for (String each : getColumnNames()) {
                if (compareColumns.contains(each)) {
                    String leftLabel = lStream.getMetaData().getColumn(each).getLabel();
                    printer.print(lPrefix+leftLabel);
                    String rightLabel = rStream.getMetaData().getColumn(each).getLabel();
                    printer.print(rPrefix+rightLabel);
                }
                else if (primaryKeys.contains(each)) {
                    String label = Optional.ofNullable(lStream).orElse(rStream).getMetaData().getColumn(each).getLabel();
                    printer.print(label);
                }
                else {
                    printer.print(each);
                }
            }
            printer.println();
        } catch (IOException e) {
            throw new TDException("Could not open file " + filePath + ": " + e.getMessage(), e);
        }
    }

    @Override
    void accept(LinkedHashMap<String, Object> breakRow) {
        try {
            for (Object each : breakRow.values()) {
                if (each instanceof Object[] ) {
                    Object arr[] = (Object[]) each;
                    printer.print(arr[0]);
                    printer.print(arr[1]);
                }
                else {
                    printer.print(format(each));
                }
                printer.flush();
            }
            printer.println();
            printer.flush();
        } catch (Exception e) {
            throw new TDException("Error Writing compare Event to CSV file: " + filePath + ": " + e.getMessage() + " \n break: " + breakRow, e);
        }
    }

    private String format(Object o) {
        if (o == null)
            return "";
        else if (o instanceof Collection) {
            // delimit with something other than a comma, which is what would happen if a Collection is toString'ed.
            // CSVPrinter will correctly escape commas, so this is just a convenience.
            Stream<String> z = ((Collection)o).stream().map(x -> x.toString());
            return z.collect(Collectors.joining("|"));
        }
        else if (o instanceof LocalDateTime) {
            return DateTimeFormatter.ISO_LOCAL_DATE_TIME.format((LocalDateTime)o);
        }
        else {
            return o.toString();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        printer.flush();
        printer.close();
        log.info("TDCompare CSV file written to... " + filePath);
    }
}
