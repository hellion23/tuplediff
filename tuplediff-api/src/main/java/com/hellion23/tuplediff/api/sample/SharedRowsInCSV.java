package com.hellion23.tuplediff.api.sample;

import com.hellion23.tuplediff.api.TupleDiff;
import com.hellion23.tuplediff.api.compare.CompareEvent;
import com.hellion23.tuplediff.api.config.StreamConfig;
import com.hellion23.tuplediff.api.config.TDConfig;
import com.hellion23.tuplediff.api.model.TDTuple;

import java.util.LinkedList;
import java.util.List;

/**
 * Find all common rows between 2 csv files.
 */
public class SharedRowsInCSV {
    public static void main (String args[]) {
        String file1 = "C:\\myFile.csv";
        String file2 = "C:\\myFile2.csv";
        String headers1 [] = {}; // read headers from CSV apache
        String headers2 [] = {}; // read headers from CSV apache
        // if headers1 and headers2 don't have all the same columns, then automatically fail. no rows should be returned.

        // if all headers matched then use tuplediff to do comparisons:
        TDConfig config = TDConfig.builder()
                .right(StreamConfig.csvFile(file1))
                .left(StreamConfig.csvFile(file2))
                .primaryKey(headers1)
                .build();

        List <TDTuple> commonRows = new LinkedList <>();
        TupleDiff.configure(config, (compareEvent) -> {
            if (compareEvent.getType() == CompareEvent.TYPE.FOUND_MATCHED) {
                commonRows.add(compareEvent.getLeft());
            }
        }).compare(); // Run the comparison.

        // Do something w/ the commonRows
        for (TDTuple row : commonRows) {

        }
    }

}
