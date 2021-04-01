package com.hellion23.tuplediff.api;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by hleung on 5/25/2017.
 */
public class TDUtils {

   /**
     * Reverse changes from the "guessing of camelcasing"
     *
     * @param columnName
     * @return
     */
    public static String normalizeColumnName(String columnName) {
                return columnName.toUpperCase().replaceAll("[^a-zA-Z0-9]", "");
//        return columnName.toUpperCase().replaceAll("[^a-zA-Z0-9_]", "");
    }

    public static List<String> normalizedColumnNames (List<String> columnNames) {
        if (columnNames == null) {
            return null;
        }
        else if (columnNames.size() == 0) {
            return Collections.emptyList();
        }
        else {
            return columnNames.stream().map(TDUtils::normalizeColumnName).collect(Collectors.toList());
        }
    }

}
