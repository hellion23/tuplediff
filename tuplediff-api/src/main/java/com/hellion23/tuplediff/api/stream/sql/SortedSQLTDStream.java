package com.hellion23.tuplediff.api.stream.sql;

import com.hellion23.tuplediff.api.model.TDException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static java.util.stream.Collectors.joining;

/**
 * Created by hleung on 5/28/2017.
 */

public class SortedSQLTDStream extends SQLTDStream {
    private final static Logger LOG = LoggerFactory.getLogger(SortedSQLTDStream.class);
    List<String> orderBy;
    String executeSql;

    public SortedSQLTDStream(String name, Supplier<DataSource> dataSource, String sql, List<String> orderBy) {
        super(name, dataSource, sql);
        this.orderBy = orderBy;
    }

    @Override
    protected String getExecuteSql () {
        if (executeSql == null) {
            constructExecuteSql();
        }
        return executeSql;
    }

    private void constructExecuteSql() {
        // ensure meta data has been instantiated.
        getMetaData();
        // Validate that the primary keys are present in the underlying SQL
        StringBuilder orderBySql = new StringBuilder(super.getExecuteSql()).append("\n order by ");
        switch (metaData.getDbType()) {
            case ORACLE:
                orderBySql.append(createOracleOrderedByClause ());
                break;
            case SQL_SERVER:
                orderBySql.append(createSqlServerOrderedByClause());
                break;
            case UNKNOWN:
            default:
                orderBySql.append(createUnknownDBOrderedByClause());
        }
        this.executeSql = orderBySql.toString();
    }

    protected String createOracleOrderedByClause () {
        final SQLTDStreamMetaData metaData = getMetaData();
        return orderBy.stream().map(pk -> {
            if (isColumnAString(metaData, pk)) {
                return "NLSSORT (" + pk + ", 'NLS_SORT = BINARY') asc ";
            } else {
                return pk + " asc";
            }
        }).collect(joining(", "));

    }

    protected String createSqlServerOrderedByClause () {
        final SQLTDStreamMetaData metaData = getMetaData();
        return orderBy.stream().map(pk -> {
            if (isColumnAString(metaData, pk)) {
                return pk + " collate Latin1_General_BIN asc ";
            } else {
                return pk + " asc";
            }
        }).collect(joining(", "));
    }

    protected boolean isColumnAString(SQLTDStreamMetaData metaData, String colName) {
        SQLTDColumn column = Optional
                .ofNullable(metaData.getColumnByLabel(colName))
                .orElse(metaData.getColumn(colName));
        // Cannot
        if (column == null) {
            throw new TDException(name + " Stream error: Cannot order by " + colName +
                    " as it does not exist as a column. All columns: " + metaData.getColumnLabels());
        }
        return String.class == column.getColumnClass();
    }

    protected String createUnknownDBOrderedByClause () {
        return orderBy.stream().map (pk -> pk + " asc").collect(joining(", "));
    }

    @Override
    public boolean isSorted () {
        return true;
    }
//    @Override
//    public List<String> getSortKey () {
//        // The Sort Key is the normalized variable name, NOT the original primary key name.
//        return TDUtils.normalizedColumnNames(this.orderBy);
////        return this.orderBy;
//    }

}
