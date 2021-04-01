package com.hellion23.tuplediff.api.stream.sql;

import com.hellion23.tuplediff.api.model.TDException;
import com.hellion23.tuplediff.api.model.TDStreamMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by hleung on 5/28/2017.
 */
public class SQLTDStreamMetaData extends TDStreamMetaData<SQLTDColumn> {
    enum DB_TYPE {UNKNOWN, ORACLE, SQL_SERVER}
    public interface Getter <O>{
        O get(ResultSet rs, Integer index) throws SQLException;
    }

    // Get String
    static Getter<String> STRING_GETTER = (rs, i) -> rs.getString(i);
    // Get as native Object.
    static Getter<Object> OBJECT_GETTER =  (rs, i) -> rs.getObject(i);
    // Get Boolean
    static Getter<Boolean> BOOLEAN_GETTER = (rs, i) -> rs.getBoolean(i);

    // Numeric Getters (will be transformed into BigDecimal if necessary (i.e. if the value to be compared is not of the same type)
    static Getter<Integer> INT_GETTER = (rs, i) -> rs.getInt(i);
    static Getter<Long> LONG_GETTER = (rs, i) -> rs.getLong(i);
    static Getter<Float> FLOAT_GETTER = (rs, i) -> rs.getFloat(i);
    static Getter<Double> DOUBLE_GETTER = (rs, i) -> rs.getDouble(i);
    static Getter<BigDecimal> BIGDECIMAL_GETTER = (rs, i) -> rs.getBigDecimal(i);

    /// Date Getters (will be converted to LocalDateTime if unmatched types.
    static Getter<Date> DATE_GETTER = (rs, i) -> rs.getDate(i);
    static Getter<Time> TIME_GETTER = (rs, i) -> rs.getTime(i);
    static Getter<Timestamp> TIMESTAMP_GETTER = (rs, i) -> rs.getTimestamp(i);


    SQLTDStreamMetaData.DB_TYPE dbType;
    String name;

    public SQLTDStreamMetaData (String name, DatabaseMetaData dbmd,  ResultSetMetaData rsmd) throws SQLException, TDException {
        super();
        this.name = name;

        List<SQLTDColumn> columns = new LinkedList<>();
        for (int i=0; i<rsmd.getColumnCount(); i++) {
            String colName = rsmd.getColumnName(i+1);
            String  clazz= rsmd.getColumnClassName(i+1);
            int sqlType = rsmd.getColumnType(i+1);
            try {
                SQLTDColumn column = createColumn(colName, clazz, sqlType);
                columns.add(column);
            } catch (ClassNotFoundException e) {
                throw new TDException(name + " STREAM: ClassNotFoundException for column name " + colName + " class = " + clazz);
            }
        }
        setColumns(columns);
        String dbProduct = dbmd.getDatabaseProductName();
        if ("Microsoft SQL Server".equals(dbProduct)) {
            dbType = SQLTDStreamMetaData.DB_TYPE.SQL_SERVER;
        }
        else if ("Oracle".equals (dbProduct)) {
            dbType = SQLTDStreamMetaData.DB_TYPE.ORACLE;
        }
        else {
            dbType =  SQLTDStreamMetaData.DB_TYPE.UNKNOWN;
        }
    }

    private SQLTDColumn createColumn(String colName, String clazz, int sqlType) throws ClassNotFoundException {
        SQLTDColumn column = null;
        switch (sqlType) {
            case Types.NUMERIC:
                column = new SQLTDColumn(colName, BigDecimal.class, sqlType, BIGDECIMAL_GETTER);
                break;
            case Types.DOUBLE:
                column = new SQLTDColumn(colName, Double.class, sqlType, DOUBLE_GETTER);
                break;
            case Types.FLOAT:
                column = new SQLTDColumn(colName, Float.class, sqlType, FLOAT_GETTER);
                break;
            case Types.INTEGER:
                column = new SQLTDColumn(colName, Integer.class, sqlType, INT_GETTER);
                break;
            case Types.BIGINT:
                column = new SQLTDColumn(colName, Long.class, sqlType, LONG_GETTER);
                break;
            case Types.BIT:
            case Types.BOOLEAN:
                column = new SQLTDColumn(colName, Boolean.class, sqlType, BOOLEAN_GETTER);
                break;
            case Types.VARCHAR:
            case Types.CHAR:
                column = new SQLTDColumn(colName, String.class, sqlType, STRING_GETTER);
                break;
            case Types.DATE:
                column = new SQLTDColumn(colName, Date.class, sqlType, DATE_GETTER);
                break;
            case Types.TIME:
                column = new SQLTDColumn(colName, Time.class, sqlType, TIME_GETTER);
                break;
            case Types.TIMESTAMP:
                column = new SQLTDColumn(colName, Timestamp.class, sqlType, TIMESTAMP_GETTER);
                break;
            default:
                // Oracle weird date formatting:
                if ("oracle.sql.TIMESTAMP".equals(clazz)) {
                    column = new SQLTDColumn(colName, Timestamp.class, sqlType, TIMESTAMP_GETTER);
                }
                else if ("oracle.sql.DATE".equals(clazz)) {
                    column = new SQLTDColumn(colName, Date.class, sqlType, DATE_GETTER);
                }
                else if ("oracle.sql.TIME".equals(clazz)) {
                    column = new SQLTDColumn(colName, Time.class, sqlType, TIME_GETTER);
                }
                else {
                    column = new SQLTDColumn(colName, Class.forName(clazz), sqlType, OBJECT_GETTER);
                }
                break;
        }
        return column;
    }

    public DB_TYPE getDbType () {
        return dbType;
    }
}
