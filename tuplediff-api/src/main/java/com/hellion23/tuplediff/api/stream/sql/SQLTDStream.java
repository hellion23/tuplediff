package com.hellion23.tuplediff.api.stream.sql;

import com.hellion23.tuplediff.api.model.TDException;
import com.hellion23.tuplediff.api.model.TDStream;
import com.hellion23.tuplediff.api.model.TDTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Created by hleung on 5/27/2017.
 */
public class SQLTDStream implements TDStream<SQLTDStreamMetaData, TDTuple> {
    private final static Logger LOG = LoggerFactory.getLogger(SQLTDStream.class);

    //////////////////////////////////
    // Parameter configuration variables
    //////////////////////////////////
    String sql;
    String name;
    DataSource dataSource;

    //////////////////////////////////
    // Runtime variables
    //////////////////////////////////

    protected SQLTDStreamMetaData metaData = null;
    volatile boolean opened = false;
    volatile boolean closed = false;
    Connection conn = null;
    ResultSet resultSet = null;
    boolean didNext = false;
    boolean hasNext = false;
    TDTuple tuple = null;

    public SQLTDStream (String name, Supplier<DataSource> dataSource, String sql) {
        this.sql = sql;
        this.dataSource = dataSource.get();
        // Should give a name, if not will generate a dummy one.
        this.name = name != null ? name : dataSource.toString() + System.currentTimeMillis();
    }

    @Override
    public SQLTDStreamMetaData getMetaData() {
        if (metaData == null) {
            constructMetaData();
        }
        return metaData;
    }

    private void constructMetaData() {
        try (
            Connection conn = dataSource.getConnection();
            PreparedStatement ps = conn.prepareStatement(sql)
        )
        {
            metaData = new SQLTDStreamMetaData(name, conn.getMetaData(), ps.getMetaData());
        } catch (SQLException e) {
            LOG.error(name + " STREAM- Could not create metadata: ", e);
            throw new TDException(name + " STREAM- Could not create metadata: " + e.getMessage() + " SQL: " + sql);
        }
    }

    @Override
    public void open() throws TDException {
        if (opened || closed) {
            throw new TDException(name + " STREAM: attempting to re-open a closed (or already open stream");
        }
        String sql = getExecuteSql();
        try {
            LOG.info(name + " STREAM: Executing query...");
            opened = true;
            conn = dataSource.getConnection();
            // getExecuteSql case another class intends to override the default executeSql
            PreparedStatement ps = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            resultSet = ps.executeQuery();

        }
        catch (SQLException e){
            LOG.error(name + " STREAM- Could not create open stream: ", e);
            closed = true;
            throw new TDException(name + " STREAM- Could not run query: " + e.getMessage() + " QUERY: " + sql);
        }
    }

    @Override
    public TDTuple next(){
        if (!isOpen()) {
            throw new TDException("This stream has not been opened. Call open() first. ");
        }
        try {
            if (!didNext) {
                fetchNext();
            }
            didNext = false;
            return tuple;
        }
        catch (SQLException e) {
            LOG.error(name + " STREAM error w/ getting next() ", e);
            throw new TDException (name + " STREAM Could not retrieve the next ResultSet: "+e.getMessage());
        }
    }

    @Override
    public boolean hasNext(){
        if (this.closed) {
            return false;
        }
        else if (!isOpen())
            throw new TDException("This stream has not been opened. Call open() first. ");
        else if (!didNext) {
            try {
                fetchNext();
                didNext = true;
            } catch (Exception e) {
                LOG.error(name + " STREAM error w/ hasNext() ", e);
            }
        }
        return hasNext;
    }

    private void fetchNext () throws SQLException {
        hasNext = resultSet.next();
        tuple = hasNext ? createTuple() : null;
    }


    private TDTuple createTuple() throws SQLException {
        List<Object> values = new LinkedList<>();
        for (int i=0; i<metaData.getColumnCount(); i++) {
            values.add(metaData.getColumn(i).getter().get(resultSet, i+1));
        }
        TDTuple tuple = new TDTuple(metaData, values);
        return tuple;
    }

    @Override
    public boolean isOpen() {
        return !closed && opened;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void close() throws Exception {
        if (this.closed)
            return;
        this.closed = true;
        if (resultSet != null) {
            resultSet.close();
            resultSet = null;
        }
        if (conn != null) {
            conn.close();
            conn = null;
        }
    }

    public String getSql() {
        return sql;
    }

    /**
     * Wraps the entire select query into it's own subquery, so that aliased columns can be referenced.
     * Also wraps starting from the markers --SQL_START and --SQL_END if they exist. This allows for declare statements
     * that aren't part of the select statement proper.
     * be referenced.
     * @return
     */
    public static final String SQL_START = "--SQL_START";
    public static final String SQL_END = "--SQL_END";

    protected String getExecuteSql () {
        int sqlStart = sql.indexOf(SQL_START);
        int sqlEnd = sql.indexOf(SQL_END);
        if (sqlStart >=0 && sqlEnd >=0) {
            return sql.replaceAll(SQL_START, "select * from (").replaceAll(SQL_END, ") a");
        }
        else
            return "select * from (" + sql + ") a";
    }
}
