package com.hellion23.tuplediff.api.db;

import com.hellion23.tuplediff.api.*;
import com.hellion23.tuplediff.api.monitor.Monitor;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by margaret on 9/30/2014.
 */
public class SqlTupleStream implements TupleStream {
    String name;
    Monitor monitor;
    boolean stopped;
    Connection connection;
    String sql;
    SqlSchema schema;
    TupleKey tupleKey;
    Field [] primaryKey;

    public static SqlTupleStream create (Connection connection) {
        try {
            SqlTupleStream ts = null;
            switch (connection.getMetaData().getDatabaseProductName()) {
                case "Oracle":
                    break;
                case "Microsoft SQL Server":
                    break;
                default:
                    ts = new SqlTupleStream(connection);
                    break;
            }
            return ts;
        }
        catch (SQLException ex) {
            throw new RuntimeException ("Error creating SQLTupleStream");
        }
    }

    private SqlTupleStream (Connection connection) {
        this.connection = connection;

    }

    @Override
    public void init() {

    }

    @Override
    public void open() {

    }

    protected Tuple createTuple (ResultSet rs) throws SQLException {
        Map<Field, Comparable> row = new HashMap<Field, Comparable>();
        Tuple tuple = new Tuple(schema, row);
        tuple.setKey(new PrimaryKeyComparable(primaryKey, tuple));
        return tuple;
    }

    static class PrimaryKeyComparable implements Comparable <Comparable> {
        Comparable c[];

        public PrimaryKeyComparable (Field [] k, Tuple t) {
            c = new Comparable [k.length];
            for (int i = 0; i<c.length; i++) {
                c[i] = t.getValue(k[i]);
            }
        }

        @Override
        public int compareTo(Comparable o) {
            for (Comparable x : c) {

            }
            return 0;
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Monitor getMonitor() {
        return monitor;
    }

    public void setMonitor(Monitor monitor) {
        this.monitor = monitor;
    }

    @Override
    public void stop() {
        stopped = true;
    }

    @Override
    public boolean isStopped() {
        return stopped;
    }

    @Override
    public TupleKey getTupleKey() {
        return tupleKey;
    }

    @Override
    public void setTupleKey(TupleKey key) {
        this.tupleKey = key;
    }


    @Override
    public Schema getSchema() {
        return schema;
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Tuple getNext() {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public boolean isClosed() {
        return false;
    }
}
