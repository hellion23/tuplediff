package com.hellion23.tuplediff.api.db;

import com.hellion23.tuplediff.api.*;
import com.hellion23.tuplediff.api.monitor.Monitor;

import java.sql.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

/**
 * Created by margaret on 9/30/2014.
 */
public class SqlTupleStream implements TupleStream {

    private static final Logger logger = Logger.getLogger(TupleComparison.class.getName());
    String name;
    Monitor monitor;
    boolean stopped = false;
    Connection connection;
    String baseSql;
    String runSql;
    PreparedStatement stmt;
    ResultSet rs;
    SqlSchema sqlSchema;
    TupleStreamKey tupleStreamKey;
    int bufferSize = 50;
    LinkedBlockingQueue <Tuple> buffer = new LinkedBlockingQueue<Tuple>();
    boolean initialized = false;

    public static SqlTupleStream create (Connection connection) {
        try {
            SqlTupleStream ts = null;
            switch (connection.getMetaData().getDatabaseProductName()) {
                case "Oracle":
                    ts = new OracleSqlTupleStream(connection);
                    break;
                case "Microsoft SQL Server":
                    ts = new SQLServerTupleStream(connection);
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

    protected void init() {
        if(initialized) return;
        try {
//            monitor.handleEvent(this, STATE.STARTING, "INIT_START");
            if (sqlSchema == null) {
                sqlSchema = createSchemaFor();
            }
            runSql = initQuerySql();
            stmt = connection.prepareStatement(runSql);
            initialized = true;
//            monitor.handleEvent(this, STATE.RUNNING, "INIT_END");
        }
        catch (SQLException e) {
            throw new TupleDiffException("Encountered error initializing SqlTupleStream "
                    + this.getName() + ": " + e.getMessage(),
                    this, e);
//            monitor.handleEvent(this, STATE.TERMINATED, STOP_REASON.FAILED, e);
        }
    }

    protected SqlSchema createSchemaFor () {
        assert (baseSql != null);
        assert (tupleStreamKey != null);

        SqlSchema schema = null;
        try {
            final PreparedStatement stmt = connection.prepareStatement(baseSql);
            final ResultSetMetaData rsmd = stmt.getMetaData();
            List<SqlField> allFields = new ArrayList<SqlField>();
            for (int i=1; i<=rsmd.getColumnCount(); i++) {
                SqlField field = new SqlField(
                        extractColumnName(rsmd.getColumnName(i)),
                        rsmd.getColumnType(i),
                        rsmd.getColumnClassName(i),
                        rsmd.getColumnName(i),
                        i
                );
                allFields.add(field);
            }

            //Identify the primary key fields:
            List<SqlField> keyFields = new LinkedList<SqlField>();
            for (Field k : tupleStreamKey.getFields()) {
                boolean found = false;
                String lookFor = extractColumnName(k.getExpression());
                for (SqlField af: allFields) {
                    if (lookFor.equals(af.getName())) {
                        found = true;
                        keyFields.add(af);
                        break;
                    }
                }
                if (!found) {
                    throw new TupleDiffException(" Could not find Tuple Key " + lookFor + " amongst these fields: " +
                            allFields, this);
                }
            }

            schema = new SqlSchema(tupleStreamKey, allFields, keyFields);
            schema.setVendor(connection.getMetaData().getDatabaseProductName());
            schema.setVersion(connection.getMetaData().getDatabaseProductVersion());
        }
        catch (SQLException se) {
            throw new TupleDiffException ("Could not create Schema for " + getName() + ": " + se.getMessage(),
                    this, se);
        }
        return schema ;
    }

    protected static String extractColumnName (String string) {
        int i = string.indexOf('.');
        if (i < 0) {
            return string.toUpperCase();
        }
        else {
            return string.substring(i+1).toUpperCase();
        }
    }

    protected String initQuerySql () {
        StringBuilder orderBy = new StringBuilder("\norder by ");
        constructOrderByClause(orderBy, sqlSchema.getKeyFields());
        return baseSql + orderBy ;
    }

    /**
     * Should be overridden by specific db imlementations.
     * @param sb
     * @param keyFields
     */
    protected void constructOrderByClause (StringBuilder sb, List<SqlField> keyFields) {
        for (int i=0; i< keyFields.size(); i++) {
            sb.append(keyFields.get(i).getColumnName()).append(" asc");
            if (i<keyFields.size() - 1) sb.append (", ");
        }
    }

    @Override
    public void open() {
        try {
            init ();
//            monitor.handleEvent(this, STATE.RUNNING, "QUERY_START");
            rs = stmt.executeQuery();
            buffer(bufferSize);
//            monitor.handleEvent(this, STATE.RUNNING, "QUERY_END");
        }
        catch (Exception e) {
            stopped = true;
            throw new TupleDiffException("Encountered error running SqlTupleStream query: "
                    + this.getName() + ": " + e.getMessage(),
                    this, e);
//            monitor.handleEvent(this, STATE.TERMINATED, STOP_REASON.FAILED, e);
        }
    }

    protected int buffer(int num) throws SQLException {
        assert (num>0);
        for (int i=0; i<num; i++) {
            if (rs.next()) {
                Tuple tuple = createTuple(rs);
                buffer.add(tuple);
            }
            else {
                cleanup();
                return i;
            }
        }
        return num;
    }


    @Override
    public boolean hasNext() {
        if (!buffer.isEmpty()) {
            return true;
        }
        else {
            if (stopped)
                return false;
            else {
                // Get some more:
                try {
                    int available = buffer(bufferSize);
                    return available > 0;
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }

            }
        }
    }

    @Override
    public Tuple getNext() {
        if (!hasNext()) {
            throw new TupleDiffException("No more Tuples in this SqlTupleStream <"
                    + this.getName()  + ">",  this);
        }
        return buffer.poll();
    }

    protected Tuple createTuple (ResultSet rs) throws SQLException {
        Map<SqlField, Comparable> row = new HashMap<SqlField, Comparable>();

        for(SqlField field : sqlSchema.getAllFields()) {
            row.put(field, extractComparable(field, rs));
        }

        Tuple tuple = new Tuple(sqlSchema, row);
        return tuple;
    }

    protected Comparable extractComparable (SqlField field, ResultSet rs) throws SQLException, TupleDiffException {
            Object o = rs.getObject(field.getColumnIndex());
            if (o == null || o instanceof Comparable) {
                return (Comparable) o;
            }
            else if (o instanceof java.sql.Timestamp) {
                return rs.getTimestamp(field.getColumnIndex());
            }
            else {
                throw new TupleDiffException ("Field " + field.getName() + ", Class=" + o.getClass() +
                        " is not a Comparable object. Exclude this field from query. ", this);
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
    public TupleStreamKey getTupleStreamKey() {
        return tupleStreamKey;
    }

    @Override
    public void setTupleStreamKey(TupleStreamKey key) {
        this.tupleStreamKey = key;
    }

    @Override
    public Schema getSchema() {
        if (sqlSchema == null) {
            sqlSchema = createSchemaFor();
        }
        return sqlSchema;
    }

    @Override
    public void setSchema(Schema schema) {
        this.sqlSchema = (SqlSchema) schema;
    }

    @Override
    public void close() {
        stopped = true;
        cleanup ();
    }

    protected void cleanup () {
        try {
            if (!connection.isClosed())
                connection.close();
        }
        catch (SQLException se) {
            logger.severe(name + " error closing DB connection.");
        }
    }

    public String getSql() {
        return baseSql;
    }

    public void setSql(String sql) {
        this.baseSql = sql;
    }

    protected static class OracleSqlTupleStream extends SqlTupleStream {
        public OracleSqlTupleStream (Connection connection) {
            super(connection);
        }

        @Override
        protected void constructOrderByClause (StringBuilder sb, List<SqlField> keyFields) {
            for (int i=0; i<keyFields.size(); i++) {
                if (java.lang.String.class.equals(keyFields.get(i).getFieldClass())) {
                    sb.append("NLSSORT (").append(keyFields.get(i).getName()).append(", 'NLS_SORT = BINARY') asc ");
                }
                else {
                    sb.append(keyFields.get(i).getName()).append(" asc");
                }
                if (i<keyFields.size()-1) sb.append(", ");
            }
            sb.append (" nulls first ");
        }

        protected Comparable extractComparable (SqlField field, ResultSet rs) throws SQLException, TupleDiffException {
            Object o = rs.getObject(field.getColumnIndex());
//            TODO: Fix this. Although this works for the purposes of sorting and comparing across Oracle queries, will not work when comparing vs another DB. Also could matter to downstream systems expecting an actual Date object.
            if (o.getClass().getName().equals( "oracle.sql.TIMESTAMP")) {
                return rs.getString(field.getColumnIndex());
//                return rs.getTimestamp (field.getColumnIndex()+1);
            }
            else {
                return super.extractComparable(field, rs);
            }
        }
    }

    protected static class SQLServerTupleStream extends SqlTupleStream {
        public SQLServerTupleStream(Connection connection) {
            super(connection);
        }

        @Override
        protected void constructOrderByClause (StringBuilder sb, List<SqlField> keyFields) {
            for (int i=0; i<keyFields.size(); i++) {
                if (java.lang.String.class.equals(keyFields.get(i).getFieldClass())) {
                    sb.append(keyFields.get(i).getName()).append(" collate Latin1_General_BIN asc ");
                }
                else {
                    sb.append(keyFields.get(i).getName()).append(" asc");
                }
                if (i<keyFields.size()-1) sb.append(", ");
            }
        }
    }
}
