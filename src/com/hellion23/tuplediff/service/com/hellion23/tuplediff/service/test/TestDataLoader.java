package com.hellion23.tuplediff.service.com.hellion23.tuplediff.service.test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.*;
import java.text.Format;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * @author: Hermann Leung
 * Date: 1/23/2015
 */
public class TestDataLoader {
    private static final Logger logger = Logger.getLogger(TestDataLoader.class.getName());
    final static String COMMENT = "--";
    String dbName = "TUPLEDIFF_TEST";
    String file;
    JavaDB.EmbeddedDB embeddedDB;
    final static String TABLE_DEFINITION_BEGINS = "TABLE_DEFINITION_BEGINS:";
    final static String TABLE_NAME = "TABLE_NAME";
    final static String COLUMN_NAMES = "COLUMN_NAMES:";
    final static String COLUMN_TYPES = "COLUMN_TYPES:";
    static final String PRIMARY_KEYS =  "PRIMARY_KEYS:";
    final static String TABLE_DEFINITION_ENDS = "TABLE_DEFINITION_ENDS";
    static final SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");

    public TestDataLoader(String file) throws Exception {
        this.embeddedDB = JavaDB.Instance().getDB(dbName);
        this.file = file;
    }

    public static void main (String args[]) throws Exception {
        TestDataLoader dataLoader = new TestDataLoader(args[0]);
        dataLoader.load();
    }

    public Connection getConnection () throws SQLException {
        return this.embeddedDB.createConnection();
    }

    public void load() throws Exception {
        TableLoader tableLoader = null;
        BufferedReader br = new BufferedReader(new FileReader(this.file));
        String line = br.readLine();
        while (line != null) {
//            logger.info("Processing line " + line);
            if (line.startsWith(COMMENT) || "".equals(line.trim())) {

            }
            else if (line.startsWith(TABLE_DEFINITION_BEGINS)) {
                if (tableLoader != null) {
                    tableLoader.dumpTableData();
                }
                tableLoader = new TableLoader(this.embeddedDB);
            }
            else {
                if (tableLoader == null) {
                    throw new Exception ("No Table Definition exists. Use "
                            + TABLE_DEFINITION_BEGINS + " to begin defining table");
                }
                tableLoader.processLine(line);
            }
            line = br.readLine();
        }
        if (tableLoader != null) {
            tableLoader.dumpTableData();
        }
    }

    static class TableLoader {

        JavaDB.EmbeddedDB embeddedDB;
        String tableName;
        String [] columnNames;
        String [] columnTypes;
        String [] primaryKeys;
        String insertSql;
        String createSql;
        String delim = ",";
        boolean tableCreated = false;
        Map<Integer, Format> columnFormatters = new HashMap<Integer, Format>();

        public TableLoader (JavaDB.EmbeddedDB embeddedDB) {
            this.embeddedDB = embeddedDB;
        }

        public void processLine (String line) throws Exception {
            if (line.startsWith (TABLE_NAME)) {
                this.tableName = line.substring(TABLE_NAME.length()+1).trim();
            }
            else if (line.startsWith (COLUMN_NAMES)) {
                this.columnNames = line.substring(COLUMN_NAMES.length()+1).trim().split(delim, -1);
            }
            else if (line.startsWith (COLUMN_TYPES)) {
                this.columnTypes = line.substring(COLUMN_TYPES.length()+1).trim().split(delim, -1);
            }
            else if (line.startsWith (PRIMARY_KEYS)) {
                this.primaryKeys = line.substring(PRIMARY_KEYS.length()+1).trim().split(delim, -1);
            }
            else if (line.startsWith(TABLE_DEFINITION_ENDS)) {
                createTable();
            }
            else {
                if (tableCreated) {
                    insertData (line);
                }
                else {
                    throw new Exception ("Table was not created. Missing the line " +
                            TABLE_DEFINITION_ENDS + " or table wasn't created successfully");
                }
            }
        }

        private void insertData(String line) throws Exception {
            Connection conn = embeddedDB.createConnection();
            PreparedStatement pstmt = conn.prepareStatement(insertSql);
            String [] tokens = line.split(delim, -1);
            pstmt.clearParameters();
            for (int i=0; i<tokens.length; i++) {
                Format fmt = columnFormatters.get(i);
                final Object o = fmt == null ? tokens[i] : fmt.parseObject(tokens[i]);
                pstmt.setObject(i+1, o);
//                logger.info ("Setting object in index: " + i + " object " + o + " class: " + o.getClass());
            }
            pstmt.execute();
        }


        public void createTable  () throws Exception {
            Connection conn = embeddedDB.createConnection();
            DatabaseMetaData dbmd = conn.getMetaData();
            boolean tableExists = dbmd.getTables(null, "APP", tableName, new String[]{"TABLE"} ).next();
            if (tableExists) {
                logger.info("Dropping existing table "+ tableName);
                // Drop table
                conn.prepareStatement("drop table " + tableName).execute();
             }

            StringBuilder sql = new StringBuilder("create table ");
            sql.append (tableName).append (" (");

            // Define columns
            for (int i=0; i<columnNames.length; i++) {
                String columnName = columnNames[i].trim();
                String columnType = columnTypes[i].trim().toUpperCase();
                sql.append(columnName).append (' ');
                sql.append(columnType).append (", ");

                switch (columnType) {
                    case "INT":
                    case "DOUBLE":
                    case "FLOAT":
                        columnFormatters.put(i, NumberFormat.getInstance());
                        break;
                    case "TIMESTAMP":
                        columnFormatters.put(i, sdf);
                        break;
                }

            }

            // Define primary key
            sql.append("\n primary key ("+commaSeparated(primaryKeys)+")");
            sql.append(" )");
            createSql = sql.toString();

            sql.setLength(0);
            sql.append("insert into " + tableName + " (");
            sql.append(commaSeparated(columnNames) + ") \n");
            String paramHolders [] = new String[columnNames.length];
            Arrays.fill(paramHolders, "?");
            sql.append("values (" + commaSeparated(paramHolders) + ") \n");
            insertSql = sql.toString();
            logger.info("Creating sql with the following sql statement: \n" + createSql);
            PreparedStatement stmt = conn.prepareStatement(createSql);
            stmt.execute();

            logger.info("Table " + tableName + " successfully created.");
            logger.info("Insert statement sql: \n" + insertSql);
            tableCreated = true;
        }

        private static String commaSeparated (String [] strings) {
            StringBuilder sb = new StringBuilder();
            for (int i=0; i<strings.length; i++) {
                sb.append(strings[i]);
                if (i < strings.length - 1) {
                    sb.append( ", ");
                }
            }
            return sb.toString();
        }

        public void dumpTableData() throws Exception {
            ResultSet rs = embeddedDB.createConnection().prepareStatement("select * from " + tableName).executeQuery();
            StringBuilder sb = new StringBuilder();
            sb.append("RESULTSet DUMP: \n");
            ResultSetMetaData rsmd = rs.getMetaData();
            int col = rsmd.getColumnCount();
            for (int i=0; i<col; i++) {
                sb.append(rsmd.getColumnName(i+1)).append(',');
            }
            sb.append('\n');
            while (rs.next()) {
                for (int i=0; i<col; i++) {
                    sb.append(rs.getObject(i+1)).append(',');
                }
                sb.append('\n');
            }
            logger.info(sb.toString());
            sb.setLength(0);
            logger.info(sb.toString());
        }
    }
}
