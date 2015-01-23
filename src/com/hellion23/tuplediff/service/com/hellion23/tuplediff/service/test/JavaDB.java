package com.hellion23.tuplediff.service.com.hellion23.tuplediff.service.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * @author: Hermann Leung
 * Date: 12/16/2014
 */
public class JavaDB {
    static JavaDB instance;
    static String port = "1527";
    static String server = "localhost";
    final static String protocol = "jdbc:derby:";
    Map<String, EmbeddedDB> dbs = new HashMap<String, EmbeddedDB>();
    private static final Logger logger = Logger.getLogger(JavaDB.class.getName());

    private JavaDB() {
        try {
            Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static JavaDB Instance () {
        if (instance == null) {
            instance = new JavaDB();
        }
        return instance;
    }

    public synchronized EmbeddedDB getDB (String name) throws SQLException {
        EmbeddedDB db = dbs.get(name);
        if (db == null) {
            db = new EmbeddedDB(name);
            dbs.put(name, db);
        }
        return db;
    }

    public static class EmbeddedDB {
        String name;
        String dbUrl;

        public EmbeddedDB(String name) throws SQLException {
            this.name = name;
//            this.dbUrl = protocol + "//" + server + ":" + port + "/" + name;
            this.dbUrl = protocol + name;
            String url =  dbUrl  + ";create=true";
            logger.info("Creating DB using this String: " + url);
            Connection conn = DriverManager.getConnection(url);
            logger.info("Database created: " + name);
        }

        public Connection createConnection () throws SQLException{
            Connection conn = DriverManager.getConnection(dbUrl);
            return conn;
        }
    }
}
