package com.hellion23.tuplediff.api.variable;

import com.hellion23.tuplediff.api.compare.ComparableComparator;
import com.hellion23.tuplediff.api.model.TDException;
import com.hellion23.tuplediff.api.stream.json.JSONStreamParser;
import com.hellion23.tuplediff.api.stream.source.StreamSource;
import com.hellion23.tuplediff.api.stream.sql.DataSourceProviders;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.*;

/**
 * A collection of basic Variable.EvalFunction functions. EvalFunctions are used to acquire the value that would be
 * assigned to the variable
 */
@Slf4j
public class VariableEvals {
    private final static Logger LOG = LoggerFactory.getLogger(VariableEvals.class);
    /**
     * Just reflects the actual value back; identity function.
     *
     * @param value
     * @return
     */
    public static Variable.EvalFunction fromIdentity (Object value) {
        return (context) -> value;
    }

    static class SQLVariableEval implements Variable.EvalFunction {
        String hbdbid, sql, primaryKey;
        DataSource dataSource;

        public SQLVariableEval(DataSource dataSource, String hbdbid, String sql, String primaryKey) {
            this.dataSource = dataSource;
            this.hbdbid = hbdbid;
            this.sql = sql;
            this.primaryKey = primaryKey;
        }

        /**
         * If primaryKey is provided, creates a Map of rows.
         * If not, creates a List of rows.
         * Each row of a ResultSet as a column name -> column value
         *
         * @param context
         * @return
         */
        @Override
        public Object apply(VariableContext context) {
            // Resolve first in case the sql also has things to resolve.
            String sqlToExecute = VariableEngine.doVariableSubstitutions(sql, context);
            Object value;
            if (dataSource == null) {
                dataSource = DataSourceProviders.getDb(hbdbid+".properties").get();
            }
            try (
                    Connection conn = dataSource.getConnection();
                    PreparedStatement ps = conn.prepareStatement(sqlToExecute)
            )
            {
                ResultSet rs = ps.executeQuery();
                ResultSetMetaData rsmd = ps.getMetaData();
                String readColumns [] = new String[rsmd.getColumnCount()];

                boolean primaryKeyLookupSucceeded = primaryKey == null;

                for (int i=1; i<=rsmd.getColumnCount(); i++ ) {
                    readColumns[i-1]=rsmd.getColumnName(i);
                    primaryKeyLookupSucceeded = primaryKeyLookupSucceeded || readColumns[i-1].equalsIgnoreCase(primaryKey);
                }

                if (!primaryKeyLookupSucceeded) {
                    throw new TDException("Could not find defined primary key: " + primaryKey);
                }

                Map<String, Map<String, Object>> rowMap = new HashMap<>();
                List<Map<String, Object>> rowList = new LinkedList<>();
                while (rs.next()) {
                    Map<String, Object> tuple = new HashMap<>();

                    for (String col : readColumns) {
                        tuple.put(col, rs.getObject(col));
                    }

                    if (primaryKey == null) {
                        rowList.add(tuple);
                    }
                    else {
                        rowMap.put(rs.getString(primaryKey), tuple);
                    }
                }
                if (primaryKey == null) {
                    value = rowList;
                }
                else {
                    value = rowMap;
                }
            }  catch (Exception ex) {
                throw new TDException("Could not execute sqlVariableEval because <" + ex.getMessage() + "> sql: " + sql);
            }
            return value;
        }

    }


    /**
     * Creates a variable using sql.
     *
     * @param dataSource  The dataSource to be used for this sql query. If not provided, will use the hbdbid.
     * @param hbdbid      HBDB key, this is the full property file name: "dwv2-production" (tacks on .properties
     *                    at end of property name)
     * @param sql         sql to run.
     * @param primaryKey  If the ResultSet from
     * @return
     */
    public static Variable.EvalFunction fromSQL(DataSource dataSource, String hbdbid, String sql, String primaryKey) {
        return new SQLVariableEval(dataSource, hbdbid, sql, primaryKey);
    }


    static class ScriptVariableEval implements Variable.EvalFunction {
        final String script;

        public ScriptVariableEval(String script) {
            this.script = script;
        }

        /**
         * Returns a Binding object which is actually a Map object.
         * @param variableContext
         * @return
         */
        @Override
        public Object apply(VariableContext variableContext) {
            String scriptToEval = VariableEngine.doVariableSubstitutions(this.script, variableContext);
            try {
                ScriptEngine engine = variableContext.getScriptEngine();
                engine.eval(scriptToEval);
                return engine.getBindings(ScriptContext.ENGINE_SCOPE);
            } catch (ScriptException ex) {
                throw new TDException("Could not execute ScriptEval because <" + ex.getMessage() +
                        "> script: " + scriptToEval);
            }
        }

    }

    /**
     * Evaluate a script and create a Map&lt;String,Object&gt; variables. The script scriptEngine used is Java's "Nashorn"
     * javascript.
     *
     * scriptEngine
     * @param script Javascript script
     * @return
     */
    public static ScriptVariableEval fromScript (String script) {
        return new ScriptVariableEval(script);
    }

    static class JSONEval implements Variable.EvalFunction {
        StreamSource streamSource;
        boolean shouldSort;

        public JSONEval (StreamSource streamSource) {
            this (streamSource, false);
        }

        public JSONEval (StreamSource streamSource, boolean shouldSort) {
            this.streamSource = streamSource;
            this.shouldSort = shouldSort;
        }

        @Override
        public Object apply(VariableContext variableContext) {
            JSONStreamParser parser = new JSONStreamParser(streamSource, null);
            Object value = null;
            List<Comparable> results = new ArrayList<>();
            try {
                parser.open();
                while (parser.hasNext()) {
                    results.add(parser.next().getComparable());
                }
                if (shouldSort) {
                    results.sort(ComparableComparator.comparator);
                }
                value = results;
            } catch(IOException e){
                log.error("Could not open JSONStreamParser for JSONEval", e);
            }
            return value;
        }
    }

    /**
     * Creates a Variable value from JSON (mostly from Http calls).
     * @param streamSource
     * @param shouldSort
     * @return
     */
    public static JSONEval fromJSON (StreamSource streamSource, boolean shouldSort) {
        return new JSONEval(streamSource, shouldSort);
    }
}
