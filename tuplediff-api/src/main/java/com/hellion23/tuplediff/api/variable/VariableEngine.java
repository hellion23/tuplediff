package com.hellion23.tuplediff.api.variable;

import com.hellion23.tuplediff.api.TupleDiff;
import com.hellion23.tuplediff.api.TupleDiffContext;
import com.hellion23.tuplediff.api.config.*;
import com.hellion23.tuplediff.api.model.TDException;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * VariableEngine was modelled on principles similar to Java's ScriptEngine.
 * Variables embedded into a string take the format of ${VARIABLE_NAME} or ${MAP_NAME.VARIABLE_NAME}. The latter
 *  format is only acceptable if the Variable object is a Map&lt;String, Object&gt; type.
 * Initial VariableContext has a couple of useful variables baked into it such as DATE_ID which represents "today",
 *  etc...
 */
@Slf4j
public class VariableEngine {
    private final static Logger LOG = LoggerFactory.getLogger(VariableEngine.class);

    /**
     * Replaces all instances of ${VARIABLE_NAME} or ${MAP_NAME.VARIABLE_NAME} in a String with the variable's value in
     * the VariableContext.
     *
     * @param string
     * @param context
     * @return
     */
    static final Pattern variablePattern = Pattern.compile("\\$\\{([^}]*)\\}");
    public static String doVariableSubstitutions (String string, VariableContext variableContext) {
        List<String> substituted = doVariableSubstitutionsMulti(string, variableContext);
        return substituted.get(0);
    }

    /**
     * Replaces all instances of ${VARIABLE_NAME} or ${MAP_NAME.VARIABLE_NAME} in a String with the variable's value in
     *  the VariableContext.
     * joinCollections set to false will cause the substituted String to fork and create multiple Strings. This will
     * cause a Cartesian product if the Substition for each of these variables are Collections. Here's an example:
     *
     * If variable DATES = [20190401, 20190402] (i.e. an ArrayList)
     * And the String to substitute is "select * from DIM_DATE where DATE_ID in ( ${DATES} )"
     *
     * If joinCollections = true, this will result in these ONE String:
     * "select * from DIM_DATE where DATE_ID in ( 20190401, 20190402 )"
     *
     * If joinCollections = false, this will result in a fork into 2 separate Strings:
     *
     * "select * from DIM_DATE where DATE_ID in ( 20190401 )"
     * "select * from DIM_DATE where DATE_ID in ( 20190402 )"
     *
     * @param string the String that contains variable names which need to be substituted.
     * @param variableContext The Variable names and values.
     * @return
     */
    public static List<String> doVariableSubstitutionsMulti (String string, VariableContext variableContext) {
        if (variableContext == null || string == null) {
            return Collections.singletonList(string);
        }
        log.debug("Doing substitutions for string: " + string);
        LinkedList<String> resultList = new LinkedList<>();
        resultList.add(string);
        Matcher matcher = variablePattern.matcher(string);

//        SubstitutionContext substitutionContext = variableContext.getSubstitutionContext(substitutionContextName);

        // Discover if there are any variables that need to be resolved as they will match the pattern ${VAR}
        Map<String, String> variablesToResolve = new HashMap<>();
        while (matcher.find()) {
            // group(0) = ${NAME}, group(1) = NAME.
            String varName = matcher.group(1);
            String replaceRegex = "\\$\\{"+varName+"\\}";
            variablesToResolve.put(varName, replaceRegex);
        }
        if (variablesToResolve.size() > 0) {
            LOG.info("Resolving these variables : "+variablesToResolve.keySet() + " for string: " + string);
            List<String> toReplaceList = new LinkedList<>();

            // 1) Iteratively go over all variables that need substitution
            // 2) resultList contains the String(s) that need variable substitution. Put all the substitutionList
            //      objects into the toReplace list and clear the resultList which is the result set.
            // 3) For each Variable, get the substitution value.
            // 4) If Substituted value is a non-Collection Object, then use the substituted value to replace the
            //  placeholder ${VARIABLE_NAME} in the String. Add to the resultList
            // 5) If Substituted value is a Collection, then for *each* value, create a new String with that replacement
            //   i.e. for each object in the substitute Collection, replace the ${VARIABLE_NAME} and add *ALL* of these
            //   Strings back to the resultList.
            // 6) Repeat from step 2.
            for (Map.Entry<String, String> me : variablesToResolve.entrySet()) {
                // Drain the list of strings back into the candidates for variable substitutions.
                toReplaceList.clear();
                toReplaceList.addAll(resultList);
                resultList.clear();
                // for String "This is the date ${TODAY;FORMAT=YYYYMMDD;QUOTE_CHAR='}.",
                // replaceRegex is "\$\{TODAY;FORMAT=YYYYMMDD;QUOTE_CHAR='\}" because that is how we'll locate the variable,
                // lookup is "TODAY"
                // VariableStringifier is created from string "FORMAT=YYYYMMDD;QUOTE_CHAR='" as this is the custom logic to
                // stringify format for the substitution value.
                String replaceRegex = me.getValue();
                int index = me.getKey().indexOf(VariableStringifier.SEPARATOR);
                String lookup = index > 0 ? me.getKey().substring(0, index) : me.getKey();
                VariableStringifier variableStringifier = VariableStringifier.DEFAULT;
                // Create a custom Stringifier if there isone defined.
                try {
                    if (index > 0) {
                        variableStringifier = VariableStringifier.create(me.getKey().substring(index+1));
                        log.info("Created custom stringifier: " + variableStringifier);
                    }
                }
                catch (Exception ex) {
                    throw new TDException("Could not create custom Stringifier logic for key " + me.getKey() + ". Reason: " + ex.getMessage(), ex);
                }

                Object substitutionValue = variableContext.getValue(lookup);
//                LOG.info("Looking up: [{}], substitute value: [{}]", me.getKey(), substitutionValue);
                for (String toReplace : toReplaceList) {
                    // Create the new String by replacing the stringified value string into where
                    // the variable name was in the target String.
                    resultList.addAll(
                        variableStringifier.intoStrings(substitutionValue).stream()
                        .map (substitutionString -> toReplace.replaceAll(replaceRegex, substitutionString) )
                        .collect(Collectors.toList())
                    );
                }
            }
            log.info("Variable Key: " + string + " Resolved to Value: " + resultList);
        }

        return resultList;
    }

    /**
     * Create initial VariableContext.
     * If withDefaults, then push some default variables (such as "TODAY" which is a
     * Map of date fields relative to today (e.g. ${TODAY.PREV_WORKING_DATE_ID} pulled from DIM_DATE).
     *
     * @param withDefaults
     * @return
     */
    public static VariableContext createInitialContext (boolean withDefaults ) {
        VariableContext variableContext = new VariableContext();
        if (withDefaults) {
            populateContextWithDefaults(variableContext);
        }
        return variableContext;
    }

    /**
     * Add defaults to Context This is a Hashmap with commonly used variables. In this default
     * implementation the following variables are added:
     *
     * TODAY = all columns of the DIM_DATE row relative to today (i.e. at the time this method is called) will be made
     * available, such as PREV_WORKING_DATE, DATE_ID, NEXT_MONTH_END_DATE etc... These variables are available for
     * reference like so:
     *      'Today's date is ${TODAY.CALENDAR_DATE}. Tomorrow is ${TODAY.NEXT_WORKING_DATE}.'
     *
     * @param variableContext
     */
    public static final String TODAY="TODAY";
    protected static void populateContextWithDefaults(VariableContext variableContext) {
        // Create a variable called "TODAY" that maps to the row in DIM_DATE that represents the current date.
        variableContext.putVariable(new Variable(TODAY, VariableEvals.fromSQL(
                null,
                "dwv2-production",
                "select * from dim_date where calendar_date = trunc(sysdate)",
                null)));
    }

    /**
     * Create a VariableContext object using the provided configs. If variableConfig is a null or is empty or 0 length,
     * create an empty array, create an empty one.
     * @param config
     * @return
     */
    public static VariableContext createVariableContext(VariableConfigs config) {
        return createVariableContext (null, config);
    }

    public static VariableContext createVariableContext(TupleDiffContext tupleDiffContext, VariableConfigs config) {
        VariableConfig[] variableConfig = config.getVariableconfigs();
        if (variableConfig != null && variableConfig.length>0) {
            VariableContext variableContext = createInitialContext(true);
            return mergeVariables(tupleDiffContext, variableContext, config);
        }
        else {
            return createInitialContext(false);
        }
    }

    /**
     * Add Variables defined in the VariableConfigs into the VariableContext. If VariableConfig is non-null and context
     * is null, will create a new, empty one with the variables populated.
     * If VariableConfigs is null, will return the context back unmodified.
     *
     * @param variableContext
     * @param config
     * @return
     */
    public static VariableContext mergeVariables(VariableContext variableContext, VariableConfigs config) {
        return mergeVariables (null, variableContext, config);
    }

    public static VariableContext mergeVariables(TupleDiffContext tupleDiffContext, VariableContext variableContext, VariableConfigs config) {
        if (config != null) {
            VariableConfig[] variableConfig = config.getVariableconfigs();
            List<Variable> variables = new LinkedList<>();
            if (variableContext == null) {
                variableContext = createInitialContext(true);
            }
            for (VariableConfig cfg : variableConfig) {
                Variable.EvalFunction eval;
                if (cfg instanceof KeyValueVariableConfig) {
                    KeyValueVariableConfig kv = (KeyValueVariableConfig)(cfg);
                    Object value = kv.getObjValue() == null ? kv.getValue() : kv.getObjValue();
                    eval = VariableEvals.fromIdentity(value);
                }
                else if (cfg instanceof SQLVariableConfig) {
                    SQLVariableConfig sqlCfg = (SQLVariableConfig) cfg;
                    eval = VariableEvals.fromSQL(sqlCfg.getActualDataSource(), sqlCfg.getHbdbid(), sqlCfg.getSql(), sqlCfg.getPrimaryKey());
                }
                else if (cfg instanceof ScriptVariableConfig) {
                    ScriptVariableConfig scriptCfg = (ScriptVariableConfig) cfg;
                    eval = VariableEvals.fromScript(scriptCfg.getScript());
                }
                else if (cfg instanceof JSONVariableConfig) {
                    JSONVariableConfig jsonCfg = (JSONVariableConfig) cfg;
                    eval = VariableEvals.fromJSON(TupleDiff.constructStreamSource(jsonCfg.getSource(), tupleDiffContext), jsonCfg.isShouldSort());
                }
                else {
                    throw new TDException("Don't know how to instantiate VariableConfig " + cfg.getClass());
                }
                variables.add(new Variable(cfg.getName(), eval));
            }
            LOG.info("Will evaluate these variables " + variables.stream().map(Variable::getName).collect(Collectors.joining(",")));
            variableContext.putVariables(variables);
        }
        return variableContext;
    }

    /**
     * Extracts the embedded VariableConfig definition from the sql statement. This is really ghetto, the variable
     * configuration should be in a separate file and not baked into the SQL query like this.
     *
     *  --VARIABLES_START
     * --{
     * --  "variables" : [ {
     * --    "@type" : "SQLVariableConfig",
     * --    "name" : "UPDATED_TIMESTAMP",
     * --    "hbdbid" : "dwv2-production",
     * --    "sql" : "select to_char(updated_timestamp,'YYYY-MM-DD HH24:MI.SS.FF3') UPDATED_TIMESTAMP from etl_process_control where process_name='PRICE_MASTER' and parameter_name='START_DATE'"
     * --  } ]
     * --}
     * --VARIABLES_END
     * select ${UPDATED_TIMESTAMP} RESULT
     *
     * String[0] = SQL without the variableConfig comment
     * String[1] = Variable Config definition, ready to marshalled into a VariableConfig object.
     * String[2] = Mime ContentType of the VariableConfig
     *
     * @param sql
     * @return
     */
    public static String [] extractEmbeddedVariableConfigsFromSQLComment(String sql) {
        String [] lines = sql.split("\n");
        StringBuilder sqlSb = new StringBuilder();
        StringBuilder varSB = new StringBuilder();
        String contentType = null;
        boolean readingVariables = false;
        for (int i=0; i<lines.length; i++) {
            String line = lines[i].trim();
            if (line.startsWith("--VARIABLES_START")) {
                readingVariables = true; // toggle reading on.
                continue;
            }
            else if (readingVariables && line.equalsIgnoreCase("--VARIABLES_END")) {
                readingVariables = false; // toggle reading off.
                if (contentType == null) {
                    throw new TDException("Extracted variable definition is not in either JSON or XML type: " + varSB.toString());
                }
                continue;
            }
            if (readingVariables) {
                if (line.startsWith("--"))
                    varSB.append(line.substring(2)).append('\n');
                // Cheesy simplistic way to ascertain whether this is a JSON or XML definition.
                if (contentType == null) {
                    if( line.contains(("<variableconfigs>"))) {
                        contentType = "text/xml";
                    }
                    else if (line.contains("\"@type\"")) {
                        contentType = "text/json";
                    }
                }
            }
            else {
                if (i < lines.length-1) {
                    sqlSb.append(line).append('\n');
                }
                else {
                    sqlSb.append(line);
                }
            }
        }

        return new String [] {sqlSb.toString(), varSB.toString(), contentType};
    }
}
