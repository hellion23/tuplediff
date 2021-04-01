package com.hellion23.tuplediff.api.variable;

import com.hellion23.tuplediff.api.model.TDException;
import lombok.extern.slf4j.Slf4j;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.util.*;

/**
 * HashMap of Variables. Map of Variable name -> Variable.
 * All evaluations of variables are done within this context.
 *
 */
@Slf4j
public class VariableContext extends HashMap<String, Variable> {
    ScriptEngine scriptEngine;

    boolean tryToFlattenCollections = true;

    public void putVariable(Variable variable) {
        put(variable.getName(), variable);
    }

    public void putVariables (Collection<Variable> variables) {
        for(Variable variable : variables) {
            putVariable(variable);
        }
    }

    /**
     * Get or create/get ScriptEngine from Variable.Context. Creates Java's Nashorn scriptEngine (which is deprecated
     * from Java 11.
     *
     * @return
     */
    public ScriptEngine getScriptEngine() {
        if (scriptEngine == null) {
            scriptEngine = new ScriptEngineManager().getEngineByName("nashorn");
        }
        return scriptEngine;
    }


    public boolean isTryToFlattenCollections() {
        return tryToFlattenCollections;
    }

    /**
     * If the Object to be returned is a Collection Object and there is only a single value inside, then unwrap
     * the Collection and return the raw single value. If it is a Map, then return the value of the key/value pair if
     * there is a singleton inside of it.
     * This action is done recursively, i.e. after unravelling the singleton object, it will proceed to apply the
     * unravelling technique until it can no longer be unravelled (i.e. not a Map or Collection with a singleton).
     * By default, this should be turned on if your variable evaluations involve sql queries, since these queries
     * result in List of Maps or Map of Maps, which are inconvenient to access.
     *
     * @param tryToFlattenCollections
     */
    public void setTryToFlattenCollections(boolean tryToFlattenCollections) {
        this.tryToFlattenCollections = tryToFlattenCollections;
    }

    /**
     * Use the default Stringifier to convert the value inside the VariableContext into some value.
     *
     * @param key
     * @return
     */
    public String getValueAsString (String key) {
        return VariableStringifier.DEFAULT.intoString(getValue(key));
    }

    /**
     * key is the lookup value in the VariableContext. key can be in the format
     * X, in which case it is a straightforward variableContext.get(X) or
     * X.Y, where X is the lookup value and Y is the name of lookup if variableContext.get(X) is a Map.
     *  If variableContext.get(X) is not a Map &lt;String, Object&gt; an Exception will be thrown.
     *  If variableContext.get(X) returns a null value, an Exception will be thrown.
     * X.Y.Z etc... Same as X.Y, except another level of mapping.
     *
     * The value of the variable is calculated LAZILY; i.e. the evalFunction function is never called until this method
     * is invoked for that specific variable name. If evaluation was successful, the value will be set. If evaluation
     * did not succeed, the exception will be set. In both cases, variable.isEvaluated() will return true.
     *
     * @param key
     * @return
     */
    public Object getValue (String key) {
        // Get the "." prefix, which is the actual variable name to look up. I.e. TODAY.NEXT_WORKING_DATE is
        // resolves into lookup = {"TODAY", "NEXT_WORKING_DATE"}. It will be "TODAY" that will be looked up
        // in the VariableContext.
        String [] lookup = key.split("\\.");
        Variable var = get(lookup[0]);
        if (var == null) {
            throw new TDException("Could not find variable ["+lookup[0]+"] in variable context. The variable" +
                    " needs to be defined before it can be resolved. Current contents of VariableContext: " + this);
        }
        Object value = valueOf(var);

        // ${MAP_NAME.VARIABLE_NAME} format. Each dot will be a lookup into a Map.
        if (lookup.length > 1) {
            if (tryToFlattenCollections && !(value instanceof Map)) {
                value = flattenCollections(value);
            }
            try {
                value = recursiveLookup(lookup, 1, value);
            }
            catch (Exception ex) {
                throw new TDException("Found Variable  [" + lookup[0] + "] but could not resolve the rest of the key. "
                        + key + " from the Map. Exception: " + ex.getMessage(), ex);
            }
        }
        if (tryToFlattenCollections) {
            value = flattenCollections(value);
        }
        return value;
    }

    protected Object recursiveLookup (String lookup[], int lookupIndex, Object value) {
        if (value == null || lookupIndex+1 > lookup.length ) {
            return value;
        }
        else {
            if (value instanceof Map) {
//                LOG.info("Looking up index value " + lookup[lookupIndex] + " in value " + value);
                Map map = (Map) value;
                String key = lookup[lookupIndex];
                if ((map.containsKey(key))) {
                    return recursiveLookup(lookup, lookupIndex+1, map.get(key));
                }
                else if (map.size() == 1) {
                    return recursiveLookup(lookup, lookupIndex, map.values().iterator().next());
                }
                else {
                    throw new TDException("Cannot extract lookup key fragment " + key + " from value " + value);
                }
            }
            else {
                throw new TDException("Could not resolve lookup " + lookup[lookupIndex]
                        + ". Expected a Map, but instead got a " + value.getClass() + " value: " + value);
            }
        }
    }

    /**
     * Progressively unravel the singleton object buried inside a Map or Collection. If the value is not
     * a Collection or a Map or is a null, this method has no effect and will just reflect back the original object.
     * @param value
     * @return
     */
    protected Object flattenCollections (Object value) {
        if (value == null) {
            // NOOP, just return the null.
        }
        else if (value instanceof Map) {
            Map map = (Map) value;
            if (map.size() == 1) {
                value = map.values().iterator().next();
                return flattenCollections(value);
            }
        }
        // List or Set
        else if (value instanceof Collection){
//            LOG.info("Convert to Collection : " + value);
            Collection collection = (Collection) value;
            if (collection.size() == 1) {
                return flattenCollections(collection.iterator().next());
            }
            else {
                List list = new LinkedList();
                for (Object o : collection) {
                    Object flattened = flattenCollections(o);
                    if (o!= flattened) {
                        list.add(flattened);
                    }
                    else {
//                        LOG.info("NOT flattenable " + value);
                        return value;
                    }
                }
                value = list;
            }
        }
        return value;
    }

    /**
     * Lazily calculates variable value if not already evaluated.
     * If not already evaluated, will call the variable's evalFunction function using the given variableContext. If
     * successful, stores the variable value, otherwise, stores the Exception.
     * Once evaluated, will not be re-evaluated.
     * @param variable
     * @return
     */
    protected Object valueOf (Variable variable)  {
        try {
            Object result = variable.getValue();
            if (!variable.isEvaluated()) {
                result = variable.getEvalFunction().apply(this);
                if (result instanceof String) {
                    result = VariableEngine.doVariableSubstitutions((String)result, this);
                }
                variable.setValue(result);
                variable.setEvaluated(true);
            }
            return result;
        }
        catch (Exception ex) {
            variable.setException(ex);
            variable.setEvaluated(true);
            throw new TDException("Could not evaluate variable " + variable.getName() + " Reason: " + ex.getMessage(), ex);
        }
    }

    /**
     * Force an eager evaluation of every variable in the Context.
     */
    public void evaluateAll () {
        values().forEach(v -> valueOf(v));
    }
}
