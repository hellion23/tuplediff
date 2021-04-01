package com.hellion23.tuplediff.api.variable;

import com.hellion23.tuplediff.api.model.TDException;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;
import java.util.function.Function;

/**
 * Variable container. If evaluation was completed successfully, the value will be populated.
 * If evaluated, unsuccessfully, an Exception will be stored.
 */
public class Variable {
    String name;
    Object value;
    EvalFunction evalFunction;
    Exception exception = null;
    boolean evaluated = false;

    public Variable (String name, EvalFunction evalFunction) {
        this.name = name;
        this.evalFunction = evalFunction;
        if (!isNameValid(this.name)) {
            throw new TDException("Variable name <"+name+"> is not acceptable because it is either null, empty string" +
                    " or not alphanumeric.");
        }
    }

    private boolean isNameValid (String name) {
        if (name == null || "".equals(name)) {
            return false;
        }
        return name.chars().allMatch(x -> Character.isLetterOrDigit(x) || x == '_' || x == '-');
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public EvalFunction getEvalFunction() {
        return evalFunction;
    }

    public void setEvalFunction(EvalFunction evalFunction) {
        this.evalFunction = evalFunction;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public boolean isEvaluated() {
        return evaluated;
    }

    public void setEvaluated(boolean evaluated) {
        this.evaluated = evaluated;
    }

    public Exception getException() {
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Variable{");
        sb.append("name='").append(name).append('\'');
        sb.append("evalFunction='").append(evalFunction).append('\'');
        if (exception != null) {
            sb.append(", exception=" ).append(exception.toString());
        }
        else {
            sb.append(", value=").append(value);
        }
        sb.append("}");
        return sb.toString();
    }

    public static Variable KeyValue (String key, Object value) {
        return new Variable (key, VariableEvals.fromIdentity(value));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Variable)) return false;
        Variable variable = (Variable) o;
        return Objects.equals(name, variable.name) &&
                Objects.equals(value, variable.value) &&
                Objects.equals(evalFunction, variable.evalFunction) &&
                Objects.equals(evaluated, variable.evaluated) &&
                Objects.equals(exception, variable.exception);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value, evalFunction, evaluated, exception);
    }

    /**
     * Function to evaluate a Variable's value given a VariableContext.
     */
    public interface EvalFunction extends Function<VariableContext, Object> {}
}

