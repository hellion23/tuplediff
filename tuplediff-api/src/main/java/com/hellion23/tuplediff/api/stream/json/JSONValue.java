package com.hellion23.tuplediff.api.stream.json;


import java.util.Comparator;
import java.util.Objects;

/**
 * Container for a JSON type and value.
 */
public class JSONValue implements Comparable <JSONValue> {
    public static JSONValue TRUE = new JSONValue(JSONType.BOOLEAN, Boolean.TRUE);
    public static JSONValue FALSE = new JSONValue(JSONType.BOOLEAN, Boolean.FALSE);
    public static JSONValue NULL = new JSONValue(JSONType.NULL, null);

    static Comparator<JSONValue> jsonValueComparator = Comparator
            .comparing(JSONValue::getJSONType)
            .thenComparing(JSONValue::getComparable);

    Comparable comparable;
    JSONType jsonType;

    public JSONValue (JSONType jsonType, Comparable comparable) {
        this.jsonType = jsonType;
        this.comparable = comparable;
    }

    @Override
    public int compareTo(JSONValue o) {
        return jsonValueComparator.compare(this, o);
    }

    public Comparable getComparable() {
        return comparable;
    }

    public JSONType getJSONType() {
        return jsonType;
    }

    public String toString () {
        return "(" + jsonType + ")" + comparable;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof JSONValue)) return false;
        JSONValue jsonValue = (JSONValue) o;
        return Objects.equals(comparable, jsonValue.comparable) &&
                jsonType == jsonValue.jsonType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(comparable, jsonType);
    }
}
