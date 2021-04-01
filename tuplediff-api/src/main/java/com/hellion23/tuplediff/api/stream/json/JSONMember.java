package com.hellion23.tuplediff.api.stream.json;

import com.hellion23.tuplediff.api.compare.ComparableLinkedHashMap;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Represents either a JSON "field : element" pair or an "element" of an array. (as defined by
 * http://json.org/ grammar). This is the construct returned by parsing a JSON stream)
 * This exists to support both the common JSON array of objects which translates directly to a TupleDiff Tuple
 * or an Field -> Value construct, where the Field also happens to be the PrimaryKey to be used by TDStreams.
 * JSONMember is the top level Object that is used as the raw datum that would be transformed into a TDTuple.
 *
 */
@EqualsAndHashCode
@ToString(callSuper=true)
public class JSONMember extends JSONValue {
    String field;
    JSONType memberOf;
    ComparableLinkedHashMap<String, JSONValue> element;

    public JSONMember(JSONType jsonType, Comparable comparable) {
        this (jsonType, JSONType.ARRAY, null, null, comparable);
    }

    public JSONMember( ComparableLinkedHashMap<String, JSONValue> element) {
        this (JSONType.OBJECT, JSONType.ARRAY, null, element, element);
    }

    public JSONMember(String field, ComparableLinkedHashMap<String, JSONValue> element) {
        this (JSONType.OBJECT, JSONType.OBJECT, field, element, element);
    }

    public JSONMember(JSONType jsonType, JSONType memberOf, String field, ComparableLinkedHashMap<String, JSONValue> element, Comparable comparable) {
        super (jsonType, comparable);
        this.memberOf = memberOf;
        this.field = field;
        this.element = element;
    }

    /**
     * The type of member this is a part of
     * @return
     *  If JSONType.ARRAY, this is a member of an array [member, member, ...]
     *  If JSONType.OBJECT, this is a field : element pair in a JSON Object
     */
    public JSONType getMemberOf() {
        return memberOf;
    }

    /**
     *
     * @return
     *  If memberType = JSONType.OBJECT and this is a memberOf JSONType.OBJECT returns field name.
     *  If memberType = JSONType.OBJECT and this is a memberOf JSONType.ARRAY, returns null
     *
     */
    public String getField() {
        return field;
    }

    /**
     *
     * @return
     *  If memberType = JSONType.OBJECT and this is a memberOf JSONType.OBJECT returns element of the field : element pair.
     *  If memberType = JSONType.OBJECT and this is a memberOf JSONType.ARRAY, returns the element in the array.
     */
    public ComparableLinkedHashMap<String, JSONValue> getElement() {
        return element;
    }
}
