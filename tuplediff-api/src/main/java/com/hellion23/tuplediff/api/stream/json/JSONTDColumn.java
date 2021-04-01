package com.hellion23.tuplediff.api.stream.json;

import com.hellion23.tuplediff.api.model.TDColumn;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * A TDColumn with an additional JSONType field.
 * The class of this TDColumn is the JSONType's Java class.
 */
@EqualsAndHashCode
@ToString
public class JSONTDColumn extends TDColumn {
    JSONType jsonType;

    public JSONTDColumn (JSONType jsonType, String label) {
        super(label, jsonType == null ? Comparable.class : jsonType.getJavaClass());
        this.jsonType = jsonType;
    }

    public JSONType getJSONType() {
        return jsonType;
    }

//    /**
//     * Produces a normalized Column name for TDStreamMetaData. Normalized column names are upper cased,
//     * have no spaces, etc...
//     *
//     * @return
//     */
//    protected JSONTDColumn normalized () {
//        return new JSONTDColumn(jsonType, TDUtils.normalizeColumnName(name));
//    }

}
