package com.hellion23.tuplediff.api.config;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.hellion23.tuplediff.api.compare.ComparableLinkedHashMap;
import com.hellion23.tuplediff.api.stream.json.JSONType;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_DEFAULT)
// Override the default Lombok Builder because we want to simplify the setters.
//@Builder
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
@ToString
public class JSONStreamConfig implements StreamConfig{

    @JacksonXmlProperty(localName = "name")
    @JsonProperty(value="name")
    String name;

    @JacksonXmlProperty(localName = "pathtotuples")
    @JsonProperty(value="pathtotuples")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    String pathToTuples;

    @JacksonXmlProperty(localName = "source")
    @JsonProperty(value="source")
    SourceConfig source;

    String [] primarykeypaths;

    @JsonSetter (value="primarykeypaths")
    private void unmarshalExcludeFields (String excludeFields) {
        this.primarykeypaths = MarshalUtils.unmarshalStringArray(excludeFields);
    }

    @JsonGetter (value="primarykeypaths")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String marshalExcludeFields() {
        return MarshalUtils.marshalStringArray(primarykeypaths);
    }


    LinkedHashMap<String, JSONType> comparecolumns;

    public String getPathToTuples() {
        return pathToTuples;
    }

    public SourceConfig getSource() {
        return source;
    }

    public String[] getPrimarykeypaths() {
        return primarykeypaths;
    }

    public LinkedHashMap<String, JSONType> getComparecolumns() {
        return comparecolumns;
    }

    @JsonGetter(value="comparecolumns")
    private String marshalCompareColumns () {
        if (this.comparecolumns == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();

        Iterator<Map.Entry<String,JSONType>> it = comparecolumns.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String,JSONType> me = it.next();
            sb.append(me.getKey());
            JSONType type = me.getValue();
            if (type != null && JSONType.NULL != type) {
                sb.append(MarshalUtils.fieldValueDelim).append(type.toString());
            }
            if (it.hasNext()) {
                sb.append(MarshalUtils.delim);
            }
        }
        return sb.toString();
    }

    @JsonSetter(value="comparecolumns")
    private void unmarshalCompareColumns (String string) {
        comparecolumns = new ComparableLinkedHashMap();
        String [] columns = string.split(MarshalUtils.delim);
        for (String col : columns) {
            String [] colDef = col.split(MarshalUtils.fieldValueDelim);
            if (colDef.length == 2) {
                comparecolumns.put(colDef[0], JSONType.valueOf(colDef[1]));
            }
            else {
                comparecolumns.put(colDef[0], null);
            }
        }

    }


    @Override
    public String getName() {
        return name;
    }

    public static JSONStreamConfigBuilder builder() {
        return new JSONStreamConfigBuilder();
    }

    public static class JSONStreamConfigBuilder {
        String pathToTuples;
        String [] primaryKeyPaths;
        ComparableLinkedHashMap <String, JSONType> compareColumns;
        SourceConfig source;

        public JSONStreamConfigBuilder pathToTuples(String pathToTuples) {
            this.pathToTuples = pathToTuples;
            return this;
        }

        public JSONStreamConfigBuilder primaryKeyPaths(String ... primaryKeyPaths) {
            this.primaryKeyPaths = primaryKeyPaths;
            return this;
        }

        public JSONStreamConfigBuilder compareColumns(String... toCompare) {
            compareColumns = new ComparableLinkedHashMap();
            for (String col : toCompare) {
                compareColumns.put(col, null);
            }
            return this;
        }

        public JSONStreamConfigBuilder compareColumns(Map<String, JSONType> toCompare) {
            compareColumns = new ComparableLinkedHashMap();
            compareColumns.putAll(toCompare);
            return this;
        }

        public JSONStreamConfigBuilder source (SourceConfig source) {
            this.source = source;
            return this;
        }

        public JSONStreamConfig build () {
            JSONStreamConfig config = new JSONStreamConfig();
            config.comparecolumns = compareColumns;
            config.pathToTuples = pathToTuples;
            config.source = source;
            config.primarykeypaths = primaryKeyPaths;
            return config;
        }
    }

    @Override
    public String toString() {
        return "JSONStreamConfig{" +
                "pathToTuples='" + pathToTuples + '\'' +
                ", source=" + source +
                ", primarykeypaths=" + Arrays.toString(primarykeypaths) +
                ", comparecolumns=" + comparecolumns +
                '}';
    }
}
