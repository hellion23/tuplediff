package com.hellion23.tuplediff.api.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;


@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
@JsonSubTypes({
        @JsonSubTypes.Type(SQLVariableConfig.class),
        @JsonSubTypes.Type(KeyValueVariableConfig.class),
        @JsonSubTypes.Type(ScriptVariableConfig.class),
        @JsonSubTypes.Type(JSONVariableConfig.class),
})
public interface VariableConfig {

    static KeyValueVariableConfig keyValue(String name, Object value) {
        return new KeyValueVariableConfig(name, value);
    }

    static SQLVariableConfig.SQLVariableConfigBuilder sql () {
        return  SQLVariableConfig.builder();
    }

    static ScriptVariableConfig.ScriptVariableConfigBuilder script () {return ScriptVariableConfig.builder();}

    static JSONVariableConfig.JSONVariableConfigBuilder json () { return JSONVariableConfig.builder(); }

    String getName();
}
