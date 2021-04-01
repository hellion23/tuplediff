package com.hellion23.tuplediff.api.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlCData;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import lombok.*;

@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class ScriptVariableConfig implements VariableConfig {

    @JacksonXmlProperty(localName = "name")
    @JsonProperty(value="name")
    String name;

    @JacksonXmlProperty(localName = "script")
    @JsonProperty(value="script")
    @JacksonXmlCData
    String script;

    @Override
    public String getName() {
        return name;
    }

}
