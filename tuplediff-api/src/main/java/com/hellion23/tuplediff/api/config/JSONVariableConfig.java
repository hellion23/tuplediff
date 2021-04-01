package com.hellion23.tuplediff.api.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
public class JSONVariableConfig implements VariableConfig {

    @JacksonXmlProperty(localName = "name")
    @JsonProperty(value="name")
    String name;

    @JacksonXmlProperty(localName = "source")
    @JsonProperty(value="source")
    SourceConfig source;

    @JacksonXmlProperty(localName = "shouldSort")
    @JsonProperty(value="shouldSort")
    boolean shouldSort;
}
