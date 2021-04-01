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
@NoArgsConstructor
@AllArgsConstructor
@Data
public class StringSourceConfig implements SourceConfig {
    @JacksonXmlProperty(localName = "string")
    @JsonProperty(value="string")
    String string;
}
