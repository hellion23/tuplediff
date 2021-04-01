package com.hellion23.tuplediff.api.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import lombok.*;

@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@Builder
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
@ToString
public class CSVStreamConfig implements StreamConfig {

    @JacksonXmlProperty(localName = "name")
    @JsonProperty(value="name")
    String name;

    @Override
    public String getName() {
        return name;
    }

    @JacksonXmlProperty(localName = "source")
    @JsonProperty(value="source")
    SourceConfig source;

    public SourceConfig getSource() {
        return source;
    }

 }
