package com.hellion23.tuplediff.api.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import lombok.*;

@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@ToString
public class FileSourceConfig implements SourceConfig {
    @JacksonXmlProperty(localName = "path")
    @JsonProperty(value="path")
    String path;

    public String getPath() {
        return path;
    }

    public void setPath (String path) {
        this.path = path;
    }

}
