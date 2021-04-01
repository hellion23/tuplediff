package com.hellion23.tuplediff.api.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
@JsonSubTypes({
        @JsonSubTypes.Type(FileSourceConfig.class),
        @JsonSubTypes.Type(HttpSourceConfig.class),
        @JsonSubTypes.Type(StringSourceConfig.class)
})
public interface SourceConfig {
    static FileSourceConfig.FileSourceConfigBuilder file () {
        return FileSourceConfig.builder();
    }

    static FileSourceConfig file(String path) {
        return file().path(path).build();
    }

    static HttpSourceConfig.HttpSourceConfigBuilder http () { return HttpSourceConfig.builder();}

    /**
     * Create a new GET method w/ URL and no authentication.
     * @param url
     * @return
     */
    static HttpSourceConfig http (String url) {
        return http().url(url).build();
    }

    static StringSourceConfig.StringSourceConfigBuilder string () {return StringSourceConfig.builder();}

    static StringSourceConfig string (String string) {return StringSourceConfig.builder().string(string).build();}
}
