package com.hellion23.tuplediff.api.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Created by hleung on 6/28/2017.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
@JsonSubTypes({
        @JsonSubTypes.Type(BeanStreamConfig.class),
        @JsonSubTypes.Type(CSVStreamConfig.class),
        @JsonSubTypes.Type(DBStreamConfig.class),
        @JsonSubTypes.Type(JSONStreamConfig.class)
})
public interface StreamConfig {
    static CSVStreamConfig.CSVStreamConfigBuilder csv() {
        return CSVStreamConfig.builder();
    }

    static CSVStreamConfig csvFile(String file) {
        return csv().source(SourceConfig.file(file)).build();
    }

    static JSONStreamConfig.JSONStreamConfigBuilder json() {
        return JSONStreamConfig.builder();
    }

    static DBStreamConfig.DBStreamConfigBuilder sql () {
        return DBStreamConfig.builder ();
    }

    static DBStreamConfig sql (String hbdbid, String sql) {
        return sql().datasource(DataSourceConfig.builder().hbdbid(hbdbid).build())
                    .sql(sql)
                .build();
    }

    static BeanStreamConfig.BeanStreamConfigBuilder bean () {
        return BeanStreamConfig.builder();
    }

    default String getName() {
        return this.getClass().getName();
    }
}
