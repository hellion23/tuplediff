package com.hellion23.tuplediff.api.config;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlCData;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.sql.DataSource;

/**
 * Created by hleung on 6/28/2017.
 */
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
public class DBStreamConfig implements StreamConfig {
    @JacksonXmlProperty(localName = "datasource")
    @JsonProperty(value="datasource")
    DataSourceConfig datasource;

    @JacksonXmlProperty(localName = "sql")
    @JsonProperty(value="sql")
    @JacksonXmlCData
    String sql;

    public DataSourceConfig getDatasource() {
        return datasource;
    }

    public String getSql() {
        return sql;
    }

    @JacksonXmlProperty(localName = "name")
    @JsonProperty(value="name")
    String name;

    @Override
    public String getName() {
        return name;
    }

    String [] orderBy;

    @JsonSetter (value="orderBy")
    private void unmarshalPrimaryKey (String orderBy) {
        this.orderBy = MarshalUtils.unmarshalStringArray(orderBy);
    }

    @JsonGetter (value="orderBy")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String marshalPrimaryKey () {
        return MarshalUtils.marshalStringArray(orderBy);
    }

    /**
     * Explicit order by column. This should only be defined if you need to change order by collation.
     * @return
     */
    public String[] getOrderBy () {
        return orderBy;
    }

    /**
     * This is a helper method that creates a Datasource object that can be used for configuration (rather than
     * using the unwieldy builder() calls.
     *
     * @param dataSource
     * @return
     */
    public static DataSourceConfig configureActualDataSource (DataSource dataSource) {
        return DataSourceConfig.builder().actualDataSource(dataSource).build();
    }


}
