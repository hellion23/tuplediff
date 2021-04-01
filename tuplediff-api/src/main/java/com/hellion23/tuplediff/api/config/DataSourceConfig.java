package com.hellion23.tuplediff.api.config;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.sql.DataSource;
import java.util.Properties;

@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DataSourceConfig {
    @JacksonXmlProperty(localName = "hbdbid")
    @JsonProperty(value="hbdbid")
    String hbdbid;

    @JacksonXmlProperty(localName = "url")
    @JsonProperty(value="url")
    String url;

    @JacksonXmlProperty(localName = "user")
    @JsonProperty(value="user")
    String user;

    @JacksonXmlProperty(localName = "password")
    @JsonProperty(value="password")
    String password;

    @JacksonXmlProperty(localName = "properties")
    @JsonProperty(value="properties")
    Properties properties;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    DataSource actualDataSource = null;

    @JsonSetter(value="actualDataSource")
    private void unmarshalActualDataSource() {
        //No Op
    }

    @JsonGetter(value="actualDataSource")
    private String marshalActualDataSource () {
        if (actualDataSource != null) {
            throw new UnsupportedOperationException("A DBStreamConfig defined using an actual DataSource cannot be marshalled! " +
                    " If you want to have (un)marshallable datasources use URLs or HBDBID's instead. ");
        }
        return null;
    }
}
