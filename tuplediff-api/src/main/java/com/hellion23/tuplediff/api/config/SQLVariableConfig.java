package com.hellion23.tuplediff.api.config;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlCData;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import lombok.*;

import javax.sql.DataSource;

@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SQLVariableConfig implements VariableConfig {

    @JacksonXmlProperty(localName = "name")
    @JsonProperty(value="name")
    String name;

    @JacksonXmlProperty(localName = "hbdbid")
    @JsonProperty(value="hbdbid")
    String hbdbid;

    @JacksonXmlProperty(localName = "sql")
    @JacksonXmlCData
    @JsonProperty(value="sql")
    String sql;

    @JacksonXmlProperty(localName = "primarykey")
    @JsonProperty(value="primarykey")
    String primaryKey;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    DataSource actualDataSource = null;

    @JsonSetter(value="actualDataSource")
    private void unmarshalActualDataSource() {
        // NOOP. Will not there is nothing to unmarshal here.
    }

    @JsonGetter(value="actualDataSource")
    private String marshalActualDataSource () {
        if (actualDataSource != null) {
            throw new UnsupportedOperationException("A SQLVariableConfig defined using an actual DataSource cannot be marshalled! " +
                    " If you want to have (un)marshallable datasources use URLs or HBDBID's instead. ");
        }
        return null;
    }

//    @Override
//    public String getName() {
//        return name;
//    }

}
