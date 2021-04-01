package com.hellion23.tuplediff.api.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlText;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * Created by hleung on 6/29/2017.
 */
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
@ToString
public class Param {
    @JacksonXmlProperty(localName = "name", isAttribute = true)
    @JsonProperty(value="name")
    String name;

    @JacksonXmlText
    @JsonProperty(value="value")
    String value;

    public static Param create (String name, String value) {
        if (name == null) {
            throw new IllegalArgumentException("Cannot create a null parameter name ");
        }
        Param p = new Param();
        p.name = name;
        p.value = value;
        return p;
    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "Param{" +
                "name='" + name + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
