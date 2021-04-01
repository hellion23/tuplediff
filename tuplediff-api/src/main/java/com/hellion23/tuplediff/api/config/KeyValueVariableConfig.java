package com.hellion23.tuplediff.api.config;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
@ToString
public class KeyValueVariableConfig implements VariableConfig {
    @JacksonXmlProperty(localName = "name")
    @JsonProperty(value="name")
    String name;

    @JacksonXmlProperty(localName = "value")
    @JsonProperty(value="value")
    String value;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    Object objValue;

    @JsonSetter(value="objValue")
    private void unarmashalObjVlue() {
        //No Op
    }

    @JsonGetter(value="objValue")
    private String marshalObjValue () {
        if (objValue != null) {
            throw new UnsupportedOperationException("A KeyValueVariableConfig defined using a non-String value cannot be marshalled! " +
                    " If you want to have marshallable value set the 'value' must be a String.");
        }
        return null;
    }

    public KeyValueVariableConfig(String name, Object value) {
        this.name = name;
        if (value!=null) {
            if(value instanceof String)
                this.value = (String)value;
            else
                objValue = value;
        }

    }

    @Override
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Object getObjValue() {
        return objValue;
    }

    public void setObjValue(Object objValue) {
        this.objValue = objValue;
    }

    public void build() {
        // No-Op. This is here because all the other configs are builders and have this method.
    }
}
