package com.hellion23.tuplediff.api.config;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.hellion23.tuplediff.api.format.TypeFormatterLibrary;
import com.hellion23.tuplediff.api.model.TDSide;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * Created by hleung on 6/29/2017.
 */
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
@ToString
public class FormatterConfig {
    protected String [] fieldNames;
    protected Class [] from;
    protected Class to;

    @JacksonXmlProperty(localName = "pattern")
    @JsonProperty(value="pattern")
    protected String pattern;

    @JacksonXmlProperty(localName = "side")
    @JsonProperty(value="side")
    protected TDSide side;


    public Class [] getFrom() {
        return from;
    }

    public Class getTo() {
        return to;
    }

    public String getPattern() {
        return pattern;
    }

    public TDSide getSide () {
        return side;
    }

    @JsonGetter (value="from")
    private String marshalFrom () {
        return MarshalUtils.marshalArray(from, MarshalUtils.ClassToString);
    }

    @JsonSetter (value="from")
    private void unMarshalFrom (String fieldClassesStr) {
        this.from = MarshalUtils.unmarshalArray(fieldClassesStr, MarshalUtils.StringToClass, new Class[0]);
    }

    @JsonGetter (value="to")
    private String marshalTo () {
        return MarshalUtils.ClassToString.apply(to);
    }

    @JsonSetter  (value="to")
    private void unmarshalTo (String t) {
        to = MarshalUtils.StringToClass.apply(t);
    }

    @JsonGetter(value="fieldnames")
    private String marshalFieldNames() {
        return MarshalUtils.marshalStringArray(fieldNames);
    }

    @JsonSetter(value = "fieldnames")
    private void unmarshalFieldNames(String fieldNames) {
        this.fieldNames = MarshalUtils.unmarshalStringArray(fieldNames);
    }

    public static TypeFormatterLibrary.Builder builder() {
        return TypeFormatterLibrary.builder();
    }

    public String[] getFieldNames() {
        return fieldNames;
    }
}
