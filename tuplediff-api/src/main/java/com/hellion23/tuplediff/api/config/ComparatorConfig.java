package com.hellion23.tuplediff.api.config;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.hellion23.tuplediff.api.compare.ComparatorLibrary;
import lombok.*;


/**
 * Created by hleung on 6/29/2017.
 */
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
@ToString
public class ComparatorConfig {

    /**
     * In conjunction with Comparator class, define a specific Comparator's parameters if needed.
     */
    @JacksonXmlProperty(localName = "params")
    @JsonProperty(value="params")
    Param params[] = null;

    @JsonIgnore
    String [] fieldNames;

    @JsonIgnore
    Class [] fieldClasses;

    /**
     * How to create the comparator. Requires the Comparator class to be defined.
     */
    @JsonIgnore
    Class comparatorClass;

    /**
     * Numeric distance allowed for .equals() to still be true.
     */
    @JacksonXmlProperty(localName = "thresholdnumber")
    @JsonProperty(value="thresholdnumber")
    @Getter
    Long thresholdNumber;

    /**
     * # of millis to be "off" by for .equals() to still be true.
     */
    @JacksonXmlProperty(localName = "thresholddate")
    @JsonProperty(value="thresholddate")
    @Getter
    Long thresholdDate;

    /**
     * Truncate unit. See java.time.temporal.ChronoUnit for enumeration of possible values. E.g. DAYS, HOURS, MILLIS,
     * etc...
     */
    @JacksonXmlProperty(localName = "truncatedate")
    @JsonProperty(value="truncatedate")
    @Getter
    String truncateDate;

    public ComparatorConfig(Class comparatorClass, String[] fieldNames, Class[] fieldClasses, Param params[]) {
        this.comparatorClass = comparatorClass;
        this.fieldClasses = fieldClasses;
        this.fieldNames = fieldNames;
        this.params = params;
    }

    public Param[] getParams() {
        return params;
    }

    public String[] getFieldNames() {
        return fieldNames;
    }

    public Class[] getFieldClasses() {
        return fieldClasses;
    }

    public Class getComparatorClass() {
        return comparatorClass;
    }

    @JsonGetter(value="fieldnames")
    private String marshalFieldNames() {
        return MarshalUtils.marshalStringArray(fieldNames);
    }

    @JsonSetter(value = "fieldnames")
    private void unmarshalFieldNames(String fieldNamesStr) {
        this.fieldNames = MarshalUtils.unmarshalStringArray(fieldNamesStr);
    }

    @JsonGetter(value="fieldclasses")
    private String marshalFieldClasses() {
        return MarshalUtils.marshalArray(fieldClasses, MarshalUtils.ClassToString);
    }

    @JsonSetter(value = "fieldclasses")
    private void unmarshalFieldClasses(String fieldClassesStr) {
        this.fieldClasses = MarshalUtils.unmarshalArray(fieldClassesStr, MarshalUtils.StringToClass, new Class[0]);
    }

    @JsonGetter (value="class")
    private String marshalClass () {
        return MarshalUtils.ClassToString.apply(comparatorClass);
    }

    @JsonSetter (value="class")
    private void unMarshalClass (String f) {
        comparatorClass = MarshalUtils.StringToClass.apply(f);
    }


    public static ComparatorLibrary.Builder builder() {
        return ComparatorLibrary.builder();
    }

}
