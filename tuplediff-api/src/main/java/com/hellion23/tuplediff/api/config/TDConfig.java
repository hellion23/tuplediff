package com.hellion23.tuplediff.api.config;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.Arrays;

/**
 *
 * Created by hleung on 6/28/2017.
 */
@JacksonXmlRootElement(localName = "tuplediff")
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
@ToString
public class TDConfig {
    @JacksonXmlProperty(localName = "name")
    @JsonProperty (value="name")
    String name;

    String [] primarykey;

    @JsonSetter (value="primarykey")
    private void unmarshalPrimaryKey (String primaryKey) {
        this.primarykey = MarshalUtils.unmarshalStringArray(primaryKey);
    }

    @JsonGetter (value="primarykey")
    private String marshalPrimaryKey () {
        return MarshalUtils.marshalStringArray(primarykey);
    }

    @JacksonXmlProperty(localName = "left")
    @JsonProperty (value="left")
    StreamConfig left;

    @JacksonXmlProperty(localName = "right")
    @JsonProperty (value="right")
    StreamConfig right;

    @JacksonXmlProperty(localName = "fieldcomparator")
    @JsonProperty (value="fieldcomparators")
    @JacksonXmlElementWrapper(useWrapping = false)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    ComparatorConfig[] fieldComparators;

    @JacksonXmlProperty(localName = "fieldtypeformatter")
    @JsonProperty (value="fieldtypeformatters")
    @JacksonXmlElementWrapper(useWrapping = false)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    FormatterConfig[] fieldTypeFormatters;

    String [] excludefields;

    @JsonSetter (value="excludefields")
    private void unmarshalExcludeFields (String excludeFields) {
        this.excludefields = MarshalUtils.unmarshalStringArray(excludeFields);
    }

    @JsonGetter (value="excludefields")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String marshalExcludeFields() {
        return MarshalUtils.marshalStringArray(excludefields);
    }

    public String getName() {
        return name;
    }

    public String[] getPrimarykey() {
        return primarykey;
    }

    public StreamConfig getLeft() {
        return left;
    }

    public StreamConfig getRight() {
        return right;
    }

    public ComparatorConfig[] getFieldComparators() {
        return fieldComparators;
    }

    public FormatterConfig[] getFieldTypeFormatters() {
        return fieldTypeFormatters;
    }

    public String[] getExcludefields() {
        return excludefields;
    }

    public static class Builder {
        String name = null;
        String [] primaryKey = null;
        String []  excludeFields = null;
        StreamConfig left  = null;
        StreamConfig right  = null;
        ComparatorConfig[] fieldComparators = null;
        FormatterConfig[] fieldTypeFormatters = null;

        public Builder name (String name) {
            this.name = name;
            return this;
        }

        public Builder primaryKey (String ... primaryKey) {
            this.primaryKey = primaryKey;
            return this;
        }


        public Builder excludeFields (String ... excludeFields) {
            this.excludeFields = excludeFields;
            return this;
        }

        public Builder left (StreamConfig left) {
            this.left = left;
            return this;
        }

        public Builder right (StreamConfig right) {
            this.right = right;
            return this;
        }

        public Builder withComparator(ComparatorConfig comparatorConfig) {
            if (fieldComparators == null) {
                fieldComparators = new ComparatorConfig[] {comparatorConfig};
            }
            else {
                fieldComparators = Arrays.copyOf(fieldComparators, fieldComparators.length+1);
                fieldComparators[fieldComparators.length-1] = comparatorConfig;
            }
            return this;
        }

        public Builder withFormatter(FormatterConfig formatterConfig) {
            if (fieldTypeFormatters == null) {
                fieldTypeFormatters = new FormatterConfig[] {formatterConfig};
            }
            else {
                fieldTypeFormatters = Arrays.copyOf(fieldTypeFormatters, fieldTypeFormatters.length+1);
                fieldTypeFormatters[fieldTypeFormatters.length-1] = formatterConfig;
            }
            return this;
        }

        public Builder fieldComparators (ComparatorConfig... comparatorConfigs) {
            this.fieldComparators = comparatorConfigs;
            return this;
        }

        public Builder fieldTypeFormatters (FormatterConfig... formatterConfigs) {
            this.fieldTypeFormatters = formatterConfigs;
            return this;
        }

        public TDConfig build () {
            assert (primaryKey != null);
            assert (left != null);
            assert (right != null);
            TDConfig config = new TDConfig();
            config.name = name;
            config.primarykey = primaryKey;
            config.left = left;
            config.right = right;
            config.excludefields = excludeFields;
            config.fieldComparators = fieldComparators;
            config.fieldTypeFormatters = fieldTypeFormatters;
            return config;
        }

    }

    public static Builder builder () {
        return new Builder();
    }
}
