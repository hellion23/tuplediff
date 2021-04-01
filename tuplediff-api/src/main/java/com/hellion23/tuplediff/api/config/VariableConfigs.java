package com.hellion23.tuplediff.api.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * This is the root element wrapper for multi-VariableConfigs.
 */
@JacksonXmlRootElement(localName = "variableconfigs")
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@ToString
public class VariableConfigs {

    @JacksonXmlProperty(localName = "variable")
    @JsonProperty(value="variables")
    @JacksonXmlElementWrapper(useWrapping = false)
    VariableConfig[] variableconfigs;

    public static Builder builder() {
        return new Builder();
    }

    public VariableConfig[] getVariableconfigs() {
        return variableconfigs;
    }

    public void setVariableconfigs(VariableConfig[] variableconfigs) {
        this.variableconfigs = variableconfigs;
    }

    public static class Builder {
        VariableConfig [] configs;

        public Builder add (VariableConfig ... configs) {
            this.configs = configs;
            return this;
        }

        public VariableConfigs build() {
            VariableConfigs cfgs = new VariableConfigs();
            cfgs.variableconfigs = this.configs;
            return cfgs;
        }
    }
}
