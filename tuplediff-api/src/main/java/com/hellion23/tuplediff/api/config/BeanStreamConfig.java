package com.hellion23.tuplediff.api.config;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.hellion23.tuplediff.api.model.TDException;
import lombok.*;

import java.util.Iterator;

/**
 * This BeanStreamConfig cannot be marshalled/unmarshalled into a JSON configuration because it cannot be run
 * remotely, only locally in the same VM as the ClassLoader that knows about and can load the class in question.
 * Furthermore, it cannot iterate over a Collection that may or may not
 */
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@Builder
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
@ToString
public class BeanStreamConfig implements StreamConfig {
    @JacksonXmlProperty(localName = "name")
    @JsonProperty(value="name")
    String name;

    @JacksonXmlProperty(localName = "sortedbyprimarykey")
    @JsonProperty(value="sortedbyprimarykey")
    boolean sortedByPrimaryKey = false;

    @Override
    public String getName() {
        return name;
    }

    public boolean isSortedByPrimaryKey() {
        return sortedByPrimaryKey;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    Class beanClass;

    @JsonSetter(value="beanclass")
    private void unmarshalBeanClass (String beanclass) {
        try {
            this.beanClass = Class.forName(beanclass);
        }
        catch (Exception ex) {
            throw new TDException("Could not instantiate class ["+beanClass+"] using the system classLoader. Verify " +
                    " the class has been included in library path.", ex);
        }
    }

    @JsonGetter(value="beanclass")
    private String marshalBeanClass() {
        return beanClass.getName();
    }

    Iterator iterator;
    @JsonSetter(value="iterator")
    private void unmarshalIterator (String iterator) {
        // No Op.
    }

    @JsonGetter(value="iterator")
    private String marshalIterator() {
        if (iterator != null) {
            throw new UnsupportedOperationException("BeanStreamConfig class CANNOT be marshalled/unmarshalled. This configuration" +
                    " is intended to be configured/run in the same JVM. ");
        }
        return null;
    }

    public Class getBeanClass() {
        return beanClass;
    }

    public Iterator getIterator() {
        return iterator;
    }
}
