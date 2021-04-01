package com.hellion23.tuplediff.api.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.JacksonXmlModule;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.hellion23.tuplediff.api.compare.*;
import com.hellion23.tuplediff.api.format.MatchCriteria;
import com.hellion23.tuplediff.api.model.TDException;
import org.apache.commons.beanutils.PropertyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.PropertyDescriptor;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Function;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

/**
 * Marshalling and unmarshalling XML and JSON configuration files into *Config objects. Much of the code here is to
 * assist with creating more human-readable XML & JSON configuration files.
 *
 * Created by hleung on 6/29/2017.
 */
public class MarshalUtils {
    private final static Logger LOG = LoggerFactory.getLogger(MarshalUtils.class);
    public static final String delim = ",";
    public static final String fieldValueDelim = "=";
    static XmlMapper xmlMapper;
    static ObjectMapper jsonMapper;
    public static String javaLangPkg = "java.lang";
    public static String comparePkg = "com.hb.apps.tuplediff.compare";
    static Map<String, Class> shortNameClasses = new HashMap<>();

    public static final String TEXT_XML = "text/xml";
    public static final String APPLICATION_XML = "application/xml";
    public static final String TEXT_JSON = "text/json";
    public static final String APPLICATION_JSON = "application/json";

    static {
        JacksonXmlModule module = new JacksonXmlModule();
        module.setDefaultUseWrapper(true);
        xmlMapper = new XmlMapper(module);
        xmlMapper.enableDefaultTyping();
        jsonMapper = new ObjectMapper();

        for (Class each : new Class [] {
            // Short names for Comparators.
            ThresholdDateComparator.class, ThresholdNumberComparator.class, ThresholdLocalDateTimeComparator.class, TruncateLocalDateTimeComparator.class,
            // Short names for java.time classes.
            BigInteger.class, BigDecimal.class, Date.class, LocalDateTime.class
        }) {
            shortNameClasses.put(each.getSimpleName(), each);
        }
    }

    public static ComparatorConfig toConfig (FieldComparator fieldComparator) {
        TypedComparator typedComparator = fieldComparator.getUnderlying();
        MatchCriteria criteria = fieldComparator.getMatchCriteria();
        PropertyDescriptor props [] = PropertyUtils.getPropertyDescriptors(typedComparator);
        Param params [] = null;
        if (props != null && props.length > 0) {
            ArrayList<Param> p = new ArrayList<>();
            for (int i=0; i<props.length; i++){
                if (PropertyUtils.getReadMethod(props[i]) != null && PropertyUtils.getWriteMethod(props[i]) != null) {
                    Object value = null;
                    String name = null;
                    try {
                        name = props[i].getName();
                        value = PropertyUtils.getProperty(typedComparator, name);
                    } catch (Exception e) {
                        throw new TDException("FieldComparator " + fieldComparator + " not marshallable. " +
                                "Unable to get Property value for " + props[i] + " for object: " + typedComparator + " Exception: " + e.getMessage(), e);
                    }
                    p.add(Param.create(name, value == null ? null : value.toString().toUpperCase()));
                }
            }
            if (p.size() > 0) params = p.toArray(new Param[p.size()]);
        }
        Class classes [] = Optional.ofNullable(criteria.getClasses()).map(z -> z.toArray(new Class[z.size()])).orElse(null);
        String names [] = Optional.ofNullable(criteria.getNames()).map(n -> n.toArray(new String[n.size()])).orElse(null);
        return new ComparatorConfig(typedComparator.getClass(), names, classes, params);
    }


    protected static final Function <Class, String> ClassToString = (clazz) -> {
        if (clazz == null) return null;
        String pkg = clazz.getPackage().getName();
        if (javaLangPkg.equals(pkg) || comparePkg.equals(pkg)
                || BigDecimal.class == clazz || BigInteger.class == clazz
                || Date.class == clazz || LocalDateTime.class == clazz
           )
            return clazz.getSimpleName();
        else
            return clazz.getName();
    };

    protected static final Function <String, Class> StringToClass = (clazzName) -> {
        if (clazzName == null) return null;
        Class resolved = shortNameClasses.get(clazzName);
        if (resolved != null) {
            return resolved;
        }
        else {
            String c = clazzName.indexOf('.') < 0 ? javaLangPkg + "." + clazzName : clazzName;
            try {
                return Class.forName(c); // try java.lang first.
            } catch (ClassNotFoundException e) {
                try {
                    c=comparePkg+"."+clazzName; // now try to prepend
                    return Class.forName(c);
                }
                catch (ClassNotFoundException e2) {
                    throw new TDException("Class not found for className: " + c);
                }

            }
        }
    };


    protected static String marshalStringArray(String[] strings) {
        return marshalArray(strings, Function.identity());
    }

    protected static String [] unmarshalStringArray(String string) {
        return unmarshalArray(string, Function.identity(), new String[]{});
    }

    protected static <T> String marshalArray (T [] objects, Function<T, String> curry) {
        if (objects == null) return null;
        return Arrays.asList(objects).stream().map(curry).collect(joining(delim));
    }

    protected static <T> T[] unmarshalArray (String string, Function<String, T> curry, T[] arr) {
        if (string == null) return null;
        return Arrays.asList(string.split(delim)).stream().map(curry).collect(toList()).toArray(arr);
    }

    public static String marshalMapOfStrings (Map <String,String> map) {
        if (map == null)
            return null;
        StringBuilder sb = new StringBuilder();
        Iterator<Map.Entry<String,String>> it = map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String,String> me  = it.next();
            sb.append(me.getKey()).append(fieldValueDelim).append(me.getValue());
            if (it.hasNext()) {
                sb.append(delim);
            }
        }
        return sb.toString();
    }

    public static Map <String,String> unmarshalMapOfStrings (String string) {
        if (string == null || "".equals(string.trim())) {
            return new ComparableLinkedHashMap<>();
        }

        String [] pairs = string.split(delim);
        ComparableLinkedHashMap <String,String>  map = new ComparableLinkedHashMap<> ();
        for (String pair : pairs) {
            String [] kv = pair.split(fieldValueDelim);
            String key = "null".equals(kv[0]) ? null : kv[0];
            String value = "null".equals(kv[1]) ? null : kv[1];
            if (value == null) {
                LOG.info("Sticking in an actual null script for key " + kv[0]);
            }
            map.put(key, value);
        }
        return map;
    }

    public static <T> T readValue(File file, Class<T> valueType) throws IOException {
        return readValue(new FileReader(file), valueType, guessContentType(file));
    }

    public static <T> T readValue(Reader src, Class<T> valueType, String contentType) throws IOException {
        if (isContentTypeJSON(contentType)) {
            return jsonMapper.readValue(src, valueType);
        }
        else if (isContentTypeXML(contentType)) {
            return xmlMapper.readValue(src, valueType);
        }
        // Force read as XML, maybe deprecate this in the future:
        return xmlMapper.readValue(src, valueType);
    }

    public static String writeAsString (Object obj, String contentType)throws IOException {
        if (isContentTypeJSON(contentType)) {
            return jsonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(obj);
        }
        else if (isContentTypeXML(contentType)) {
            return xmlMapper.writerWithDefaultPrettyPrinter().writeValueAsString(obj);
        }
        throw new TDException("Don't know how to write content type: " + contentType +
                ". Supports text/xml, text/json, application/xml or application/json");
    }

    public static String toXML (Object obj) throws IOException {
        return writeAsString(obj, TEXT_XML);
    }

    public static String toJSON (Object obj) throws IOException {
        return writeAsString(obj, TEXT_JSON);
    }

    public static XmlMapper xmlMapper() {
        return xmlMapper;
    }

    public static ObjectMapper jsonMapper () {
        return jsonMapper;
    }



    public static String guessContentType (File file) {
        String contentType = null;
        try {
            contentType = Files.probeContentType(file.toPath());
        } catch (IOException e) {}

        if (contentType == null || !(isContentTypeJSON(contentType) || isContentTypeXML(contentType)) ) {
            String fileName = file.getName();
            String extension = "";
            int i = fileName.lastIndexOf('.');
            if (i > 0) {
                extension = fileName.substring(i+1);
            }
            if ("xml".equalsIgnoreCase(extension)) {
                contentType = APPLICATION_XML;
            }
            else if ("json".equals(extension)) {
                contentType = APPLICATION_JSON;
            }
            LOG.info("Derived contentType {} from file {}", contentType, file);
        }

        return contentType;
    }

    static boolean isContentTypeJSON (String contentType) {
        return TEXT_JSON.equals(contentType) || APPLICATION_JSON.equals(contentType);
    }

    static boolean isContentTypeXML (String contentType) {
        return TEXT_XML.equals(contentType) || APPLICATION_XML.equals(contentType);
    }

}


