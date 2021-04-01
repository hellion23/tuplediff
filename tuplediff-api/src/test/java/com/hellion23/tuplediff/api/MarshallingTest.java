package com.hellion23.tuplediff.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hellion23.tuplediff.api.compare.ComparableLinkedHashMap;
import com.hellion23.tuplediff.api.compare.ComparatorLibrary;
import com.hellion23.tuplediff.api.compare.FieldComparator;
import com.hellion23.tuplediff.api.config.*;
import com.hellion23.tuplediff.api.model.TDSide;
import com.hellion23.tuplediff.api.stream.json.JSONType;
import com.hellion23.tuplediff.api.variable.VariableContext;
import com.hellion23.tuplediff.api.variable.VariableEngine;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by hleung on 6/28/2017.
 */
public class MarshallingTest {
    private final static Logger LOG = LoggerFactory.getLogger(MarshallingTest.class);

    @Test
    public void testComparatorMarshalling () throws Exception {
        FieldComparator comp =  ComparatorLibrary.builder().sameLocalDateTime().names("UPDATED_TIMESTAMP").buildComparator();
        ComparatorConfig config = MarshalUtils.toConfig(comp);
        String marshalled = MarshalUtils.xmlMapper().writerWithDefaultPrettyPrinter().writeValueAsString(config);
        LOG.info("Marshalled XML value: " +  marshalled);
        config = MarshalUtils.xmlMapper().readValue(marshalled, ComparatorConfig.class);
        LOG.info("Marshalled XML value (2): " +  marshalled);
    }

    @Test
    public void testBuilding () throws IOException {
        TDConfig config = TDConfig.builder()
            //////////////////////////////////////////////
            // MANDATORY CONFIGURATIONS
            //////////////////////////////////////////////
            .primaryKey("ID")
            .left(StreamConfig.csvFile("h:\\tmp\\tuplediff\\countries.csv"))
            .right(StreamConfig.sql(
                    "dwv2-production",
                            "select *\n" +
                                    "from (select to_char (vpm_country_id) id, country_cd, UPDATED_TIMESTAMP,\n" +
                                    "null IGNORE_FIELD, 1.001 NUM_FIELD, '1.000' NUM_AS_STR_FIELD\n" +
                                    "from dim_country where country_id > 0 and vpm_country_id is not null\n" +
                                    ") a"
                    ))
            //////////////////////////////////////////////
            // OPTIONAL/ADVANCED CONFIGURATIONS
            //////////////////////////////////////////////
            .name("TestConfig")
            .excludeFields("IGNORE_FIELD", "UPDATED_TIMESTAMP")
            .withComparator(ComparatorConfig.builder().names("UPDATED_TIMESTAMP").sameLocalDateTime().buildConfig())
            .withComparator(ComparatorConfig.builder().classes(Float.class, Double.class).thresholdNumber().buildConfig())
            .withComparator(ComparatorConfig.builder().names("NUM_FIELD").thresholdNumber(.01d).buildConfig())
            .withFormatter(FormatterConfig.builder()
                    .side(TDSide.BOTH)
                    .names("NUM_AS_STR_FIELD")
                    .parse(BigDecimal.class))
            //////////////////////////////////////////////
            // CREATE CONFIGURATION OBJECT
            //////////////////////////////////////////////
            .build();
            String xml = MarshalUtils.toXML(config);
            LOG.info("Marshalled XML value: " +  xml);
            String json = MarshalUtils.toJSON(config);
            LOG.info("Marshalled JSON value: " + json );
            TDConfig unMarshalled = MarshalUtils.readValue(new StringReader(xml), TDConfig.class, MarshalUtils.APPLICATION_XML);
            LOG.info("PrimaryKey: {}, unmarshalled PK: {}", config.getPrimarykey(), unMarshalled.getPrimarykey());
            Assert.assertEquals("Unmarshalled is the same as the original ", config, unMarshalled);
    }


    /**
     * Marshal it into a JSON. Then unmarshal it back to Object. Verify that the Object built is the same!
     * object is the same.
     * @throws IOException
     */
    @Test
    public void testReversibilityOfMarshalling () throws IOException {
        TDConfig config = TDConfig.builder()
                .primaryKey("DATE_ID")
                .left(StreamConfig.sql("dwv2-production", "select * from DIM_COUNTRY"))
                .right(StreamConfig.sql("dwv2-qa", "select * from DIM_COUNTRY"))
                .name("TestConfig")
                .build();

    }

    @Test
    public void testMapMarshalling() {
        Map<String, String> map = new ComparableLinkedHashMap<>();
        map.put("A", "1");
        map.put("B", null);
        map.put(null, "2");
        map.put("D", "3");
        String result = MarshalUtils.marshalMapOfStrings(map);
        Map map2 = MarshalUtils.unmarshalMapOfStrings(result);

        LOG.info(map + " transformed to String: + " + result + "\n and then transformed back to map: " + map2);
        Assert.assertEquals("Maps are equal", map, map2);
        Assert.assertEquals("Map un/marshalled correctly. ", map, map2);
    }

    @Test
    public void testJSONStreamConfig () throws Exception {

        JSONStreamConfig.JSONStreamConfigBuilder builder1 = JSONStreamConfig.builder()
                .compareColumns("A", "B", "C")
                .pathToTuples("")
                .primaryKeyPaths("A", "B")
                .source(
                        SourceConfig.http()
                        .url("http://as-bpipe-server/subscriptions")
                        .method("GET")
                        .build()
                );
        JSONStreamConfig config =  builder1.build();

        ObjectMapper mapper = new ObjectMapper();
        String str = mapper.writeValueAsString(config);
        LOG.info("Initial read " + str);
        JSONStreamConfig config2 = mapper.readValue(str, JSONStreamConfig.class);
        LOG.info("Re-marshalled String: " + config2);
        String str2 = mapper.writeValueAsString(config2);
        LOG.info("Read back in " + str2);
        Assert.assertEquals("Marshalling and unmarshalling results in same Object ", str, str2);

        ComparableLinkedHashMap<String, JSONType> map = new ComparableLinkedHashMap<> ();
        map.put("A", null);
        map.put("B", JSONType.NULL);
        map.put("C", JSONType.NUMBER);

        String str3 = mapper.writeValueAsString(builder1.compareColumns(map).build());
        Assert.assertEquals("Change to undefined JSONType for compareColumns work ",
             "{\"@type\":\"JSONStreamConfig\",\"primarykeypaths\":\"A,B\",\"comparecolumns\":\"A,B,C=NUMBER\",\"pathtotuples\":\"\",\"source\":{\"@type\":\"HttpSourceConfig\",\"url\":\"http://as-bpipe-server/subscriptions\",\"method\":\"GET\"}}",
            str3);
    }


    @Test
    public void testVariableConfigs () throws Exception {
        String script =
                "var df = java.time.format.DateTimeFormatter.ofPattern(\"YYYYMMdd\");\n" +
                        "var prevWorkingDate = \"${TODAY.PREV_WORKING_DATE_ID}\";\n" +
                        "var fromDate = java.time.LocalDate.now();\n" +
                        "var toDate = fromDate.plusDays(5);\n" +
                        "var fromStr = df.format(fromDate);\n" +
                        "var toStr = df.format(toDate);\n";
        String sql = "select * from DIM_COUNTRY";
        String queryVarName = "TUPLEDIFF_QUERY";
        String watermarkSql = "select\n" +
                "MAX(case when PARAMETER_NAME = 'START_DATE' THEN PARAMETER_VALUE ELSE NULL END) START_DATE,\n" +
                "MAX(case when PARAMETER_NAME = 'DAYS_TO_LOAD' THEN PARAMETER_VALUE ELSE NULL END) DAYS_TO_LOAD\n" +
                "FROM Etl_process_control \n" +
                "where PROCESS_NAME = 'PRICE_MASTER'";

        VariableConfigs varConfigs = VariableConfigs.builder().add(
                VariableConfig.script().name("SCRIPT").script(script).build(),
                // Same as formattedStr defined above, but use the variableEngine to resolve this.
                VariableConfig.keyValue("FORMATTED_STR", "${SCRIPT.fromStr}-${SCRIPT.toStr}"),
                VariableConfig.keyValue(queryVarName, sql),
                VariableConfig.sql().name("WATERMARK").hbdbid("dwv2-production").sql(watermarkSql).build()
        ).build();

        String xml = MarshalUtils.toXML(varConfigs);
        String json = MarshalUtils.toJSON(varConfigs);
        LOG.info("XML of config: \n " + xml) ;
        LOG.info("JSON of config: \n " + json);
        VariableContext con = VariableEngine.createVariableContext(varConfigs);
        VariableContext con1 = VariableEngine.createVariableContext(MarshalUtils.readValue(new StringReader (xml), VariableConfigs.class, "text/xml"));
        VariableContext con2 = VariableEngine.createVariableContext(MarshalUtils.readValue(new StringReader (json), VariableConfigs.class, "text/json"));
        compareVariableContexts(con1, con);
        compareVariableContexts(con1, con2);
    }

    @Test
    public void testReadingVariableConfig () throws Exception {
        String jsonPath = path("variable_config.json");
        String xmlPath = path("variable_config.xml");
        VariableConfigs cfg1 = MarshalUtils.readValue(new File(jsonPath), VariableConfigs.class);
        VariableConfigs cfg2 = MarshalUtils.readValue(new File(xmlPath), VariableConfigs.class);
    }

    static void compareVariableContexts (VariableContext con1, VariableContext con2) {
        Map<String, String> con1Map = con1.values().stream().collect(Collectors.toMap(x -> x.getName(), x -> con1.getValueAsString(x.getName())));
        Map<String, String> con2Map = con2.values().stream().collect(Collectors.toMap(x -> x.getName(), x -> con2.getValueAsString(x.getName())));
        Assert.assertEquals("VariableContexts created are the same: ", con1Map, con2Map);
    }

    static String path (String file) {
        return MarshallingTest.class.getResource(file).getPath();
    }
}
