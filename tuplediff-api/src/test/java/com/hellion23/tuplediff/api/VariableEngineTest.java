package com.hellion23.tuplediff.api;

import com.hellion23.tuplediff.api.config.*;
import com.hellion23.tuplediff.api.handler.CascadingEventHandler;
import com.hellion23.tuplediff.api.handler.CompareEventHandler;
import com.hellion23.tuplediff.api.handler.ConsoleEventHandler;
import com.hellion23.tuplediff.api.handler.StatsEventHandler;
import com.hellion23.tuplediff.api.president.PresidentMocks;
import com.hellion23.tuplediff.api.president.PresidentTest;
import com.hellion23.tuplediff.api.stream.source.StreamSource;
import com.hellion23.tuplediff.api.stream.sql.SortedSQLTDStream;
import com.hellion23.tuplediff.api.variable.Variable;
import com.hellion23.tuplediff.api.variable.VariableContext;
import com.hellion23.tuplediff.api.variable.VariableEngine;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAdjusters;
import java.util.*;

@Slf4j
public class VariableEngineTest implements PresidentTest {

    /**
     * Simple test of variable substitutions. A String that has "${MY_VARIABLE}" embedded into a String will result
     * in the value assigned to MY_VARIABLE to be substituted with that value.
     */
    @Test
    public void testDoVariableSubstitutions () {
        List<Variable> toEvaluate = new LinkedList<>();
        toEvaluate.add (Variable.KeyValue("TEST", "REPLACED"));
        toEvaluate.add (Variable.KeyValue("TEST2", "REPLACED2"));

        Map<String, Object> mapVariable = new HashMap<>();
        mapVariable.put("M1", "MAP_VAR1");
        mapVariable.put("M2", "MAP_VAR2");
        toEvaluate.add (Variable.KeyValue("TEST3", mapVariable));

        //Call this to have the variables evaluated and put into the context
        VariableContext variableContext = VariableEngine.createInitialContext(false);
        variableContext.putVariables(toEvaluate);

        String testStrings [][] = new String[][] {
            // No substitutions required:
            {
                "No substitutions required:",                                            // Test description.
                "This } has no substitutions. No replacements ${ should be necessary. ", // Test string
                "This } has no substitutions. No replacements ${ should be necessary. "  // Expected result.
            },
            // Standard usage. Several variables with basic substitution.
            {
                "Standard usage. Several variables with basic substitution.",
                "This is a ${TEST} sample text { ${TEST2} with lots of } random characters $ { ${TEST} to test result",
                "This is a REPLACED sample text { REPLACED2 with lots of } random characters $ { REPLACED to test result"
            },
            // Mapbased variables. e.g. {$TEST3.M2}, where the variable name is TEST3, which points to a Map object
            // and "M2" is the name of the key in the Map object.
            {
                "Mapbased variables. e.g. {$TEST3.M2}, where TEST3 is a map and M2 is a variable name within it",
                "This is a ${TEST3.M2} sample text { ${TEST3.M1} with lots of } random characters $ { ${TEST} to test result",
                "This is a MAP_VAR2 sample text { MAP_VAR1 with lots of } random characters $ { REPLACED to test result"
            },
        };

        compareStrings("doVariableSubstitution worked for " , testStrings, variableContext);
    }

    /**
     * The DefaultVariableContext has TODAY as a variable that does not require any special references. Validate
     * it's presence and that it can be referenced.
     * The TODAY variable is created by selecting the DIM_DATE row in DW2 that corresponds to the current date.
     */
    @Test
    public void testDefaultVariableContext () {
        VariableContext variableContext = VariableEngine.createInitialContext(true);
        log.info("Context contents: " + variableContext);
        DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyyMMdd");
        LocalDate date = LocalDate.now();
        String nowStr = df.format(date);
        LocalDate lastDayOfMonth = date.with(TemporalAdjusters.lastDayOfMonth());
        String eomStr = df.format(lastDayOfMonth);

        String testStrings [][] = new String[][] {
            // No substitutions required:
            {
                "No substitutions required:",
                "This } has no substitutions. No replacements ${ should be necessary. ",
                "This } has no substitutions. No replacements ${ should be necessary. "
            },
            // DIM_DATE related stuff:
            {
                "DIM_DATE related stuff:",
                "Today is ${TODAY.DATE_ID}. The end of month is ${TODAY.MONTH_END_DATE_ID} ",
                "Today is " + nowStr + ". The end of month is " + eomStr +" "
            },
        };

        compareStrings("defaultVariableContext doVariableSubstitution worked for " , testStrings, variableContext);
    }

    /**
     * This is a real-life test query. The comparison isn't run, but tests the ability to do variable substitutions
     * in a
     */
    @Test
    public void testVariableContextAgainstTupleDiff () throws Exception {
        String sql = "select * from DIM_COUNTRY where UPDATED_TIMESTAMP > date '${WATERMARK}'";

        String sqlVariableName = "TUPLEDIFF_QUERY";
        // watermark is the beginning of the day. A real sql will look at some tables to determine where to start.
        String watermarkSql = "select to_char(trunc(sysdate), 'YYYY-MM-DD') WATERMARK from dual";

        VariableConfigs varConfigs = VariableConfigs.builder().add(
            // Create a variable for the sql
            VariableConfig.keyValue(sqlVariableName, sql),
            // Now define the value of WATERMARK. This is to be injected into the SQL defined above.
            VariableConfig.sql().name("WATERMARK").hbdbid("dwv2-production").sql(watermarkSql).build()
        ).build();
        VariableContext varVariableContext = VariableEngine.createVariableContext(varConfigs);

        TDConfig tupleDiffConfig = TDConfig.builder()
                // Instead of putting the sql in here, we can use a variable to inject the value of the sql. This
                // is probably not necessary as i can just do a Java variable substitution, but does demonstrate
                // the ability to do multiple, consective variable substitutions.
                .left   (StreamConfig.sql("dwv2-qa"        ,"${"+sqlVariableName+"}"))
                .right  (StreamConfig.sql("dwv2-production","${"+sqlVariableName+"}"))
                .primaryKey(
                        "COUNTRY_ID")
                .build();

        log.info("TupleDiffConfig SQL JSON: " + MarshalUtils.writeAsString(tupleDiffConfig, "application/json"));
        log.info("TupleDiffConfig Variables JSON: " + MarshalUtils.writeAsString(varConfigs, "application/json"));

        validateMarshalling(tupleDiffConfig, varConfigs);

        TupleDiffContext tableDiffContext = new TupleDiffContext();
        tableDiffContext.setEventHandler(new ConsoleEventHandler());
        tableDiffContext.setVariableContext(varVariableContext);

        TDCompare comparison = TupleDiff.configure(tupleDiffConfig, tableDiffContext);

        SortedSQLTDStream leftSQL = (SortedSQLTDStream)comparison.getLeft();
        SortedSQLTDStream rightSQL = (SortedSQLTDStream)comparison.getRight();

        String expectedSQL = "select * from DIM_COUNTRY where UPDATED_TIMESTAMP > date '"+
                DateTimeFormatter.ofPattern("yyyy-MM-dd").format(LocalDate.now()) +
                "'";
        log.info("EXPECTEDSQL " + expectedSQL);
        log.info("Variable sql replacement [{}]", leftSQL.getSql());

        // Don't need to run the TupleDiff, just verify that the sql got changed correctly.
        Assert.assertEquals("Left SQL query changed ", expectedSQL, leftSQL.getSql());
        Assert.assertEquals("Right SQL query changed ", expectedSQL, rightSQL.getSql());
    }

    @Test
    public void testJavascriptVariables () {
        DateTimeFormatter df = java.time.format.DateTimeFormatter.ofPattern("YYYYMMdd");
        LocalDate fromDate = java.time.LocalDate.now();
        LocalDate toDate = fromDate.plusDays(5);
        String fromStr = df.format(fromDate);
        String toStr = df.format(toDate);

        // Javascript-ized version of the same code above.
        String script =
        "var df = java.time.format.DateTimeFormatter.ofPattern(\"YYYYMMdd\");\n" +
        "var prevWorkingDate = \"${TODAY.PREV_WORKING_DATE_ID}\";\n" +
        "var fromDate = java.time.LocalDate.now();\n" +
        "var toDate = fromDate.plusDays(5);\n" +
        "var fromStr = df.format(fromDate);\n" +
        "var toStr = df.format(toDate);\n";

        String formattedStr = fromStr+"-"+ toStr;

        VariableConfigs varConfigs = VariableConfigs.builder().add(
            VariableConfig.script().name("SCRIPT").script(script).build(),
            // Same as formattedStr defined above, but use the variableEngine to resolve this.
            VariableConfig.keyValue("FORMATTED_STR", "${SCRIPT.fromStr}-${SCRIPT.toStr}")
        ).build();
        VariableContext variableContext = VariableEngine.createVariableContext(varConfigs);

        String prevWorkingDate = variableContext.getValueAsString("TODAY.PREV_WORKING_DATE_ID");

        String testStrings [][] = new String[][]{
            {
                "Basic Javascript formatted variable test",
                formattedStr,
                variableContext.getValueAsString("FORMATTED_STR")
            },
            {
                "Map Javascript formatted variable test",
                prevWorkingDate,
                variableContext.getValueAsString("SCRIPT.prevWorkingDate")
            }
        };

        compareStrings("Test Javascript Variables", testStrings, variableContext);
    }

    @Test
    public void testCustomStringifying () throws Exception {
        VariableConfigs configs = VariableConfigs.builder().add(
            VariableConfig.keyValue("NUMBER", Arrays.asList(6l)),
            VariableConfig.keyValue("NUMBERS", Arrays.asList(4l, 5l)),
            VariableConfig.keyValue("DATE", Arrays.asList(LocalDate.of(2019, 3, 1))),
            VariableConfig.keyValue("DATES", Arrays.asList(
                    LocalDate.of(2019, 1, 31),
                    LocalDate.of(2019, 2, 1)))
        ).build();
        VariableContext variableContext = VariableEngine.createVariableContext(configs);

        String testStrings [][] = {
            {
                "Basic no customization",
                "The date is ${DATES}",
                "[The date is '20190131','20190201']"
            },
            {
                "Date format customization",
                "The date is ${DATES;FORMAT=MM/dd/yyyy}",
                "[The date is '01/31/2019','02/01/2019']"
            },
            {
                "Date quote definition",
                "The date is ${DATES;QUOTE_CHAR=\"}",
                "[The date is \"20190131\",\"20190201\"]"
            },
            {
                "Date quote, but override with quote_logic = never",
                "The date is ${DATES;QUOTE_LOGIC=NEVER;QUOTE_CHAR=\"}",
                "[The date is 20190131,20190201]"
            },
            {
                "Single date, no quote",
                "The date is ${DATE}",
                "[The date is 20190301]"
            },
            {
                "Single date, no quote, no join",
                "The date is ${DATE;JOIN=FALSE}",
                "[The date is 20190301]"
            },
            {
                "Multiple numbers, no quote",
                "The number is ${NUMBERS}",
                "[The number is 4,5]"
            },
            {
                "Multiple numbers, force always quote",
                "The number is ${NUMBERS;QUOTE_LOGIC=ALWAYS}",
                "[The number is '4','5']"
            },
            {
                "Multiple numbers, don't join and force always quote",
                "The number is ${NUMBERS;JOIN=FALSE;QUOTE_LOGIC=ALWAYS}",
                "[The number is '4', The number is '5']"
            },
            {
                "Single numbers, default is no quote",
                "The number is ${NUMBER}",
                "[The number is 6]"
            },
            {
                "Single numbers, QUOTE_LOGIC= AUTO is no quote",
                "The number is ${NUMBER;QUOTE_LOGIC=AUTO}",
                "[The number is 6]"
            },
            {
                "Single number, don't join",
                "The number is ${NUMBER;JOIN=FALSE}",
                "[The number is 6]"
            },
            {
                "Multiple variables, join = false for one variable",
                "The number is ${NUMBER} and the date is ${DATES;JOIN=FALSE}",
                "[The number is 6 and the date is '20190131', The number is 6 and the date is '20190201']"
            },
            {
                "Multiple variables, join = false for both. CARTESIAN PRODUCT!!",
                "The number is ${NUMBERS;JOIN=FALSE;QUOTE_LOGIC=ALWAYS} and the date is ${DATES;JOIN=FALSE}",
                "[The number is '4' and the date is '20190131', The number is '4' and the date is '20190201', The number is '5' and the date is '20190131', The number is '5' and the date is '20190201']"
            }
        };

        compareStringsMulti("testCustomStringifying", testStrings, variableContext);
    }

    /**
     * Variables can take many shapes. It may be:
     * 1) a singleton, which is a single value, such as today.
     * 2) a tuple, which is a map of key-value pairs
     * 3) multiple singletons, which an array of singletons.
     * 4) multiple tuples, which is an array of tuples (maps).
     *
     * This test validates all these types of variable shapes can be assigned to a variable and referenced.
     * @throws Exception
     */
    @Test
    public void testKeyLookupInVariableContexts () throws Exception {
        String singleColumnSelect = "select DATE_ID from dim_date";
        String multiColumnSelect = "select DATE_ID, WEEKDAY from dim_date";
        String singleRowWhere = " where date_id in ( 19700101 )";
        String multiRowWhere = " where date_id in ( 19700101, 19700102 )";
        SQLVariableConfig.SQLVariableConfigBuilder builder = VariableConfig.sql().hbdbid("dwv2-production");

        VariableConfigs configs = VariableConfigs.builder().add(
            // Result: Single Integer row 1, column 1
            builder.name("ONE_COL_ONE_ROW").sql(singleColumnSelect + singleRowWhere).build(),
            // Result: List<Integer>.
            builder.name("ONE_COL_MULTI_ROW").sql(singleColumnSelect + multiRowWhere).build(),
            // Result: List<String>.
            builder.name("ONE_COL_MULTI_ROW_STR").sql("select WEEKDAY from dim_date" + multiRowWhere).build(),
            // Result: Row Map of [COLUMN] -> [COLUMN_VALUE]
            builder.name("MULTI_COL_ONE_ROW").sql(multiColumnSelect + singleRowWhere).build(),
            // Result: Map of maps: DATE_ID -> Row Map of [COLUMN] -> [COLUMN_VALUE]
            builder.name("MULTI_COL_MULTI_ROW").sql(multiColumnSelect + multiRowWhere).primaryKey("DATE_ID").build(),
            // Result: Map of maps: DATE_ID -> Row Map of [COLUMN] -> [COLUMN_VALUE], however because there is a
            // Primary key defined, you can always look up using primary key.
            builder.name("MULTI_COL_ONE_ROW_WITH_PK").sql(multiColumnSelect + singleRowWhere).primaryKey("DATE_ID").build()
        ).build();

        VariableContext variableContext = VariableEngine.createVariableContext(configs);

        String testStrings [][] = new String[][]{
            {
                "Result: Single Integer row 1, column 1",  // Test description
                "${ONE_COL_ONE_ROW}",                      // How to reference the variable.
                "19700101"                                 // Expected value.
            }
            ,{
                "Result: List<Integer>.",
                "${ONE_COL_MULTI_ROW}",
                "19700101,19700102"
            }
            ,{
                "Result: Row Map of [COLUMN] -> [COLUMN_VALUE]",
                "${MULTI_COL_ONE_ROW.WEEKDAY}",         // Use dot (.) operator to get the value in the map.
                "Thursday"
            }
            ,{
                "Result: Weekday of the date 19700101, with multiple rows ",
                "${MULTI_COL_MULTI_ROW.19700101.WEEKDAY}",   // Primary key is the DATE_ID, use dot (.) operator to access.
                "Thursday"
            }
            ,{
                "Result: Weekday of the date 19700101, with one row. ",
                "${MULTI_COL_ONE_ROW_WITH_PK.19700101.WEEKDAY}",
                "Thursday"
            }
            ,{
                "Result: Weekday of the date 19700101, within a map structure. ",
                "${MULTI_COL_ONE_ROW_WITH_PK.WEEKDAY}",
                "Thursday"
            }
            ,{
                "Result: one col, multi row -> List<String>s. ",
                "${ONE_COL_MULTI_ROW_STR}",
                "'Thursday','Friday'"
            }

        };
        compareStrings("Test SQL Variables", testStrings, variableContext);
        log.info("VariableConfigs:\n" + MarshalUtils.toJSON(configs));
    }

    /**
     *  Validates that a variable resulting from a query can be assigned to a variable and then the variable is referenced
     * inside the SQL query. Note that this is embedded as a comment in the SQL itself!. Left side is embedded variable
     * configuration in json and right sql is embedded as xml.
     *
      * @throws Exception
     */
    @Test
    public void testEmbeddedVariableSqlSubstitution() throws Exception {
        log.info ("Working dir: " + Paths.get(".").toAbsolutePath().normalize().toString());
        // This version of the SQL has the variable definitions in JSON format and assigns a singleton
        // value to the query. So, the single value returned (1 row, 1 column) is assigned to that value.
        String sql =
                "--VARIABLES_START\n" +
                "--{\n" +
                "--  \"variables\" : [ {\n" +
                "--    \"@type\" : \"SQLVariableConfig\",\n" +
                "--    \"name\" : \"UPDATED_TIMESTAMP\",\n" +
                "--    \"hbdbid\" : \"dwv2-production\",\n" +
                "--    \"sql\" : \"select to_char(updated_timestamp,'YYYY-MM-DD HH24:MI.SS') UPDATED_TIMESTAMP from etl_process_control where process_name='PRICE_MASTER' and parameter_name='START_DATE'\"\n" +
                "--  } ]\n" +
                "--}\n" +
                "--VARIABLES_END\n" +
                "select '${UPDATED_TIMESTAMP}' DT, 'NO_TEXT' FLD from dual\n"
                ;
        String [] extracted = VariableEngine.extractEmbeddedVariableConfigsFromSQLComment(sql);
        log.info("\n SQL: \n{}\n VarConfig contentType: {}\n VARConfig: \n{}", extracted[0], extracted[2], extracted[1]);
        VariableConfigs variableConfigs = MarshalUtils.readValue(new StringReader(extracted[1]), VariableConfigs.class, "text/json");
        VariableContext variableContext = VariableEngine.createVariableContext(variableConfigs);
        String out = VariableEngine.doVariableSubstitutions(sql, variableContext);
        log.info("Original: \n {}\n Replaced: \n {}", sql, out);

        // This version of the SQL has the variable definitions in XML format and makes the entire ETL_PROCESS_CONTROL
        // table available for variable referencing. the process_name and parameter_name is bashed together with the
        // "::" delimiter to create a single primary key for lookup. Querying the whole ETL_PROCCESS_CONTROl table
        // allows one to reference multiple values in a single snapshot in time.
        String sql2 =
                "--VARIABLES_START\n" +
                "--<variableconfigs>\n" +
                "--  <variable _type=\"SQLVariableConfig\">\n" +
                "--    <name>ETL_PROCESS_CONTROL</name>\n" +
                "--    <hbdbid>dwv2-production</hbdbid>\n" +
                "--    <sql><![CDATA[select process_name || '::' || parameter_name as PARAM_NAME, to_char(updated_timestamp,'YYYY-MM-DD HH24:MI.SS') UPDATED_TIMESTAMP, PARAMETER_VALUE from ETL_PROCESS_CONTROL]]></sql>\n" +
                "--    <primarykey>PARAM_NAME</primarykey>\n" +
                "--  </variable>\n" +
                "--</variableconfigs>\n"+
                "--VARIABLES_END\n" +
                "select '${ETL_PROCESS_CONTROL.PRICE_MASTER::START_DATE.UPDATED_TIMESTAMP}' DT, 'NO_TEXT' FLD from dual\n"
                ;
        String [] extracted2 = VariableEngine.extractEmbeddedVariableConfigsFromSQLComment(sql2);
        log.info("\n SQL2: \n{}\n VarConfig2 contentType: {}\n VARConfig2: \n{}", extracted2[0], extracted2[2], extracted2[1]);
        VariableConfigs variableConfigs2 = MarshalUtils.readValue(new StringReader(extracted2[1]), VariableConfigs.class, extracted2[2]);
        VariableContext variableContext2 = VariableEngine.createVariableContext(variableConfigs2);
        String out2 = VariableEngine.doVariableSubstitutions(sql2, variableContext2);
        log.info("SQL 1 \n{}\n SQL 2:\n {}", out, out2);
        Assert.assertEquals("Embedded SQL works for both XML and JSON formats, as well as singleton vs. multi-row",
                variableContext.getValueAsString("UPDATED_TIMESTAMP"),
                variableContext2.getValueAsString("ETL_PROCESS_CONTROL.PRICE_MASTER::START_DATE.UPDATED_TIMESTAMP"));

        // Now run the actual sql queries and bash them together. This should result in the same values.
        TDConfig tdConfig = TDConfig.builder()
                .left(StreamConfig.sql("dwv2-production", sql))
                .right(StreamConfig.sql("dwv2-production",sql2))
                .primaryKey("DT").build();
        StatsEventHandler stats = new StatsEventHandler();
        TupleDiff.configure(tdConfig, stats).compare();
        Assert.assertEquals("Running as TupleDiff results in all matches ", 1, stats.getNumMatched() );
    }

    /**
     * This test validates that a variable can be assigned using a sql query. An exception should not be thrown if there
     * are no errors.
     */
    @Test
    public void testVariableSQL () {
        String sql = "--VARIABLES_START\n" +
                "--{\n" +
                "--  \"variables\" : [ {\n" +
                "--    \"@type\" : \"SQLVariableConfig\",\n" +
                "--    \"name\" : \"UPDATED_TIMESTAMP\",\n" +
                "--    \"hbdbid\" : \"dwv2-production\",\n" +
                "--    \"sql\" : \"select to_char(updated_timestamp,'YYYY-MM-DD HH24:MI.SS') UPDATED_TIMESTAMP from etl_process_control where process_name='PRICE_MASTER' and parameter_name='START_DATE'\"\n" +
                "--  } ]\n" +
                "--}\n" +
                "--VARIABLES_END\n" +
                "select '${UPDATED_TIMESTAMP}' TIME  from dual";
        String sql2 = "select '${UPDATED_TIMESTAMP}' TIME  from dual";
        TDConfig tdConfig = TDConfig.builder()
                .left(StreamConfig.sql("dwv2-production", sql))
                .right(StreamConfig.sql("dwv2-production", sql2))
                .primaryKey("TIME").build();
//                .primaryKey("PAL_MASTER_SEC_ID", "D", "SOURCE_CD").build();
        StatsEventHandler stats = new StatsEventHandler();
        TupleDiff.configure(tdConfig, stats).compare();
    }


    /**
     * This test validates that we can read a .json and assign the parsed values into a Variable. In this example,
     * an array of integer ID's are read from a json file and correctly converted into an number array.
     * @throws Exception
     */
    @Test
    public void testVariableJSON () throws Exception{
        TupleDiffContext tupleDiffContext = new TupleDiffContext();

        VariableConfigs variableConfigs = VariableConfigs.builder().add(
                VariableConfig.json()
                        .name("LEFT_IDS")
                        .source(SourceConfig.file(pathOf(presidentLeftIds)))
                        .build()
                ,
                VariableConfig.json()
                        .name("RIGHT_IDS")
                        .source(PresidentMocks.httpSourceConfigOfFile(presidentRightIds))
                        .build()
        ).build();
        VariableContext variableContext = VariableEngine.createVariableContext(tupleDiffContext, variableConfigs);
        StatsEventHandler seh = new StatsEventHandler();
        CompareEventHandler ceh = new CascadingEventHandler(seh);
        tupleDiffContext.setVariableContext(variableContext);
        tupleDiffContext.setEventHandler(ceh);

        // If this cast doesn't work, Exception thrown. The JSON variable conversion should have created this as
        // a List of Numbers.
        List <Number> leftIds = (List <Number>) variableContext.getValue("LEFT_IDS");
        List <Number> rightIds = (List <Number>) variableContext.getValue("RIGHT_IDS");

        log.info("LEFT_IDS: " + variableContext.getValue("LEFT_IDS"));
        log.info("RIGHT_IDS: " + variableContext.getValue("RIGHT_IDS"));

        Assert.assertEquals("LEFT President IDs ", leftIds.toString(), "[1, 2, 3, 4, 5, 7]");
        Assert.assertEquals("RIGHT President IDs ", rightIds.toString(), "[1, 2, 3, 41, 51, 6]");


        SourceConfig srcConfig = PresidentMocks.httpSourceConfigOfFile("president_${LEFT_IDS;JOIN=FALSE}.json");

        StreamSource ss = TupleDiff.constructStreamSource(srcConfig, tupleDiffContext);
        String result = openAndReadIntoString(ss);
        log.info("Output: " + result);

    }

    @Test
    public void testVariousEvalTypes () throws Exception {
        // Timezone aware (EST) ISO date format yyyy-MM-ddTHH:mm:ss.SSSZ
        String now = java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(java.time.LocalDateTime.now());
        VariableConfigs variableConfigs = VariableConfigs.builder().add(
                VariableConfig.keyValue("TODAY_STATIC", now),
                VariableConfig.json().name("TODAY_HTTP")
                        // Returns a JSON object, where the member currentDateTime containst he date string.
                        .source(SourceConfig.http("http://worldclockapi.com/api/json/est/now")).build(),
                VariableConfig.script().name("TODAY_JAVASCRIPT")
                        // Use javascript date function to calculate NY timezone ISO date format. DATE contains the actual date string.
                        .script("var DATE = new Date(new Date().getTime()-(new Date().getTimezoneOffset()*60*1000)).toISOString();").build(),
                VariableConfig.sql().name("TODAY_SQL")
                        // sqlserver date
                        .hbdbid("vpm-full-production").sql("select CONVERT(char(30),GETDATE(), 126)").build()
        ).build();
        String myString = "Today can be expressed statically as: ${TODAY_STATIC}, or via an HTTP call: ${TODAY_HTTP.currentDateTime} or as a straight Java call: ${TODAY_JAVASCRIPT.DATE} or as a SQL Call: ${TODAY_SQL} ";

        log.info("JSON Config: \n" + MarshalUtils.toJSON(variableConfigs));
        log.info ("Original string: \n" + myString);
        VariableContext context = VariableEngine.createVariableContext(variableConfigs);
        String result = VariableEngine.doVariableSubstitutions(myString, context);
        log.info("Transformed result: \n" + result);

        // Get the expected time within the 10 minute mark.
        int index = 15;
        String expected = java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(java.time.LocalDateTime.now()).substring(0, index);
        String [][] testStrings = {
                {"Static Key/Value definition. ", "${TODAY_STATIC}"},
                // The json member currentDateTime contains the actual date.
                {"HTTP json definition. ", "${TODAY_HTTP.currentDateTime}"},
                // The javascript variable DATE contains the date string.
                {"Javascript definition. ", "${TODAY_JAVASCRIPT.DATE}"},
                {"SQL definition. ", "${TODAY_SQL}"}
        };
        for (String test[] : testStrings) {
            String substituted = VariableEngine.doVariableSubstitutions(test[1], context);
            String actual = substituted.substring(0, substituted.length() < index ? substituted.length() : index);
            Assert.assertEquals(test[0], expected, actual);
        }
    }

     void compareStringsMulti(String prefix, String testStrings [][], VariableContext variableContext) {
         for (int i=0; i<testStrings.length; i++) {
             String test[] = testStrings[i];
             String out = VariableEngine.doVariableSubstitutionsMulti(test[1], variableContext).toString();
             Assert.assertEquals(prefix + " ["+test[0]+"] " + test[1], test[2], out);
         }
     }

     void  compareStrings (String prefix, String testStrings [][], VariableContext variableContext) {
        for (int i=0; i<testStrings.length; i++) {
            String test[] = testStrings[i];
            String out = VariableEngine.doVariableSubstitutions(test[1], variableContext);
            Assert.assertEquals(prefix + " ["+test[0]+"] " + test[1], test[2], out);
        }
    }

    void validateMarshalling (TDConfig config, VariableConfigs variables) throws IOException  {
        if (config != null) {
            String jsonConfig = MarshalUtils.toJSON(config);
            TDConfig actual = MarshalUtils.readValue(new StringReader(jsonConfig), TDConfig.class, MarshalUtils.APPLICATION_JSON);
            Assert.assertEquals("JSON marshalling and unmarshalling of TDConfig matches.", config, actual);
        }
        if (variables != null) {
            String jsonVariables = MarshalUtils.toJSON(variables);
            VariableConfigs actualVars = MarshalUtils.readValue(new StringReader(jsonVariables), VariableConfigs.class, MarshalUtils.APPLICATION_JSON);
            Assert.assertEquals("JSON marshalling and unmarshalling of VariableConfigs matches.", variables, actualVars);
        }
    }
}
