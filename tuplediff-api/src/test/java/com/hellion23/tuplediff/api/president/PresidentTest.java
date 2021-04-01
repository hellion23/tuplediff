package com.hellion23.tuplediff.api.president;


import com.hellion23.tuplediff.api.CompareEventValidator;
import com.hellion23.tuplediff.api.TDCompare;
import com.hellion23.tuplediff.api.TupleDiff;
import com.hellion23.tuplediff.api.TupleDiffContext;
import com.hellion23.tuplediff.api.config.*;
import com.hellion23.tuplediff.api.handler.CascadingEventHandler;
import com.hellion23.tuplediff.api.handler.ConsoleEventHandler;
import com.hellion23.tuplediff.api.handler.StatsEventHandler;
import com.hellion23.tuplediff.api.stream.source.StreamSource;
import com.hellion23.tuplediff.api.variable.VariableContext;
import com.hellion23.tuplediff.api.variable.VariableEngine;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This test suite sets up multiple ways we can validate the Streams. The Left and Right streams always break and match
 * the same way. However the data itself can be represented in many different mediums, Java Beans, CSV files, JSON
 * files, JSON from Http, JSON from Http stitched from joined files, etc...
 *
 * See also PresidentMocks class.
 */
public interface PresidentTest {

    String presidentPrimaryKey = "PRESIDENT_ID";
    String presidentLeftTbl = "PRESIDENT_LEFT";
    String presidentRightTbl = "PRESIDENT_RIGHT";
    String presidentsLeftCsv = "presidents_left.csv";
    String presidentsLeftCamelCaseCsv = "presidents_left_camel_case.csv";
    String presidentsRightCsv = "presidents_right.csv";
    String presidentsRightCamelCaseCsv = "presidents_right_camel_case.csv";
    String presidentsLeftJson = "presidents_left.json";
    String presidentsRightJson = "presidents_right.json";
    String presidentLeftIds = "president_ids_left.json";
    String presidentRightIds = "president_ids_right.json";
    String presidentEmptyJson = "president_empty.json";
    String presidentsValidator = "presidents_validator.json";

    Map<String, String> filePaths = Arrays.asList(
        new File(PresidentTest.class.getResource(presidentsLeftCsv).getPath()).getParentFile()
        .listFiles()).stream().filter(f -> !f.isDirectory() && !f.getName().endsWith(".class"))
        .collect(Collectors.toMap(File::getName, File::getPath));

    // Define CSV File stream configurations:
    CSVStreamConfig leftPresidentCSVStreamConfig = StreamConfig.csvFile(path(presidentsLeftCsv));
    CSVStreamConfig rightPresidentCSVStreamConfig = StreamConfig.csvFile(path(presidentsRightCsv));
    // Same as above, except the column names are camel Cased instead of underscored.
    CSVStreamConfig leftPresidentCamelCaseCSVStreamConfig = StreamConfig.csvFile(path(presidentsLeftCamelCaseCsv));
    CSVStreamConfig rightPresidentCamelCaseCSVStreamConfig = StreamConfig.csvFile(path(presidentsRightCamelCaseCsv));

    // Define JSON File stream configurations:
    JSONStreamConfig.JSONStreamConfigBuilder jsonBuilder = StreamConfig.json().pathToTuples("/presidents");
    JSONStreamConfig leftPresidentJSONFileStreamConfig = jsonBuilder.source(SourceConfig.file(path(presidentsLeftJson))).build();
    JSONStreamConfig rightPresidentJSONFileStreamConfig = jsonBuilder.source(SourceConfig.file(path(presidentsRightJson))).build();

    // Define JSON Http stream configuration:
    // This variable is declared using a mock Http Source. The real way to create an HTTP source is:
    // SourceConfig httpSourceConfig = SourceConfig.http("http://my.website.com/path/to/file.json");
    SourceConfig leftPresidentJSONHttpSource = PresidentMocks.httpSourceConfigOfFile(presidentsLeftJson);
    SourceConfig rightPresidentJSONHttpSource = PresidentMocks.httpSourceConfigOfFile(presidentsRightJson);
    JSONStreamConfig leftPresidentJSONHttpStreamConfig = jsonBuilder.source(leftPresidentJSONHttpSource).build();
    JSONStreamConfig rightPresidentJSONHttpStreamConfig = jsonBuilder.source(rightPresidentJSONHttpSource).build();

    // Variable Configuration
    // Define what the LEFT_IDS and RIGHT_IDS are (these are the primary key ID's of the Presidents)
    VariableConfigs variableConfigs = VariableConfigs.builder().add(
        // This variable gets all PRESIDENT_ID's using sql query.
        VariableConfig.sql()
            .name("LEFT_IDS")
            // actualDataSource is how we define the datasource @ Runtime, this is NOT serializable back into a
            // configuration. Don't use this unless you never want to marshal back this into configuration xml. This is
            // only to support testing.
            .actualDataSource(PresidentMocks.dataSource)
            .sql("select " + presidentPrimaryKey + " from " + presidentLeftTbl)
            .build()
        ,
        // This variable gets all PRESIDENT_ID's using an HTTP call (that returns a JSON array of PRESIDENT_ID's).
        VariableConfig.json()
            .name("RIGHT_IDS")
            .source(PresidentMocks.httpSourceConfigOfFile(presidentRightIds))
            .build()
    ).build();

    // This variable is declared using a mock Http Source. The real way to create an HTTP source looks like this:
    // SourceConfig httpSourceConfig = SourceConfig.http("http://my.website.com/path/to/file.json");
    // Note that the option ";JOIN=FALSE" forces the VariableEngine to create multiple Strings instead of joining
    // a collection into a single String.
    SourceConfig leftPresidentCompSrc  = PresidentMocks.httpSourceConfigOfFile("president_${LEFT_IDS;JOIN=FALSE}.json");
    SourceConfig rightPresidentCompSrc = PresidentMocks.httpSourceConfigOfFile("president_${RIGHT_IDS;JOIN=FALSE}.json");
    JSONStreamConfig leftPresidentCompositeJSONStreamConfig = StreamConfig.json().source(leftPresidentCompSrc).build();
    JSONStreamConfig rightPresidentCompositeJSONStreamConfig = StreamConfig.json().source(rightPresidentCompSrc).build();

    // DBStream definitions.
    // This DBStreamConfig is created using Mock DataSource. Real way to create is:
    // DBStreamConfig dbStreamConfig = StreamConfig.sql("dwv2-production","select * from DIM_SECURITY");
    DBStreamConfig leftPresidentDBStreamConfig = PresidentMocks.dbStreamConfig("select * from " + presidentLeftTbl);
    DBStreamConfig rightPresidentDBStreamConfig = PresidentMocks.dbStreamConfig("select * from " + presidentRightTbl);

    default String presidentJson(int id) {
        return "president_"+id+".json";
    }

    /**
     * Constructs a bean stream configuration of Presidents
     * @param isLeft
     * @return
     */
    default BeanStreamConfig presidentBeansConfig(boolean isLeft) {
        return  StreamConfig.bean().beanClass(President.class)
            .sortedByPrimaryKey(false)
            .iterator(new PresidentIterator(isLeft ? leftPresidentJSONFileStreamConfig : rightPresidentJSONFileStreamConfig))
            .build();
    }

    static String path (String file) {
        return filePaths.get(file);
        //return PresidentTest.class.getResource(file).getPath();
    }

    default String pathOf (String file) {
        return path(file);
    }

    default String openAndReadIntoString (StreamSource streamSource) throws IOException {
        streamSource.open();
        BufferedReader bufferedReader = new BufferedReader(streamSource.getReader());
        return bufferedReader.lines().collect(Collectors.joining("\n"));
    }

    default void compareAndValidate(TDConfig config, String fileName) throws IOException {
        compareAndValidate(config, CompareEventValidator.fromFile(config.getName(), path(fileName)) );
    }

    default void compareAndValidate (TDConfig config, CompareEventValidator validator ) {
        compareAndValidate (config, null, validator);
    }

    default void compareAndValidate (TDConfig config, VariableConfigs varCfgs, CompareEventValidator validator ) {
        TupleDiffContext context = new TupleDiffContext();
        VariableConfigs variableConfigs = varCfgs == null ? this.variableConfigs : varCfgs;
        VariableContext variableContext = VariableEngine.createVariableContext(context, variableConfigs);
        context.setVariableContext(variableContext);
        context.setEventHandler( new CascadingEventHandler(
                validator,                                      // validate results.
                new StatsEventHandler(true),      // collect stats
                new ConsoleEventHandler())                      // print to console);
        );
        TDCompare compare = TupleDiff.configure(config, context);
        compare.compare();
    }
}
