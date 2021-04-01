package com.hellion23.tuplediff.api;

import com.hellion23.tuplediff.api.config.*;
import com.hellion23.tuplediff.api.handler.CascadingEventHandler;
import com.hellion23.tuplediff.api.handler.CompareEventHandler;
import com.hellion23.tuplediff.api.handler.ConsoleEventHandler;
import com.hellion23.tuplediff.api.handler.StatsEventHandler;
import com.hellion23.tuplediff.api.model.TDStream;
import com.hellion23.tuplediff.api.model.TDTuple;
import com.hellion23.tuplediff.api.president.PresidentMocks;
import com.hellion23.tuplediff.api.president.PresidentTest;
import com.hellion23.tuplediff.api.stream.bean.BeanTDStream;
import com.hellion23.tuplediff.api.stream.bean.BeanTDStreamMetaData;
import com.hellion23.tuplediff.api.stream.json.JSONMember;
import com.hellion23.tuplediff.api.stream.json.JSONStreamParser;
import com.hellion23.tuplediff.api.stream.json.JSONTDStream;
import com.hellion23.tuplediff.api.stream.json.JSONType;
import com.hellion23.tuplediff.api.stream.source.CompositeFileStreamSource;
import com.hellion23.tuplediff.api.stream.source.FileStreamSource;
import com.hellion23.tuplediff.api.variable.VariableContext;
import com.hellion23.tuplediff.api.variable.VariableEngine;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;


import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This suite of tests validates the Stream objects have been implemented correctly.
 */
@Slf4j
public class StreamTest implements PresidentTest {

    static List<BasicBean> bunchOfBeans = new LinkedList<>();

    static {
        bunchOfBeans.add(new BasicBean(1, "1", LocalDateTime.now(), null));
        bunchOfBeans.add(new BasicBean(2, "2", LocalDateTime.now().plusMinutes(1), new Integer [] {1, 2,3}));
        bunchOfBeans.add(new BasicBean(3, "3", LocalDateTime.now().plusMinutes(2), new Integer [] {4, 4, 6}));
        bunchOfBeans.add(new BasicBean(4, "4", LocalDateTime.now().plusMinutes(3), new Integer [] {6, 6, 8}));
        bunchOfBeans.add(new BasicBean(5, "5", LocalDateTime.now().plusMinutes(4), new Integer [] {8, 9, 8}));
    }

    static final String sampleJSONPath = StreamTest.class.getResource("sample.json").getPath();

    /**
     * Validate the DBStream mocking works as expected.
     * @throws Exception
     */
    @Test
    public void testMockDBStream() throws Exception {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(PresidentMocks.dataSource);
        Set actualTbls = jdbcTemplate.queryForList("SHOW TABLES; ")
                .stream().map(m -> m.get("TABLE_NAME")).collect(Collectors.toSet());
        log.info("Tables: " + actualTbls);
        Assert.assertTrue("Left Table created ", actualTbls.contains(presidentLeftTbl));
        Assert.assertTrue("Right Table created ", actualTbls.contains(presidentRightTbl));


        Assert.assertTrue("ROW count matched for LEFT Table: ",
                jdbcTemplate.queryForObject("select count(1) FROM " + presidentLeftTbl, Integer.class) == 6);
        Assert.assertTrue("ROW count matched for RIGHT Table: ",
                jdbcTemplate.queryForObject("select count(1) FROM " + presidentRightTbl, Integer.class) == 6);

//        RowMapper <President> presidentRowMapper = new BeanPropertyRowMapper(President.class);
//        List <President> leftPresidents = jdbcTemplate.query("select * FROM " + presidentLeftTbl(), presidentRowMapper);
//        log.info("LEFT PRESIDENTS: " + leftPresidents);
    }

    /**
     * Read the entire file as a single object.
     * @throws IOException
     */
    @Test
    public void testParsingJsonObject() throws IOException {
        String pathToTuples = "";
        JSONStreamParser parser = new JSONStreamParser(new FileStreamSource(sampleJSONPath), pathToTuples);
        parser.open();
        int count = 0;
        JSONMember member = null;
        while (parser.hasNext()) {
            count ++;
            member = parser.next();
            log.info("Got Object: " + member);
        }
        Assert.assertEquals("Match expected JSON count", 1, count);
        Assert.assertTrue("JSON object retrieved non null", member != null);
        Assert.assertEquals("JSON Object is member of an Array ", JSONType.ARRAY, member.getMemberOf());

    }

    /**
     * Read the array portion of the sample file. Also tests fastforwarding to a location.
     * @throws IOException
     */
    @Test
    public void testParsingJsonArray() throws IOException {
        String pathToTuples = "/dataset/persons";
        JSONStreamParser parser = new JSONStreamParser(new FileStreamSource(sampleJSONPath), pathToTuples);
        parser.open();
        int count = 0;
        JSONMember member = null;
        while (parser.hasNext()) {
            count ++;
            member = parser.next();
            log.info("Got Array Object: " + member);
        }
        Assert.assertEquals("Match expected JSON Array count", 3, count);
    }

    /**
     * Validates that the JSONStreamParser can read composite Sources (i.e. read each source sequentially until
     * the last source has been consumed. The Parser needs to be "smart" enough to know that the end of a file has
     * been reached (i.e. that a complete JSON Object or Array has been read (The parser cannot read non Object
     * or non-Array JSON objects.
     *
     * @throws IOException
     */
    @Test
    public void testJSONCompositeStreamParser () throws IOException {
        List<String> leftFiles = Arrays.asList(
                pathOf(presidentEmptyJson),
                pathOf(presidentJson(1)),
                pathOf(presidentJson(2)),
                pathOf(presidentEmptyJson),
                pathOf(presidentJson(4)),
                pathOf(presidentJson(5)),
                pathOf(presidentJson(7)));
        CompositeFileStreamSource leftFileSource = new CompositeFileStreamSource(leftFiles.iterator());
        JSONStreamParser parser = new JSONStreamParser(leftFileSource, null);
        JSONMember member;
        int count = 0;
        parser.open();
        while (parser.hasNext()) {
            count ++;
            member = parser.next();
            log.info("Got Object: " + member);
        }
        Assert.assertEquals("Composite Stream Parser count", 5, count);
    }

    /**
     * Validates that a Stream using a Composite source can read/use the
     * @throws Exception
     */
    @Test
    public void testCompositeJSONSourceStream () throws Exception {
        List<String> primaryKey = Arrays.asList(presidentPrimaryKey);
        List<String> leftFiles = Arrays.asList(
                pathOf(presidentEmptyJson),
                pathOf(presidentJson(1)),
                pathOf(presidentJson(2)),
                pathOf(presidentEmptyJson),
                pathOf(presidentJson(4)),
                pathOf(presidentJson(5)),
                pathOf(presidentJson(7)));
        CompositeFileStreamSource leftFileSource = new CompositeFileStreamSource(leftFiles.iterator());

        List<String> rightFiles = Arrays.asList(
                pathOf(presidentJson(1)),
                pathOf(presidentJson(2)),
                pathOf(presidentJson(3)),
                pathOf(presidentJson(4)),
                pathOf(presidentEmptyJson),
                pathOf(presidentJson(6)),
                pathOf(presidentEmptyJson)
        );
        CompositeFileStreamSource rightFileSource = new CompositeFileStreamSource(rightFiles.iterator());

        StatsEventHandler seh = new StatsEventHandler();

        CompareEventValidator cev = new CompareEventValidator();
        cev.validateOnlyRight(3);
        cev.validateOnlyLeft(5);
        cev.validateOnlyRight(6);
        cev.validateOnlyLeft(7);

        TDStream left = new JSONTDStream("LEFT", leftFileSource, primaryKey, false, null, null);
        TDStream right = new JSONTDStream("RIGHT", rightFileSource, primaryKey, false, null, null);
        CascadingEventHandler eh = new CascadingEventHandler(new ConsoleEventHandler(), seh, cev);

        TDCompare comparison = new TDCompare ( primaryKey, left, right, eh);
        comparison.compare();
        log.info("Stats: " + seh);

    }

    @Test
    public void testCompositeJSONStreamSeededByVariable() throws IOException {
        // This variable already exists in the PresidentsTest class. Repeated here for clarity:
        VariableConfigs variableConfigs = VariableConfigs.builder().add(
            VariableConfig.sql()
                .name("LEFT_IDS")
                .actualDataSource(PresidentMocks.dataSource)
                .sql("select " + presidentPrimaryKey + " from " + presidentLeftTbl)
                .build()
            ,
            VariableConfig.json()
                .name("RIGHT_IDS")
                .source(PresidentMocks.httpSourceConfigOfFile(presidentRightIds))
                .build()
        ).build();

        JSONStreamConfig left = StreamConfig.json()
                .source(PresidentMocks.httpSourceConfigOfFile("president_${LEFT_IDS;JOIN=FALSE}.json"))
                .build();
        JSONStreamConfig right = StreamConfig.json()
                .source(PresidentMocks.httpSourceConfigOfFile("president_${RIGHT_IDS;JOIN=FALSE}.json"))
                .build();

        TDConfig config = TDConfig.builder()
            .primaryKey("PRESIDENT_ID")
            .left(left)
            .right(right)
            .build();

        // Setup TupleDiffContext w/ validation handler and VariableContext.
        TupleDiffContext tupleDiffContext = new TupleDiffContext();
        CompareEventHandler ceh = new CascadingEventHandler(
            CompareEventValidator.fromFile(pathOf(presidentsValidator)),   // JUnit validate results.
            new ConsoleEventHandler(),                                    // write results to screen.
            new StatsEventHandler(true)                   // Collect and print stats
        );

        VariableContext variableContext = VariableEngine.createVariableContext(tupleDiffContext, variableConfigs);
        tupleDiffContext.setVariableContext(variableContext);
        tupleDiffContext.setEventHandler(ceh);

        TDCompare comparison = TupleDiff.configure(config, tupleDiffContext);
        comparison.compare();
    }

    @Test
    public void testBasicBeans () {
        // Use the BasicBean class to test Bean streams.
        BeanTDStream<BasicBean> myBeans = new BeanTDStream("testBasicBeans", BasicBean.class, bunchOfBeans.iterator());
        BeanTDStreamMetaData md = myBeans.getMetaData();
        log.info("Column Names; " + md.getColumnNames());
        Assert.assertEquals("ColumnNames transformed correctly",
                Arrays.asList("DATE", "ID", "NUMARRAY", "STRING"),
                md.getColumnNames());
        List<Integer> ids = new LinkedList<>();
        while(myBeans.hasNext()) {
            TDTuple next = myBeans.next();
            log.info("Got next Tuple: " + next);
            ids.add((Integer)next.getValue("ID"));
        }

        Assert.assertEquals("Tuple count is as expected: ", Arrays.asList(1, 2, 3, 4, 5), ids);

        List<BasicBean> leftBeans = new LinkedList<>(bunchOfBeans);
        leftBeans.remove(2); // Remove item 3.
        BasicBean toAlter = leftBeans.get(1).clone();
        toAlter.setDate(LocalDateTime.now().plusMonths(1));
        leftBeans.set(1, toAlter); // Alter ID 2 to create a break

        List<BasicBean> rightBeans = new LinkedList<>(bunchOfBeans);
        rightBeans.remove(3); // Remove item 4.
        toAlter = leftBeans.get(0).clone();
        toAlter.setNumArray(new Integer[]{8, 9, 10});
        rightBeans.set(0, toAlter); // Alter ID 1 to to create a break.

        BeanStreamConfig.BeanStreamConfigBuilder beanStream = StreamConfig.bean()         // Bean stream configuration.
                .beanClass(BasicBean.class) // The class for which you are tupleDiffing. This will automatically
                                            // pull needed data
                .sortedByPrimaryKey(true);  // If Stream is already sorted by primary key, then will skip doing
                                            // an in memory sort.

        TDConfig config = TDConfig.builder()
                .left(  beanStream.iterator(leftBeans.iterator()   ).build())
                .right( beanStream.iterator(rightBeans.iterator()  ).build())
                .primaryKey("id")
                .build();

        StatsEventHandler seh = new StatsEventHandler();
        TDCompare comparison = TupleDiff.configure(config, new ConsoleEventHandler(), seh);
        comparison.compare();

        Assert.assertArrayEquals("Expected results for tuplediff: ",
            new Long[] {2l, 1l, 4l, 4l, 1l, 1l},
            new Long[] {seh.getNumBreaks(), seh.getNumMatched(), seh.getNumLeft(), seh.getNumRight(), seh.getNumLeftOnly(), seh.getNumRightOnly()}
        );

        log.info("Stats: " + seh);
    }


    /**
     * BasicBean object used for testing.
     */
     static class BasicBean {
        int id;
        String string;
        LocalDateTime date;
        Number numArray[];

        public BasicBean () {}

        public BasicBean(int id, String string, LocalDateTime date, Number[] numArray) {
            this.id = id;
            this.string = string;
            this.date = date;
            this.numArray = numArray;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getString() {
            return string;
        }

        public void setString(String string) {
            this.string = string;
        }

        public LocalDateTime getDate() {
            return date;
        }

        public void setDate(LocalDateTime date) {
            this.date = date;
        }

        public Number[] getNumArray() {
            return numArray;
        }

        public void setNumArray(Number[] numArray) {
            this.numArray = numArray;
        }

        @Override
        public BasicBean clone(){
            return new BasicBean(id, string, date, numArray);
        }
    }
}
