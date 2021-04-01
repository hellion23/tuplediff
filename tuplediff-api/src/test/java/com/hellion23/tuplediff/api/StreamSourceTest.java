package com.hellion23.tuplediff.api;

import com.hellion23.tuplediff.api.config.*;
import com.hellion23.tuplediff.api.handler.ConsoleEventHandler;
import com.hellion23.tuplediff.api.president.PresidentMocks;
import com.hellion23.tuplediff.api.president.PresidentTest;
import com.hellion23.tuplediff.api.stream.json.JSONStreamParser;
import com.hellion23.tuplediff.api.stream.source.CompositeFileStreamSource;
import com.hellion23.tuplediff.api.stream.source.FileStreamSource;
import com.hellion23.tuplediff.api.stream.source.StreamSource;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class StreamSourceTest implements PresidentTest {
    static final String nonExistentFile = "C:/a/non-existent/file.txt";

    /**
     * Added a new feature where you can define your own Order By clause. The OrderBy columns *Must* match
     * the column label exactly.
     */
    @Test
    public void testOrderByClause () {
        String inClause = " in (\n" +
                "1, -- USA break on Country_DESC\n" +
                "2, -- JAPAN match!\n" +
                "10000046, -- Niger ONLY LEFT\n" +
                "10000305 -- ASIA ONLY_RIGHT\n" +
                ")";
        String vpmCountrySql = "select convert(varchar(20), CtryId) country_Id, CtryCode countryCd, upper(c.CtryDesc) countryDesc  " +
                "from vpm..country c where c.CtryId " + inClause;
        String dwCountrySql = "select to_char (vpm_country_id) country_id, country_cd, country_desc " +
                "from dim_country where vpm_country_id " + inClause;
        CompareEventValidator validator = new CompareEventValidator();
        validator.validateOnlyLeft(10000046);
        validator.validateOnlyRight(10000305);
        validator.validateBreak(Arrays.asList("COUNTRY_DESC"), 1);

        TDConfig.Builder config = TDConfig.builder()
                .left(StreamConfig.sql("dwv2-production", dwCountrySql))
                .right(StreamConfig.sql("vpm-full-production", vpmCountrySql))
                .primaryKey("country_id");

        //Standard comparison:
        TupleDiff.configure(config.build(), validator, new ConsoleEventHandler()).compare();

        // Okay, let's modify the config so that we change the primary key to camel casing and change the order by:
        config.right(StreamConfig.sql().datasource(DataSourceConfig.builder().hbdbid("vpm-full-production").build())
                .sql(vpmCountrySql.replace("country_Id", "countryId"))
                .orderBy(new String[] {"countryId"})
                .build());
        TupleDiff.configure(config.build(), validator, new ConsoleEventHandler()).compare();

        // Let's change the primary key too!!!
        config.primaryKey("country_id");
        TupleDiff.configure(config.build(), validator, new ConsoleEventHandler()).compare();

    }

    @Test
    public void testCompositeFileStreamSource () throws Exception {
        List<String> files = new ArrayList(Arrays.asList(
            pathOf(presidentEmptyJson),
            pathOf(presidentJson(1)),
            pathOf(presidentJson(2))
        ));
        validateStreams(files, "Basic Test");
        files.add(0, nonExistentFile);
        validateStreams(files, "Non existent file in the beginning");

        files.add(nonExistentFile);
        validateStreams(files, "Non existent file in the end");

        files.add(2, nonExistentFile);
        validateStreams(files, "Non existent file in the end");

    }

    /**
     * This is an actual webservice call hit.
     * @throws IOException
     */
    @Test
    public void testHttpStreamSource ()  throws IOException {
        // The eze webservice allows the qa/qa user/pass only to the qa site, but not the production site
        String successURL = "https://as-ezewebservice-qa.nyc.hcmny.com:8443/trades/?manager=EQA";
        String failUrl = "https://as-ezewebservice.nyc.hcmny.com:8443/trades/?manager=EQA";
        String user = "qa";
        String pass = "qa";

        // Failed Http access
        HttpSourceConfig config = HttpSourceConfig.builder()
                .auth(HttpSourceConfig.Auth.basicUserAndPass(user, pass))
                .url(failUrl)
                .build();
        StreamSource source = TupleDiff.constructStreamSource(config, new TupleDiffContext());
        try {
            source.open();
            Assert.fail("Expected an authentication failure.");
        }
        catch(IOException ioe) {}

        // Successful Http access.
        config =  HttpSourceConfig.builder()
                .auth(HttpSourceConfig.Auth.basicUserAndPass(user, pass))
                .url(successURL)
                .build();
        source = TupleDiff.constructStreamSource(config, new TupleDiffContext());
        source.open();
        BufferedReader reader = new BufferedReader(source.getReader());
        reader.lines().forEach(log::info);
    }

    /**
     * We will mock all http client calls.
     * @throws Exception
     */
    @Test
    public void testMockHttpClient () throws Exception {
        TupleDiffContext tupleDiffContext = new TupleDiffContext();
        int id = 1;
        SourceConfig config = PresidentMocks.httpSourceConfigOfFile(presidentJson(id));
        StreamSource streamSource = TupleDiff.constructStreamSource(config, tupleDiffContext);
        streamSource.open();
        String actual = new BufferedReader (streamSource.getReader()).lines().collect(Collectors.joining("\n"));
        log.info("Output str " + actual);
        String expected = PresidentMocks.fileContent(pathOf(presidentJson(id)));
        Assert.assertEquals("Mock Http Client returned expected data: ",expected, actual);
    }

    /**
     * This validates that the JSONStreamParser can process
     * @throws Exception
     */
    @Test
    public void testJsonParserWithArrayOfNonObjects () throws Exception {
        TupleDiffContext tupleDiffContext = new TupleDiffContext();
        SourceConfig config = PresidentMocks.httpSourceConfigOfFile(presidentLeftIds);
        StreamSource streamSource = TupleDiff.constructStreamSource(config, tupleDiffContext);
        JSONStreamParser parser = new JSONStreamParser(streamSource, null);
        parser.open();
        List<Comparable> values = new LinkedList<>();
        while (parser.hasNext()) {
            values.add(parser.next().getComparable());
        }
        log.info("Values: " + values);
        String expected = "[1, 2, 3, 4, 5, 7]";
        String actual = values.toString();
        Assert.assertEquals("Correctly read in array of numbers: ", expected, actual);
    }

    static void validateStreams (List<String> files, String msg ) throws IOException {
        Assert.assertEquals(msg, readIntoLine(files), new FileConsumer(files).read());
    }

    static String readIntoLine (List<String> files) throws IOException {
        StringBuilder sb = new StringBuilder();
        for(String f : files) {
            File file = new File(f);
            if (file.exists()) {
                FileStreamSource fs = new FileStreamSource(file);
                fs.open();
                new BufferedReader(fs.getReader()).lines().forEach(l -> sb.append(l));
            }
        }
        return sb.toString();
    }

    static class FileConsumer  {
        CompositeFileStreamSource fs;
        StringBuilder sb = new StringBuilder();

        FileConsumer (List<String> files) {
            this.fs = new CompositeFileStreamSource(files.iterator());
        }

        public String read () throws IOException  {
            this.fs.open();
            new BufferedReader(this.fs.getReader()).lines().forEach(l -> sb.append(l));
            return sb.toString();
        }
    }

}
