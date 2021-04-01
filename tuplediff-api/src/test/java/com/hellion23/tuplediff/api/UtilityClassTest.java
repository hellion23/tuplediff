package com.hellion23.tuplediff.api;

import com.hellion23.tuplediff.api.compare.ComparableLinkedHashMap;
import com.hellion23.tuplediff.api.compare.ComparableLinkedList;
import com.hellion23.tuplediff.api.format.TypeFormatterLibrary;
import com.hellion23.tuplediff.api.stream.sql.DataSourceProviders;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.function.Supplier;

import static junit.framework.TestCase.assertTrue;

@Slf4j
public class UtilityClassTest {

    /**
     * The TypeFormatterLibrary.LocalDateTimeFormatter does smart parsing of the most common date formats. Need to test
     * that this implementation works properly.
     *
     */
    @Test
    public void testDateTimeFormat() {
        // This is just to make tests easier.
        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss.SSS");

        // testStrings[][] index meanings:
        int message=0, toParse=1, expected=2;
        String testStrings[][] = {
            {"EXCEL Short Date",            "2-JAN-19",                 "01/02/2019 00:00:00.000"},
            {"EXCEL Short Date, 0 padding", "02-JAN-19",                "01/02/2019 00:00:00.000"},
            {"Short m/d/y date",            "1/3/2019",                 "01/03/2019 00:00:00.000"},
            {"Short m/d/y date, 0 pad",     "01/03/2019",               "01/03/2019 00:00:00.000"},
            {"DateTime m/d/y date",         "1/3/2019 01:02:03.456",    "01/03/2019 01:02:03.456"},
            {"DateTime m/d/y date, 0 pad",  "01/03/2019 01:02:03.456",  "01/03/2019 01:02:03.456"},
            {"DateTime SQLServer",          "2019-01-04 15:20:54.000",  "01/04/2019 15:20:54.000"},
            {"Timestamp Oracle",            "30-APR-19 06.31.40.000000000 PM", "04/30/2019 18:31:40.000"},
            {"Timestamp Oracle, w/ nanos",  "3-APR-19 06.31.40.123456789 PM", "04/03/2019 18:31:40.123"},
        };

        for (String test[] : testStrings) {
            TypeFormatterLibrary.LocalDateTimeFormatter dtf = new TypeFormatterLibrary.LocalDateTimeFormatter();
            LocalDateTime ldt = dtf.apply(test[toParse]);
            String actual = fmt.format(ldt);
            log.info(String.format("For test string %s, chosen pattern is: %s", test[toParse], dtf.getPattern()));
            Assert.assertEquals(test[message], test[expected], actual);
        }
    }

    /**
     * Switching over from HBDbDatabase. Test that we can still create dataSources.
     * @throws Exception
     */
    @Test
    public void testHikariDB() throws Exception {
        // Oracle test:
        Supplier<DataSource> dss = DataSourceProviders.getDb("dw-production");
        DataSource ds = dss.get();
        // Make sure we can get an actual connection.
        ds.getConnection().close();
        // Now run a simple query.
        JdbcTemplate jdbcTemplate = new JdbcTemplate(ds);
        jdbcTemplate.execute("select * from DIM_DATE where DATE_ID = 20190430");
        Supplier<DataSource> dss2 = DataSourceProviders.getDb("dw-production.properties");
        Assert.assertEquals("Verifying the dss is the same: " ,dss ,dss2);

        // SQLServer test:
        JdbcTemplate jdbcTemplateVPM = new JdbcTemplate(DataSourceProviders.getDb ("vpm-development").get());
        jdbcTemplateVPM.execute("select * from vpm..cu where cucode = 'USD'");
    }

    @Test
    public void testComparableLinkedList () {
        ComparableLinkedList<Integer> x = new ComparableLinkedList<>(Arrays.asList(1,2, 3, 4));
        ComparableLinkedList<Integer> y = new ComparableLinkedList<>(Arrays.asList(1,2, 3, 4));
        ComparableLinkedList<Integer> z = new ComparableLinkedList<>(Arrays.asList(1,2, 3));
        ComparableLinkedList<Integer> a = new ComparableLinkedList<>(Arrays.asList(1,2, 4, 3));
        Comparator<ComparableLinkedList> comparator = ComparableLinkedList.COMPARATOR;
        assertTrue("Same content list is equal", comparator.compare(x, y) == 0);
        assertTrue("Different length list is different", comparator.compare(x, z) != 0);
        assertTrue("Different Ordered list is different", comparator.compare(x, a) != 0);
    }

    @Test
    public void testComparableHashMap () {
        Comparator<ComparableLinkedHashMap> comparator = ComparableLinkedHashMap.COMPARATOR;

        ComparableLinkedHashMap<String, Integer> a = new ComparableLinkedHashMap<>();
        a.put("A", 1); a.put("B", 2); a.put("C", 3);

        ComparableLinkedHashMap<String, Integer> b = new ComparableLinkedHashMap<>();
        b.put("C", 3); b.put("A", 1); b.put("B", 2);

        ComparableLinkedHashMap<String, Integer> c = new ComparableLinkedHashMap<>();
        c.put("A", 1); c.put("B", 2); c.put("C", 3); c.put("D", 4);

        ComparableLinkedHashMap<String, Integer> d = new ComparableLinkedHashMap<>();
        d.put("C", 3); d.put("D", 4); d.put("B", 2);

        assertTrue("Same contents, different order ", comparator.compare(a, b) != 0);
        assertTrue("Different length. Contents not the same. ", comparator.compare(a, c) != 0);
        assertTrue("Different content ", comparator.compare(a, d) != 0);


    }

    /**
     * This test doesn't work (yet), until the name issue is fixe.d
    @Test
    public void testVariableConversion () {
        CaseFormat normed = CaseFormat.UPPER_UNDERSCORE;
        CaseFormat java = CaseFormat.LOWER_CAMEL;
        CaseFormat db = CaseFormat.LOWER_UNDERSCORE;
        CaseFormat unknown = CaseFormat.LOWER_CAMEL;

        Assert.assertEquals("basic", "MIXED_CASE_TEST", java.to(normed, "mixedCaseTest"));
        Assert.assertEquals("With Letters", "ISO_A3_CODE", java.to(normed, "isoA3Code"));
        Assert.assertEquals("With Single Letters", "THIS_IS_A_TEST", java.to(normed, "thisIsATest"));

        // Okay now test db Strings with lower cases, even though it should be upper.
        Assert.assertEquals("no casing", "COUNTRYID", db.to(normed, "countryid"));
        Assert.assertEquals("lower case camel", "COUNTRY_ID", db.to(normed, "country_id"));

        // Now do unknown transformations:
        Assert.assertEquals("unknown camel", "COUNTRY_ID", unknown.to(normed, "countryId"));
        Assert.assertEquals("unknown snake", "COUNTRY_ID", unknown.to(normed, "country_id"));


        // Okay, now test that the TDUtils method actually does the transformation w/o guessing.
        Assert.assertEquals("TDUtils: lower underscore", "COUNTRY_ID", TDUtils.normalizeColumnName("country_id"));
        Assert.assertEquals("TDUtils: upper underscore", "COUNTRY_ID", TDUtils.normalizeColumnName("COUNTRY_ID"));
        Assert.assertEquals("TDUtils: lower camel", "COUNTRY_ID", TDUtils.normalizeColumnName("countryId"));
    }
    **/
}
