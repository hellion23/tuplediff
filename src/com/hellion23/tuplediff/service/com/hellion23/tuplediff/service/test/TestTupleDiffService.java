package com.hellion23.tuplediff.service.com.hellion23.tuplediff.service.test;

import com.hellion23.tuplediff.api.*;
import com.hellion23.tuplediff.api.db.SqlTupleStream;
import com.hellion23.tuplediff.api.monitor.Nameable;
import com.hellion23.tuplediff.api.monitor.TupleComparisonMonitor;
import com.hellion23.tuplediff.service.TupleDiffService;

import java.util.*;
import java.util.logging.Logger;

/**
 * @author: Hermann Leung
 * Date: 12/16/2014
 */
public class TestTupleDiffService {
    private static final Logger logger = Logger.getLogger(TestTupleDiffService.class.getName());

    String sql = "select * from TUPLES where SIDE=";
    String keys [] = new String [] {"TEST_CASE_NUM", "PRIMARY_KEY_ID"};
    String ignoreFields [] = new String [] {"SIDE"};
    TestDataLoader tdl;

    public void test (String file) throws Exception {
        // load data:
        tdl = new TestDataLoader(file);
        tdl.load();

        // Run individual tests
        testComparisonCorrectness();
    }

    public void testComparisonCorrectness() throws Exception {

        SqlTupleStream leftStream = SqlTupleStream.create(tdl.getConnection(), sql + "'L'", keys);
        leftStream.setExcludeFieldNames(ignoreFields);

        SqlTupleStream rightStream = SqlTupleStream.create(tdl.getConnection(), sql + "'R'", keys);
        rightStream.setExcludeFieldNames(ignoreFields);

        Config config = new Config("testComparisonCorrectness");
        config.setLeftStream(leftStream);
        config.setRightStream(rightStream);

        TestMonitor monitor = new TestMonitor();
        ComparisonResult cr = TupleDiffService.Instance().compare(config, monitor, null);
        logger.info(cr.toString());
        monitor.analyze();
    }

    public static void main (String args[]) throws Exception {
        TestTupleDiffService ttd = new TestTupleDiffService();
        ttd.test(args[0]);
        System.exit(0);
    }

    public static class TestMonitor extends TupleComparisonMonitor {
        Set<Tuple> allData = new HashSet<Tuple>();
        Set<Tuple> leftOnly = new HashSet<Tuple>();
        Set<Tuple> matched = new HashSet<Tuple>();
        Set<Tuple> rightOnly = new HashSet<Tuple>();
        Map<Tuple, CompareEvent> breaks = new HashMap<Tuple,CompareEvent>();

        @Override
        public void reportEvent(Nameable source, String eventName, Object... params) throws TupleDiffException {
            super.reportEvent(source, eventName, params);
            if (!TupleComparison.COMPARE_EVENT.equals(eventName))
                return;
            final CompareEvent.TYPE event =  (CompareEvent.TYPE) params[0];
            Tuple left = (Tuple)params[1];
            Tuple right = (Tuple)params[2];
            List<String> breakFields = (List<String>)params[3];

            switch (event) {
                case DATA_LEFT:
                    allData.add(left);
                    break;
                case DATA_RIGHT:
                    allData.add(right);
                    break;
                case PAIR_MATCHED:
                    matched.add(left);
                    matched.add(right);
                    break;
                case PAIR_BREAK:
                    CompareEvent x = new CompareEvent(event, left, right, breakFields);
                    breaks.put(left, x);
                    breaks.put(right, x);
                    break;
                case LEFT_BREAK:
                    leftOnly.add(left);
                    break;
                case RIGHT_BREAK:
                    rightOnly.add(right);
                    break;
            }
        }

        public void analyze() {
            logger.info("Analyzing... " + allData.size() + " number of tuples.");
            List<Tuple> expectedLeftBreak = new LinkedList<Tuple>();
            List<Tuple> expectedRightBreak = new LinkedList<Tuple>();
            List<Tuple> expectedPairBreak = new LinkedList<Tuple>();
            List<Tuple> expectedBreakFields = new LinkedList<Tuple>();
            List<Tuple> expectedMatch = new LinkedList<Tuple>();

            for (Tuple tuple : allData) {

                final String ev = (String)tuple.getValue("COMPARE_EVENT");
//                logger.info("TUPLE: " + tuple.getAllValues() + " EVENT: <" + ev + ">.");
                CompareEvent.TYPE type = CompareEvent.TYPE.valueOf(ev);

                switch (type) {
                    case LEFT_BREAK:
                        if (!leftOnly.contains(tuple)){
                            expectedLeftBreak.add(tuple);
                            logger.info("Expected LEFT BREAK not found: " + tuple.getAllValues());
                        }
                        break;
                    case RIGHT_BREAK:
                        if (!rightOnly.contains(tuple)){
                            expectedRightBreak.add(tuple);
                            logger.info("Expected RIGHT BREAK not found: " + tuple.getAllValues());
                        }
                        break;
                    case PAIR_MATCHED:
                        if (!matched.contains(tuple)){
                            expectedMatch.add(tuple);
                            logger.info("Expected MATCH not found: " + tuple.getAllValues());
                        }
                        break;
                    case PAIR_BREAK:
                        if (breaks.containsKey(tuple)) {
                            CompareEvent x = breaks.get(tuple);
                            String y = (String)tuple.getValue("BREAK_FIELDS");
                            List<String>expectedFieldBreaks = Arrays.asList(y.split("\\|", -1));
                            if (! (expectedFieldBreaks.containsAll(x.getBreakFields()) && x.getBreakFields().containsAll(expectedFieldBreaks) )) {
                                expectedBreakFields.add(tuple);
                                logger.info("PAIR_BREAK, but unexpected break fields. Expected TUPLE breaks = " + expectedFieldBreaks
                                        + " GOT breaks: " + x.getBreakFields() + "\n" + tuple.getAllValues());
                            }
                        }
                        else {
                            expectedPairBreak.add(tuple);
                            logger.info("Expected BREAK not found: " + tuple.getAllValues());
                        }
                        break;
                    default:
                        logger.info("Unknown event type: " + type);
                }
            }

            if (expectedBreakFields.size() + expectedLeftBreak.size() + expectedMatch.size() + expectedPairBreak.size()
                    + expectedPairBreak.size() + expectedRightBreak.size() == 0) {
                logger.info("testComparisonCorrectness SUCCEEDED. No Unexpected Problems with data set.");
            }
            else {
                logger.info("testComparisonCorrectness FAILED! ");
            }
        }

        private CompareEvent.TYPE getExpectedEventType (Tuple tuple) {
            final String ev = (String)tuple.getAllValues().get("NOTES");
            CompareEvent.TYPE type = null;

            return type;
        }
    }
}
