package com.hellion23.tuplediff.api.sample;
import com.hellion23.tuplediff.api.TDCompare;
import com.hellion23.tuplediff.api.handler.*;
import com.hellion23.tuplediff.api.model.TDStream;
import com.hellion23.tuplediff.api.stream.sql.DataSourceProviders;
import com.hellion23.tuplediff.api.stream.sql.SortedSQLTDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

/**
 * Not a Test. But here as an example of Java usage of TDCompare.
 * Created by hleung on 6/28/2017.
 */
public class PricemasterDWRecon {
    private final static Logger LOG = LoggerFactory.getLogger(PricemasterDWRecon.class);
    static String baseDir = "h:\\tmp\\tuplediff\\";

    public void run() {
        List<String> primaryKey = Arrays.asList("DATE_ID", "PRICE_SOURCE_CD", "PAL_MASTER_SEC_ID") ;

        // Construct left stream:
        Supplier<DataSource> leftDS = DataSourceProviders.getDb("paladyne-pricemaster-production");
        String leftSql =
                "select \n" +
                        "CONVERT(VARCHAR(10), Date, 112) as DATE_ID, \n" +
                        "SOURCE_CD PRICE_SOURCE_CD, \n" +
                        "PAL_MASTER_SEC_ID\n" +
                        "from FACT_SECURITY_PRICE\n" +
                        "where DATE between '20150101' and '20150102'";
        TDStream left =
                new SortedSQLTDStream("LEFT", leftDS, leftSql, primaryKey);

        // Construct right stream:
        Supplier<DataSource> rightDS =  DataSourceProviders.getDb("dwv2-production");
        String rightSql =
                "SELECT\n" +
                        "  TO_CHAR(fspm.date_id) as DATE_ID,\n" +
                        "  sec.PAL_MASTER_SEC_ID,\n" +
                        "  ps.PRICE_SOURCE_CD \n" +
                        "FROM\n" +
                        "  DW.FACT_SECURITY_PRICE_MASTER fspm,\n" +
                        "  dw.dim_security sec,\n" +
                        "  dw.dim_price_source ps,\n" +
                        "  dw.dim_date dd\n" +
                        "WHERE\n" +
                        "  sec.security_id      = fspm.security_id\n" +
                        "AND dd.date_id         = fspm.date_id\n" +
                        "AND weekend_ind        = 'N'\n" +
                        "AND ps.price_sourcE_id = fspm.price_source_id\n" +
                        "AND fspm.date_id BETWEEN 20150101 AND 20150102";

        TDStream right = new SortedSQLTDStream("RIGHT", rightDS, rightSql, primaryKey);

        // Setup Event Handlers:
        // Collect stats, i.e. how many breaks, left only, right only.
        StatsEventHandler stats = new StatsEventHandler();

        // Stream breaks into a file:
        CSVEventHandler csv =  new CSVEventHandler(baseDir + "results.csv");
        ConsoleEventHandler log = new ConsoleEventHandler();
        CompareEventHandler handler = new CascadingEventHandler(stats, csv);

        // Create the comparison & set parameters.
        TDCompare compare = new TDCompare(primaryKey,left, right,handler);
//        compare.setExcludeFields(Arrays.asList(
//                "IGNORE_FIELD"
//                , "UPDATED_TIMESTAMP"
//        ));

        // Initialize to get the metadata setup created for the streams. At this point you can query metadata for the streams
        // This call will also fail if there is something wrong w/ configuration (e.g. trying to compare Dates with Numbers.
        compare.initialize();
        compare.compare();

        LOG.info("Compare stats: " + stats);
    }

    public static void main (String args[]) {
        new PricemasterDWRecon().run();
    }
}
