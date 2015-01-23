package com.hellion23.tuplediff.service;

import com.hellion23.tuplediff.api.ComparisonResult;
import com.hellion23.tuplediff.api.Config;
import com.hellion23.tuplediff.api.TupleComparison;
import com.hellion23.tuplediff.api.listener.CompareEventListener;
import com.hellion23.tuplediff.api.monitor.Monitor;
import com.hellion23.tuplediff.service.com.hellion23.tuplediff.service.test.JavaDB;
import com.hellion23.tuplediff.service.com.hellion23.tuplediff.service.test.TestDataLoader;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

/**
 * @author: Hermann Leung
 * Date: 12/16/2014
 */
public class TupleDiffService {

    static final TupleDiffService tds = new TupleDiffService();

    public static TupleDiffService Instance () {
        return tds;
    }

    public ComparisonResult compare (Config config) {
        return compare(config, null, null);
    }

    /**
     *
     * @param config
     * @param monitor
     * @param compareEventListener
     * @return
     */
    public ComparisonResult compare (Config config, Monitor monitor, CompareEventListener compareEventListener) {
        TupleComparison tc = new TupleComparison(config, monitor, compareEventListener);
        tc.compare();
        return tc.getResult();
    }

    public Future <ComparisonResult> compareAsynch (Config config, Monitor monitor, CompareEventListener compareEventListener) {
        TupleComparison tc = new TupleComparison(config, monitor, compareEventListener);
        ComparisonResult result = tc.getResult();
        return new FutureTask <ComparisonResult> (new TupleComparisonCallable(tc));
    }

    static class TupleComparisonCallable implements Callable <ComparisonResult>  {
        TupleComparison tc;
        public TupleComparisonCallable (TupleComparison tc) {
            this.tc = tc;
        }
        @Override
        public ComparisonResult call() throws Exception {
            tc.compare();
            return tc.getResult();
        }
    }

}
