package com.hellion23.tuplediff.api.monitor;

import com.hellion23.tuplediff.api.TupleComparison;
import com.hellion23.tuplediff.api.TupleDiffException;
import com.hellion23.tuplediff.api.TupleStream;

import java.util.Map;

/**
 * A Monitor tracks  events that are reported by the different sources that it is tracking.
 *
 * Created by Hermann on 9/28/2014.
 */
public interface Monitor {
    public static final String EVENT_START = "EVENT_START";
    public static final String EVENT_STOP_NORMAL = "EVENT_STOP_NORMAL";
    public static final String EVENT_STOP_ABNORMAL = "EVENT_STOP_ABNORMAL";

    // Don't know if these should be/can be used...
    public static final String EVENT_STOP_CANCELLED = "EVENT_STOP_CANCELLED";
    public static final String EVENT_STOP_BY_MONITOR = "EVENT_STOP_BY_MONITOR";

    public void reportEvent(Nameable source, String eventName, Object... params) throws TupleDiffException;
    public Map<Nameable, Stats> getAllStats();

    // These methods are called by the TupleComparison on startup. Left and Right Streams themselves should not be
    // expected to be intialized.
    public void setTupleComparison(TupleComparison tc);
    public void setLeftStream (TupleStream leftStream);
    public void setRightStream (TupleStream leftStream);

    // Invoked by TupleComparison.
    public void init();

    // Helper method to get only the CompareStats associated w/ the TupleComparison set.
    // The CompareStats should already in getAllStats()
    public CompareStats getCompareStats();
    public Stats getLeftStats ();
    public Stats getRightStats ();
}
