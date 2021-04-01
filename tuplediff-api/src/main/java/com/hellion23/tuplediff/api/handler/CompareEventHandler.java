package com.hellion23.tuplediff.api.handler;

import com.hellion23.tuplediff.api.TDCompare;
import com.hellion23.tuplediff.api.compare.CompareEvent;

import java.util.function.Consumer;

/**
 * Created by hleung on 5/24/2017.
 */
public interface CompareEventHandler extends AutoCloseable, Consumer<CompareEvent> {
    /**
     * This method is invoked after the left & right streams have been opened/initialized but before any
     * comparisons happen. This method is allow access to metadata of the @TDCompare as well as providing a hook
     * to stop the TDCompare from continuing if this a "monitoring" handler.
     *
     * @param comparison
     */
    default void init (TDCompare comparison) {}

    @Override
    default void close() throws Exception {
        //NOOP
    }
}
