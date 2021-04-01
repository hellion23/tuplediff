package com.hellion23.tuplediff.api.handler;

import com.hellion23.tuplediff.api.TDCompare;
import com.hellion23.tuplediff.api.compare.CompareEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by hleung on 5/30/2017.
 */
public class CascadingEventHandler implements CompareEventHandler {
    private final static Logger LOG = LoggerFactory.getLogger(CSVEventHandler.class);
    List<CompareEventHandler> handlers;
    List<CompareEventHandler> activeHandlers = new LinkedList<>();

    public CascadingEventHandler (CompareEventHandler ... handlers) {
        this (Arrays.asList(handlers));
    }

    public CascadingEventHandler(Collection<CompareEventHandler> handlers) {
        this.handlers = new ArrayList<> (handlers);
    }

    /**
     * Any handlers that exception during the init() process are no longer active and accept() and close() methods
     * will not be called.
     *
     * @param comparison
     */
    @Override
    public void init(TDCompare comparison) {
        for (CompareEventHandler handler : handlers) {
            try {
                handler.init(comparison);
                activeHandlers.add(handler);
            }
            catch (Exception ex) {
                LOG.error ("Error initializing handler: " + handler, ex);
            }
        }
    }

    @Override
    public void close() throws Exception {
        for (CompareEventHandler handler : activeHandlers) {
            try {
                handler.close();
            }
            catch (Exception ex) {
                LOG.error ("Error closing handler: " + handler, ex);
            }
        }
    }

    @Override
    public void accept(CompareEvent compareEvent) {
        for (CompareEventHandler handler : activeHandlers) {
            try {
                handler.accept(compareEvent);
            }
            catch (Exception ex) {
                LOG.error ("Error processing compare event: " + compareEvent + " for handler: " + handler, ex);
            }
        }
    }
}
