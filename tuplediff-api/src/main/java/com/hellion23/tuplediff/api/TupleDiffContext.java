package com.hellion23.tuplediff.api;

import com.hellion23.tuplediff.api.handler.CascadingEventHandler;
import com.hellion23.tuplediff.api.handler.CompareEventHandler;
import com.hellion23.tuplediff.api.variable.VariableContext;
import org.apache.http.client.HttpClient;

import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

/**
 * Variables that control how the TupleDiff needs to be executed. This does not control what is being compared but
 * rather:
 *
 * CompareEventHandler - what do we do with the results?
 * ExecutorService - what threadpool do we use to run the streams?
 * VariableContext - Variable definitions that allow us to inject values into text.
 * HttpClient - Custom HttpClient used to make Http based calls.
 *
 */
public class TupleDiffContext {
    CompareEventHandler eventHandler;
    ExecutorService executor;
    VariableContext variableContext;
    Supplier<HttpClient> httpClientSupplier = () -> null;

    public TupleDiffContext () {
        this (null, null, null);
    }

    public TupleDiffContext(CompareEventHandler... handlers) {
        this (handlers.length == 1 ? handlers[0] : new CascadingEventHandler(handlers), null, null);
    }

    public TupleDiffContext(CompareEventHandler eventHandler, ExecutorService executor, VariableContext variableContext) {
        this.eventHandler = eventHandler;
        this.executor = executor;
        setVariableContext(variableContext);
    }

    public CompareEventHandler getEventHandler() {
        return eventHandler;
    }

    public void setEventHandler(CompareEventHandler eventHandler) {
        this.eventHandler = eventHandler;
    }

    public void setEventHandlers(CompareEventHandler ... eventHandler) {
        this.eventHandler = new CascadingEventHandler(eventHandler);
    }

    public ExecutorService getExecutor() {
        return executor;
    }

    public void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    public Supplier<HttpClient> getHttpClientSupplier() {
        return httpClientSupplier;
    }

    public void setHttpClientSupplier(Supplier<HttpClient> httpClientSupplier) {
        this.httpClientSupplier = httpClientSupplier;
    }

    public VariableContext getVariableContext() {
        return variableContext;
    }

    public void setVariableContext(VariableContext variableContext) {
        this.variableContext = variableContext;
        if (this.variableContext == null) {
            this.variableContext = new VariableContext();
        }
    }
}
