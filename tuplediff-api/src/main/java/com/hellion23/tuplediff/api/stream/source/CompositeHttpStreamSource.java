package com.hellion23.tuplediff.api.stream.source;

import com.hellion23.tuplediff.api.model.TDException;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.HttpClient;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;


/**
 * Chains together multiple HTTPStreamSources.
 */
@Slf4j
@EqualsAndHashCode
@ToString
public class CompositeHttpStreamSource extends CompositeStreamSource<HttpStreamSource> {
    String method;
    String url;
    Iterator<String> urls;
    String requestPayload;
    Iterator <String> requestPayloads;
    HttpAuth auth;
    Map<String, String> headers;
    HttpClient httpClient;

    {
        /**
         * The trustStore property needs to be set to access
         */
        if (System.getProperty("javax.net.ssl.trustStore") == null) {
            String defaultKeyStorePath =
                    Thread.currentThread().getContextClassLoader().getResource("hb_ssl_keystore.jks").getPath();
            log.info("Setting trustStore path: " + defaultKeyStorePath );
            System.setProperty("javax.net.ssl.trustStore", defaultKeyStorePath);
            System.setProperty("javax.net.ssl.keyStore", defaultKeyStorePath);
        }
    }

    public CompositeHttpStreamSource (HttpClient httpClient, String method, String url, Iterator <String> urls,  String requestPayload, Iterator<String> requestPayloads, HttpAuth auth,  Map<String, String>headers) {
        this.httpClient = httpClient;
        this.method = method;
        this.url = url;
        this.urls = urls;
        this.requestPayload = requestPayload;
        this.requestPayloads = requestPayloads;
        this.auth = auth;
        this.headers = headers;
    }

    @Override
    public HttpStreamSource doNext () {
        HttpStreamSource nextSource = null;

        // Okay now open the Stream and prepare the InputStream
        while (nextSource == null && advanceToNextUrlAndPayload()) {
            nextSource = new HttpStreamSource(httpClient, url, method,  auth,  requestPayload, headers);
            try {
                nextSource.open();
            } catch (IOException e) {
                throw new TDException("Could not open HTTP URL: " + url + " Reason: " + e.getMessage(), e);
            }
            if (nextSource.getStatusCode() == 204) { // No content! need to advance to next Stream!
                nextSource = null;
            }
        }
        return nextSource;
    }

    /**
     * Advances to next URL or payload if possible.
     *
     * @return returns false if no more URLs or payloads to read.
     */
    protected boolean advanceToNextUrlAndPayload () {
        boolean advanced = false;
        // Advance to the next url or payload to read:
        if (urls != null && urls.hasNext()) {
            advanced = true;
            this.url = urls.next();
        }
        if (requestPayloads != null && requestPayloads.hasNext()) {
            advanced = true;
            this.requestPayload = requestPayloads.next();
        }
        return advanced;
    }

    public void setHttpClient(HttpClient httpClient) {
        this.httpClient = httpClient;
    }
}
