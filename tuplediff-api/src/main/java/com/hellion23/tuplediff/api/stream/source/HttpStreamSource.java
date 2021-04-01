package com.hellion23.tuplediff.api.stream.source;

import com.hellion23.tuplediff.api.compare.ComparableLinkedHashMap;
import com.hellion23.tuplediff.api.model.TDException;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.*;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Wraps an Http Response source. Mainly used to make RESTful calls.
 * Currently only supports BASIC and NONE auth types.
 *
 */

@Slf4j
@EqualsAndHashCode
@ToString
@NoArgsConstructor
public class HttpStreamSource extends StreamSource {
    private static final Logger LOG = LoggerFactory.getLogger(HttpStreamSource.class);
    String url;
    String method;
    HttpAuth auth;
    String requestPayload;
    Map<String, String> headers;
    int statusCode = 0;
    HttpClient httpClient;

    {
        /**
         * The trustStore property needs to be set to access
         */
        if (System.getProperty("javax.net.ssl.trustStore") == null) {
            String defaultKeyStorePath =
                    Thread.currentThread().getContextClassLoader().getResource("hb_ssl_keystore.jks").getPath();
            LOG.info("Setting trustStore path: " + defaultKeyStorePath );
            System.setProperty("javax.net.ssl.trustStore", defaultKeyStorePath);
            System.setProperty("javax.net.ssl.keyStore", defaultKeyStorePath);
        }
    }

    public HttpStreamSource(HttpClient httpClient, String url, String method, HttpAuth auth, String requestPayload, Map<String, String> headers) {
        this.httpClient = httpClient;
        this.url = url;
        this.method = method == null ? "GET" : method;
        this.auth = auth == null ? HttpAuth.NONE : auth;
        this.requestPayload = requestPayload;
        this.headers = headers == null ? new ComparableLinkedHashMap<>() : headers;
    }

    @Override
    protected InputStream openSource() throws IOException {
        HttpClient httpClient =  getHttpClient();

        HttpUriRequest request;
        switch (method) {
            case "GET":       request = new HttpGet(url);     break;
            case "POST":      request = new HttpPost(url);    break;
            case "PUT":       request = new HttpPut(url);     break;
            case "DELETE":    request = new HttpDelete(url);  break;
            default:
                throw new IOException("Unsupported HTTPMethod: "+method);
        }

        // Attach a payload if there is a payload and if POST/PUT request
        if (requestPayload != null && request instanceof HttpEntityEnclosingRequest) {
            ((HttpEntityEnclosingRequest)request).setEntity(new StringEntity(requestPayload));
        }

        //attach headers (if any):
        if (headers != null && headers.size() > 0) {
            headers.entrySet().forEach(me -> request.setHeader(me.getKey(), me.getValue()));
        }
        HttpResponse response = httpClient.execute(request);
        statusCode = response.getStatusLine().getStatusCode();
        String statusReason = response.getStatusLine().getReasonPhrase();

        if (statusCode < 200 || statusCode >= 300) {
            throw new IOException (String.format("Could not open Http Response for URL <%s>. Code: %s, Reason: %s", url, statusCode, statusReason));
        }
        else {
            // Only codes 2xx codes are acceptable, successful calls.
            return response.getEntity().getContent();
        }
    }

    public HttpClient getHttpClient () {
        if (httpClient == null) {
            HttpClientBuilder builder = HttpClientBuilder.create();
            httpClient =  setCredentialsProvider(builder).build();
        }
        return httpClient;
    }

    /**
     * Override the default httpClient with custom implementation.
     *
     * @param httpClient
     */
    public void setHttpClient (HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public int getStatusCode () {
        return this.statusCode;
    }

    protected HttpClientBuilder setCredentialsProvider (HttpClientBuilder builder) {
        switch (auth.getType()) {
            case BASIC:
                CredentialsProvider provider = new BasicCredentialsProvider();
                provider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth.getUsername(), auth.getPassword()));
                builder.setDefaultCredentialsProvider(provider);
                break;
            case NONE:
                break;
            default:
                throw new TDException("Unsupported Credentials type " + auth.getType());
        }
        return builder;
    }


}
