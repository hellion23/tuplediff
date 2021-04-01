package com.hellion23.tuplediff.api.config;


import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import lombok.*;
import org.apache.http.client.HttpClient;

import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@Builder
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
@ToString
public class HttpSourceConfig implements SourceConfig{

    @JacksonXmlProperty(localName = "url")
    @JsonProperty(value="url")
    String url;

    @JacksonXmlProperty(localName = "method")
    @JsonProperty(value="method")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    String method;

    @JacksonXmlProperty(localName = "auth")
    @JsonProperty(value="auth")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    Auth auth;

    @JacksonXmlProperty(localName = "payload")
    @JsonProperty(value="payload")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    String requestPayload;

    Map<String, String> headers;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    HttpClient httpClient;
    @JsonSetter(value="httpClient")
    private void unmarshalHttpClient() {
        //No Op
    }

    @JsonGetter(value="httpClient")
    private String marshalHttpClient () {
        if (httpClient != null) {
            throw new UnsupportedOperationException("A HttpSourceConfig defined using an actual httpClient cannot be marshalled! " +
                    " If you want to have (un)marshallable httpClient use explicit Auth type, user/pass, headers instead");
        }
        return null;
    }

    public String getUrl() {
        return url;
    }

    public String getMethod() {
        return method;
    }

    public Auth getAuth() {
        return auth;
    }

    public String getRequestPayload() {
        return requestPayload;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    @JsonGetter(value="headers")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String marshalheaders() {
        return MarshalUtils.marshalMapOfStrings(headers);
    }

    @JsonSetter(value="headers")
    private void unmarshalHeaders (String string) {
        headers = MarshalUtils.unmarshalMapOfStrings(string);
    }

    public HttpClient getHttpClient() {
        return httpClient;
    }

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public static class Auth {
        @JacksonXmlProperty(localName = "type")
        @JsonProperty(value="type")
        String type = "NONE";

        @JacksonXmlProperty(localName = "username")
        @JsonProperty(value="username")
        String username;

        @JacksonXmlProperty(localName = "password")
        @JsonProperty(value="password")
        String password;

        public Auth() {}

        protected Auth(String type, String username, String password) {
            this.type = type;
            this.username = username;
            this.password = password;
        }

        public static Auth none() {
            return new Auth("NONE", null, null);
        }

        public static Auth basicUserAndPass (String username, String password) {
            return new Auth("BASIC", username, password);
        }

        public String getType() {
            return type;
        }

        public String getUsername() {
            return username;
        }

        public String getPassword() {
            return password;
        }


    }

}
