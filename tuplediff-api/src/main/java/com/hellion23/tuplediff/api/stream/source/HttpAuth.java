package com.hellion23.tuplediff.api.stream.source;

public class HttpAuth {
    public static HttpAuth NONE = new HttpAuth(Type.NONE, null, null);
    public enum Type {NONE, BASIC, DIGEST, NTLM}
    String username;
    String password;
    Type type;

    public HttpAuth(Type type, String username, String password) {
        this.username = username;
        this.password = password;
        this.type = type;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public Type getType() {
        return type;
    }


}
