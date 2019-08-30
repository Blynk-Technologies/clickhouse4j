package ru.yandex.clickhouse.http;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;

class TrustAllHostnameVerifier implements HostnameVerifier {

    private final static TrustAllHostnameVerifier INSTANCE = new TrustAllHostnameVerifier();

    @Override
    public boolean verify(String s, SSLSession sslSession) {
        return true;
    }

    static TrustAllHostnameVerifier getInstance() {
        return INSTANCE;
    }
}
