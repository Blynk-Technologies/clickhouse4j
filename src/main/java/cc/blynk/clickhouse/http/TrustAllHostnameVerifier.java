package cc.blynk.clickhouse.http;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;

final class TrustAllHostnameVerifier implements HostnameVerifier {

    private final static TrustAllHostnameVerifier INSTANCE = new TrustAllHostnameVerifier();

    @Override
    public boolean verify(String s, SSLSession sslSession) {
        return true;
    }

    static TrustAllHostnameVerifier getInstance() {
        return INSTANCE;
    }
}
