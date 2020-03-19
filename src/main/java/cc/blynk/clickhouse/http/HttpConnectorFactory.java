package cc.blynk.clickhouse.http;

import cc.blynk.clickhouse.settings.ClickHouseProperties;
import cc.blynk.clickhouse.util.guava.StreamUtils;
import cc.blynk.clickhouse.util.ssl.NonValidatingTrustManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.Closeable;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.Iterator;

public abstract class HttpConnectorFactory implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(HttpConnectorFactory.class);

    public HttpConnectorFactory() {
    }

    private static HttpConnectorFactory httpConnectorFactory;

    private static HttpConnectorFactory getHttpConnectorFactory(ClickHouseProperties properties) {
        if (httpConnectorFactory == null) {
            synchronized (HttpConnectorFactory.class) {
                if (httpConnectorFactory == null) {
                    httpConnectorFactory = createFactory(properties);
                }
            }
        }
        return httpConnectorFactory;
    }

    public static HttpConnector getHttpConnector(ClickHouseProperties properties) {
        return getHttpConnectorFactory(properties).create(properties);
    }

    public static void shutdown() throws IOException {
        if (httpConnectorFactory != null) {
            httpConnectorFactory.close();
        }
    }

    private static HttpConnectorFactory createFactory(ClickHouseProperties properties) {
        String client;
        HttpConnectorFactory connectorFactory;
        if ("ASYNC".equals(properties.getConnectorType())) {
            client = "Async Http Client";
            connectorFactory = new AsyncConnectorFactory(properties);
        } else {
            client = "HttpUrlConnection Client";
            connectorFactory = new DefaultConnectorFactory();
        }
        log.info("Using {} for clickhouse.", client);
        return connectorFactory;
    }

    public abstract HttpConnector create(ClickHouseProperties properties);

    static SSLContext getSSLContext(ClickHouseProperties properties)
            throws CertificateException, NoSuchAlgorithmException,
            KeyStoreException, IOException, KeyManagementException {
        SSLContext ctx = SSLContext.getInstance("TLS");
        TrustManager[] tms = null;
        KeyManager[] kms = null;
        SecureRandom sr = null;

        switch (properties.getSslMode()) {
            case "none":
                tms = new TrustManager[]{new NonValidatingTrustManager()};
                kms = new KeyManager[]{};
                sr = new SecureRandom();
                break;
            case "strict":
                if (!properties.getSslRootCertificate().isEmpty()) {
                    TrustManagerFactory tmf = TrustManagerFactory
                            .getInstance(TrustManagerFactory.getDefaultAlgorithm());

                    tmf.init(getKeyStore(properties));
                    tms = tmf.getTrustManagers();
                    kms = new KeyManager[]{};
                    sr = new SecureRandom();
                }
                break;
            default:
                throw new IllegalArgumentException("unknown ssl mode '" + properties.getSslMode() + "'");
        }

        ctx.init(kms, tms, sr);
        return ctx;
    }

    private static KeyStore getKeyStore(ClickHouseProperties properties)
            throws NoSuchAlgorithmException, IOException, CertificateException, KeyStoreException {
        KeyStore ks;
        try {
            ks = KeyStore.getInstance("jks");
            ks.load(null, null); // needed to initialize the key store
        } catch (KeyStoreException e) {
            throw new NoSuchAlgorithmException("jks KeyStore not available");
        }

        InputStream caInputStream;
        try {
            caInputStream = new FileInputStream(properties.getSslRootCertificate());
        } catch (FileNotFoundException ex) {
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            caInputStream = cl.getResourceAsStream(properties.getSslRootCertificate());
            if (caInputStream == null) {
                throw new IOException(
                        "Could not open SSL/TLS root certificate file '" + properties
                                .getSslRootCertificate() + "'", ex);
            }
        }
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        Iterator<? extends Certificate> caIt = cf.generateCertificates(caInputStream).iterator();
        StreamUtils.close(caInputStream);
        for (int i = 0; caIt.hasNext(); i++) {
            ks.setCertificateEntry("cert" + i, caIt.next());
        }

        return ks;
    }

}
