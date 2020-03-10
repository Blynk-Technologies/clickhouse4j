package cc.blynk.clickhouse.http;

import cc.blynk.clickhouse.ClickHouseExternalData;
import cc.blynk.clickhouse.except.ClickHouseException;
import cc.blynk.clickhouse.except.ClickHouseExceptionSpecifier;
import cc.blynk.clickhouse.settings.ClickHouseProperties;
import cc.blynk.clickhouse.util.ClickHouseLZ4InputStream;
import cc.blynk.clickhouse.util.ClickHouseLZ4OutputStream;
import cc.blynk.clickhouse.util.guava.StreamUtils;
import cc.blynk.clickhouse.util.ssl.NonValidatingTrustManager;
import io.netty.channel.epoll.Epoll;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.JdkSslContext;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URI;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class AsyncHttpConnector implements HttpConnector {

    private static final Logger log = LoggerFactory.getLogger(AsyncHttpConnector.class);

    private final ClickHouseProperties properties;

    private final AsyncHttpClient asyncHttpClient;

    public AsyncHttpConnector(ClickHouseProperties properties) throws ClickHouseException {
        this.properties = properties;

        DefaultAsyncHttpClientConfig.Builder httpClientBuilder = new DefaultAsyncHttpClientConfig.Builder()
                .setUserAgent(null)
                .setKeepAlive(true)
                .setConnectTimeout(properties.getConnectionTimeout())
                .setMaxConnections(properties.getMaxTotal())
                .setUseNativeTransport(Epoll.isAvailable());
        if (properties.getSsl()) {
            try {
                httpClientBuilder.setSslContext(new JdkSslContext(getSSLContext(), true, ClientAuth.REQUIRE));
            } catch (Exception e) {
                throw ClickHouseExceptionSpecifier.specify(e, properties.getHost(), properties.getPort());
            }
        }
        this.asyncHttpClient = new DefaultAsyncHttpClient(httpClientBuilder.build());
    }

    @Override
    public InputStream post(String sql, URI uri) throws ClickHouseException {
        ByteArrayInputStream from = prepareInputStream(sql);
        ListenableFuture<Response> listenableFuture = this.asyncHttpClient.preparePost(uri.toString())
                .setBody(from)
                .execute();
        return getResponseBodyAsStream(listenableFuture);
    }

    @Override
    public void post(String sql, InputStream from, URI uri) throws ClickHouseException {
        ByteArrayOutputStream maybeCompressed = openOutputStream(sql, from);
        InputStream fromWithSql = new ByteArrayInputStream(maybeCompressed.toByteArray());

        CompletableFuture<Response> completableFuture = this.asyncHttpClient.preparePost(uri.toString())
                .setBody(fromWithSql)
                .execute().toCompletableFuture();
        checkForErrorAndThrow(completableFuture);
    }

    @Override
    public InputStream post(List<ClickHouseExternalData> externalData, URI uri) throws ClickHouseException {
        String boundaryString = UUID.randomUUID().toString();
        ByteArrayInputStream from = prepareInputStream(externalData, boundaryString);

        ListenableFuture<Response> listenableFuture = this.asyncHttpClient.preparePost(uri.toString())
                .addHeader("Content-Type", "multipart/form-data; boundary=" + boundaryString)
                .setBody(from)
                .execute();
        return getResponseBodyAsStream(listenableFuture);
    }

    @Override
    public void post(String sql, List<byte[]> data, URI uri) throws ClickHouseException {
        ByteArrayInputStream from = prepareInputStream(sql, data);

        CompletableFuture<Response> completableFuture = this.asyncHttpClient.preparePost(uri.toString())
                .setBody(from)
                .execute().toCompletableFuture();
        checkForErrorAndThrow(completableFuture);
    }

    @Override
    public void close() {
        try {
            this.asyncHttpClient.close();
        } catch (IOException e) {
            log.error("Error on closing HTTP client. {}", e.getMessage());
        }
    }

    private ByteArrayInputStream prepareInputStream(String sql) throws ClickHouseException {
        byte[] sqlBytes = getSqlBytes(sql);
        ByteArrayOutputStream maybeCompressed = openOutputStream(sqlBytes, null);

        return new ByteArrayInputStream(maybeCompressed.toByteArray());
    }

    private ByteArrayInputStream prepareInputStream(List<ClickHouseExternalData> externalData, String boundaryString)
            throws ClickHouseException {
        byte[] bytes = buildMultipartData(externalData, boundaryString);
        ByteArrayOutputStream maybeCompressed = openOutputStream(bytes, null);

        return new ByteArrayInputStream(maybeCompressed.toByteArray());
    }

    private ByteArrayInputStream prepareInputStream(String sql, List<byte[]> data) throws ClickHouseException {
        byte[] sqlBytes = getSqlBytes(sql);
        ByteArrayOutputStream maybeCompressed = openOutputStream(sqlBytes, data);

        return new ByteArrayInputStream(maybeCompressed.toByteArray());
    }

    private byte[] buildMultipartData(List<ClickHouseExternalData> externalData, String boundaryString)
            throws ClickHouseException {
        try (ByteArrayOutputStream requestBodyStream = new ByteArrayOutputStream();
             BufferedWriter httpRequestBodyWriter = new BufferedWriter(new OutputStreamWriter(requestBodyStream))) {
            for (ClickHouseExternalData data : externalData) {
                httpRequestBodyWriter.write("--" + boundaryString + "\r\n");
                httpRequestBodyWriter.write("Content-Disposition: form-data;"
                        + " name=\"" + data.getName() + "\";"
                        + " filename=\"" + data.getName() + "\"" + "\r\n");
                httpRequestBodyWriter.write("Content-Type: application/octet-stream" + "\r\n");
                httpRequestBodyWriter.write("Content-Transfer-Encoding: binary" + "\r\n" + "\r\n");
                httpRequestBodyWriter.flush();

                StreamUtils.copy(data.getContent(), requestBodyStream);

                requestBodyStream.flush();
            }

            httpRequestBodyWriter.write("\r\n" + "--" + boundaryString + "--" + "\r\n");
            httpRequestBodyWriter.flush();

            return requestBodyStream.toByteArray();
        } catch (IOException e) {
            log.error("Building Multipart Body failed. {}", e.getMessage());
            throw ClickHouseExceptionSpecifier.specify(e, properties.getHost(), properties.getPort());
        }
    }

    private byte[] getSqlBytes(String sql) {
        if (!sql.endsWith("\n")) {
            sql += "\n";
        }
        return sql.getBytes(UTF_8);
    }

    private ByteArrayOutputStream openOutputStream(String sql, InputStream inputStream) throws ClickHouseException {
        byte[] sqlBytes = getSqlBytes(sql);
        byte[] bytes;
        try {
            bytes = StreamUtils.toByteArray(inputStream);
        } catch (IOException e) {
            throw ClickHouseExceptionSpecifier.specify(e, properties.getHost(), properties.getPort());
        }
        ArrayList<byte[]> batches = new ArrayList<>();
        batches.add(bytes);

        return openOutputStream(sqlBytes, batches);
    }

    private ByteArrayOutputStream openOutputStream(byte[] sqlBytes, List<byte[]> batches) throws ClickHouseException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        OutputStream outputStream;

        if (properties.isDecompress()) {
            outputStream = new ClickHouseLZ4OutputStream(baos, properties.getMaxCompressBufferSize());
        } else {
            outputStream = new DataOutputStream(baos);
        }

        try {
            outputStream.write(sqlBytes);
            if (batches != null) {
                for (byte[] batch : batches) {
                    outputStream.write(batch);
                }
            }

            outputStream.flush();
            return baos;
        } catch (IOException e) {
            throw ClickHouseExceptionSpecifier.specify(e, properties.getHost(), properties.getPort());
        }
    }

    private InputStream getResponseBodyAsStream(ListenableFuture<Response> listenableFuture)
            throws ClickHouseException {
        checkForErrorAndThrow(listenableFuture);
        try {
            Response response = listenableFuture.get();
            InputStream responseBody = response.getResponseBodyAsStream();

            return properties.isCompress() ? new ClickHouseLZ4InputStream(responseBody) : responseBody;
        } catch (InterruptedException | ExecutionException e) {
            log.error("Http POST request failed. {}", e.getMessage());
            throw ClickHouseExceptionSpecifier.specify(e, properties.getHost(), properties.getPort());
        }
    }

    private void checkForErrorAndThrow(Future<Response> completableFuture)
            throws ClickHouseException {
        Response response;
        try {
            response = completableFuture.get();
        } catch (Exception e) {
            throw ClickHouseExceptionSpecifier.specify("Http POST request failed.",
                    properties.getHost(), properties.getPort());
        }

        if (response == null) {
            log.error("Http POST request failed.");
            throw ClickHouseExceptionSpecifier.specify("Http POST request failed.",
                    properties.getHost(), properties.getPort());
        }
        if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
            InputStream responseBody = response.getResponseBodyAsStream();

            byte[] bytes;
            try {
                bytes = StreamUtils.toByteArray(responseBody);
            } catch (IOException e) {
                log.warn("Error while read compressed stream. {}", e.getMessage());
                throw ClickHouseExceptionSpecifier.specify(e, properties.getHost(), properties.getPort());
            }

            InputStream messageStream = null;
            if (properties.isCompress()) {
                try {
                    messageStream = new ClickHouseLZ4InputStream(new ByteArrayInputStream(bytes));
                    bytes = StreamUtils.toByteArray(messageStream);
                } catch (IOException e) {
                    log.warn("Error while read compressed stream. {}", e.getMessage());
                }
            }

            StreamUtils.close(messageStream);
            String chMessage = new String(bytes, UTF_8);

            throw ClickHouseExceptionSpecifier.specify(chMessage, properties.getHost(), properties.getPort());
        }
    }

    private SSLContext getSSLContext()
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

                    tmf.init(getKeyStore());
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

    private KeyStore getKeyStore()
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
