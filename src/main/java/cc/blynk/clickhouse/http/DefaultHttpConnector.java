package cc.blynk.clickhouse.http;

import cc.blynk.clickhouse.ClickHouseExternalData;
import cc.blynk.clickhouse.except.ClickHouseException;
import cc.blynk.clickhouse.except.ClickHouseExceptionSpecifier;
import cc.blynk.clickhouse.response.ClickHouseLZ4Stream;
import cc.blynk.clickhouse.settings.ClickHouseProperties;
import cc.blynk.clickhouse.util.ClickHouseLZ4OutputStream;
import cc.blynk.clickhouse.util.guava.StreamUtils;
import cc.blynk.clickhouse.util.ssl.NonValidatingTrustManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
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
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

final class DefaultHttpConnector implements HttpConnector {

    private static final Logger log = LoggerFactory.getLogger(DefaultHttpConnector.class);

    protected final ClickHouseProperties properties;

    DefaultHttpConnector(ClickHouseProperties properties) {
        this.properties = properties;
    }

    @Override
    public void post(InputStream from, OutputStream to, URI uri) throws ClickHouseException {
        HttpURLConnection connection = buildConnection(uri);
        sendPostRequest(from, to, connection);
    }

    @Override
    public void post(byte[] sqlBytes, List<byte[]> data, URI uri) throws ClickHouseException {
        HttpURLConnection connection = buildConnection(uri);
        InputStream is = sendPostRequest(sqlBytes, data, connection);
        StreamUtils.close(is);
    }

    @Override
    public void post(byte[] bytes, URI uri) throws ClickHouseException {
        post(new ByteArrayInputStream(bytes), uri);
    }

    @Override
    public void post(InputStream from, URI uri) throws ClickHouseException {
        post(from, null, uri);
    }

    @Override
    public void post(String sql, OutputStream to, URI uri) throws ClickHouseException {
        byte[] bytes = sql.getBytes(StandardCharsets.UTF_8);
        post(new ByteArrayInputStream(bytes), to, uri);
    }

    @Override
    public void post(List<ClickHouseExternalData> externalData, OutputStream to, URI uri) throws ClickHouseException {
        String boundaryString = UUID.randomUUID().toString();
        HttpURLConnection connection = buildConnection(uri);
        connection.setRequestProperty("Content-Type", "multipart/form-data; boundary=" + boundaryString);

        byte[] bytes = buildMultipartData(externalData, boundaryString);
        InputStream from = new ByteArrayInputStream(bytes);

        sendPostRequest(from, to, connection);
    }

    @Override
    public void cleanConnections() {

    }

    @Override
    public void closeClient() {

    }

    private InputStream sendPostRequest(byte[] sqlBytes, List<byte[]> batches, HttpURLConnection connection)
            throws ClickHouseException {
        OutputStream outputStream = null;
        try {
            outputStream = new DataOutputStream(connection.getOutputStream());
            if (properties.isDecompress()) {
                outputStream = new ClickHouseLZ4OutputStream(outputStream, properties.getMaxCompressBufferSize());
            }
            outputStream.write(sqlBytes);
            for (byte[] batch : batches) {
                outputStream.write(batch);
            }
            outputStream.flush();
            checkForErrorAndThrow(connection);
            return connection.getInputStream();
        } catch (IOException e) {
            log.error("Http POST request failed. {}", e.getMessage());
            throw ClickHouseExceptionSpecifier.specify(e, properties.getHost(), properties.getPort());
        } finally {
            StreamUtils.close(outputStream);
        }
    }

    private void sendPostRequest(InputStream from, OutputStream to, HttpURLConnection connection)
            throws ClickHouseException {
        OutputStream requestStream = null;
        InputStream responseStream = null;

        try {
            requestStream = new DataOutputStream(connection.getOutputStream());
            if (properties.isDecompress()) {
                requestStream = new ClickHouseLZ4OutputStream(requestStream, properties.getMaxCompressBufferSize());
            }
            StreamUtils.copy(from, requestStream);
            requestStream.flush();
            checkForErrorAndThrow(connection);

            responseStream = properties.isCompress()
                    ? new ClickHouseLZ4Stream(connection.getInputStream())
                    : connection.getInputStream();

            if (to != null) {
                StreamUtils.copy(responseStream, to);
                to.flush();
            }
        } catch (IOException e) {
            log.error("Http POST request failed. {}", e.getMessage());
            throw ClickHouseExceptionSpecifier.specify(e, properties.getHost(), properties.getPort());
        } finally {
            StreamUtils.close(from);
            StreamUtils.close(requestStream);
            StreamUtils.close(responseStream);
        }
    }

    private byte[] buildMultipartData(List<ClickHouseExternalData> externalData, String boundaryString)
            throws ClickHouseException {
        ByteArrayOutputStream requestBodyStream = new ByteArrayOutputStream();
        BufferedWriter httpRequestBodyWriter =
                new BufferedWriter(new OutputStreamWriter(requestBodyStream));

        try {
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
        } finally {
            StreamUtils.close(requestBodyStream);
            StreamUtils.close(httpRequestBodyWriter);
        }
    }

    private HttpURLConnection buildConnection(URI uri) throws ClickHouseException {
        try {
            URL url = uri.toURL();
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setInstanceFollowRedirects(true);
            connection.setRequestMethod("POST");
            connection.setDoInput(true);
            connection.setDoOutput(true);

            connection.setConnectTimeout(properties.getConnectionTimeout());

            setDefaultHeaders(connection);

            if (connection instanceof HttpsURLConnection) {
                configureHttps((HttpsURLConnection) connection);
            }

            return connection;
        } catch (IOException e) {
            log.error("Can't build connection. {}", e.getMessage());
            throw ClickHouseExceptionSpecifier.specify(e, properties.getHost(), properties.getPort());
        }
    }

    private void setDefaultHeaders(HttpURLConnection connection) {
        connection.addRequestProperty("Authorization", properties.getHttpAuthorization());
        connection.addRequestProperty("Content-Type", "text/plain; charset=UTF-8");
    }

    private void configureHttps(HttpsURLConnection connection) throws ClickHouseException {

        if (properties.getSsl()) {
            try {
                SSLContext sslContext = getSSLContext();
                HostnameVerifier verifier = "strict".equals(properties.getSslMode())
                        ? HttpsURLConnection.getDefaultHostnameVerifier()
                        : TrustAllHostnameVerifier.getInstance();

                connection.setHostnameVerifier(verifier);

                SSLSocketFactory socketFactory = sslContext.getSocketFactory();
                connection.setSSLSocketFactory(socketFactory);
            } catch (Exception e) {
                throw ClickHouseExceptionSpecifier.specify(e, properties.getHost(), properties.getPort());
            }

        }
    }

    private void checkForErrorAndThrow(HttpURLConnection connection)
            throws IOException, ClickHouseException {
        if (connection.getResponseCode() != HttpURLConnection.HTTP_OK) {
            InputStream messageStream = connection.getErrorStream();

            byte[] bytes = StreamUtils.toByteArray(messageStream);
            if (properties.isCompress()) {
                try {
                    messageStream = new ClickHouseLZ4Stream(new ByteArrayInputStream(bytes));
                    bytes = StreamUtils.toByteArray(messageStream);
                } catch (IOException e) {
                    log.warn("Error while read compressed stream. {}", e.getMessage());
                }
            }

            StreamUtils.close(messageStream);
            String chMessage = new String(bytes, StandardCharsets.UTF_8);
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
