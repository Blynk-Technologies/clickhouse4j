package cc.blynk.clickhouse.http;

import cc.blynk.clickhouse.ClickHouseExternalData;
import cc.blynk.clickhouse.except.ClickHouseException;
import cc.blynk.clickhouse.except.ClickHouseExceptionSpecifier;
import cc.blynk.clickhouse.settings.ClickHouseProperties;
import cc.blynk.clickhouse.util.ClickHouseLZ4InputStream;
import cc.blynk.clickhouse.util.ClickHouseLZ4OutputStream;
import cc.blynk.clickhouse.util.guava.StreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

import static cc.blynk.clickhouse.http.HttpConnectorFactory.getSSLContext;
import static java.nio.charset.StandardCharsets.UTF_8;

final class DefaultHttpConnector implements HttpConnector {

    private static final Logger log = LoggerFactory.getLogger(DefaultHttpConnector.class);

    private final ClickHouseProperties properties;

    private HttpURLConnection httpURLConnection;

    DefaultHttpConnector(ClickHouseProperties properties) {
        this.properties = properties;
    }

    @Override
    public void post(String sql, List<byte[]> data, URI uri) throws ClickHouseException {
        HttpURLConnection connection = buildConnection(uri);
        sendPostRequest(sql, data, connection);
    }

    @Override
    public InputStream post(String sql, URI uri) throws ClickHouseException {
        byte[] bytes = getSqlBytes(sql);
        HttpURLConnection connection = buildConnection(uri);
        return sendPostRequest(new ByteArrayInputStream(bytes), connection);
    }

    @Override
    public void post(String sql, InputStream from, URI uri) throws ClickHouseException {
        HttpURLConnection connection = buildConnection(uri);
        sendPostRequest(sql, from, connection);
    }

    @Override
    public InputStream post(List<ClickHouseExternalData> externalData, URI uri) throws ClickHouseException {
        String boundaryString = UUID.randomUUID().toString();
        HttpURLConnection connection = buildConnection(uri);
        connection.setRequestProperty("Content-Type", "multipart/form-data; boundary=" + boundaryString);

        byte[] bytes;
        bytes = buildMultipartData(externalData, boundaryString);
        InputStream from = new ByteArrayInputStream(bytes);

        return sendPostRequest(from, connection);
    }

    @Override
    public void close() {
        if (this.httpURLConnection != null) {
            this.httpURLConnection.disconnect();
        }
    }

    private void sendPostRequest(String sql, List<byte[]> batches, HttpURLConnection connection)
            throws ClickHouseException {
        try (OutputStream outputStream = properties.isDecompress()
                ? new ClickHouseLZ4OutputStream(connection.getOutputStream(), properties.getMaxCompressBufferSize())
                : new DataOutputStream(connection.getOutputStream())) {

            byte[] sqlBytes = getSqlBytes(sql);

            outputStream.write(sqlBytes);
            for (byte[] batch : batches) {
                outputStream.write(batch);
            }

            outputStream.flush();
            checkForErrorAndThrow(connection);
            //we have to read fully, just in case
            try (InputStream is = connection.getInputStream()) {
                StreamUtils.toByteArray(is);
            }
        } catch (IOException e) {
            log.error("Http POST request failed. {}", e.getMessage());
            throw ClickHouseExceptionSpecifier.specify(e, properties.getHost(), properties.getPort());
        }
    }

    private void sendPostRequest(String sql,
                                 InputStream from,
                                 HttpURLConnection connection) throws ClickHouseException {
        try (OutputStream requestStream = openOutputStream(connection);
             InputStream fromIS = from) {
            byte[] sqlBytes = getSqlBytes(sql);
            requestStream.write(sqlBytes);

            StreamUtils.copy(fromIS, requestStream);

            requestStream.flush();
            checkForErrorAndThrow(connection);

            //we have to read fully, just in case
            try (InputStream is = connection.getInputStream()) {
                StreamUtils.toByteArray(is);
            }
        } catch (IOException e) {
            log.error("Http POST request failed. {}", e.getMessage());
            throw ClickHouseExceptionSpecifier.specify(e, properties.getHost(), properties.getPort());
        }
    }

    private byte[] getSqlBytes(String sql) {
        if (!sql.endsWith("\n")) {
            sql += "\n";
        }
        return sql.getBytes(UTF_8);
    }

    private InputStream sendPostRequest(InputStream from, HttpURLConnection connection)
            throws ClickHouseException {
        try (OutputStream requestStream = openOutputStream(connection);
             InputStream fromIS = from) {
            StreamUtils.copy(fromIS, requestStream);
            requestStream.flush();
            checkForErrorAndThrow(connection);

            InputStream is = connection.getInputStream();
            return properties.isCompress() ? new ClickHouseLZ4InputStream(is) : is;
        } catch (IOException e) {
            log.error("Http POST request failed. {}", e.getMessage());
            throw ClickHouseExceptionSpecifier.specify(e, properties.getHost(), properties.getPort());
        }
    }

    private OutputStream openOutputStream(HttpURLConnection connection) throws IOException {
        OutputStream outputStream = connection.getOutputStream();
        if (properties.isDecompress()) {
            return new ClickHouseLZ4OutputStream(outputStream, properties.getMaxCompressBufferSize());
        } else {
            return new DataOutputStream(outputStream);
        }
    }

    private byte[] buildMultipartData(List<ClickHouseExternalData> externalData, String boundaryString)
            throws ClickHouseException {
        byte[] bytes;
        try {
            bytes = HttpConnector.buildMultipartData(externalData, boundaryString);
        }  catch (IOException e) {
            log.error("Building Multipart Body failed. {}", e.getMessage());
            throw ClickHouseExceptionSpecifier.specify(e, properties.getHost(), properties.getPort());
        }
        return bytes;
    }

    private HttpURLConnection buildConnection(URI uri) throws ClickHouseException {
        try {
            HttpURLConnection prevConnection = this.httpURLConnection;
            if (prevConnection != null) {
                prevConnection.disconnect();
            }

            URL url = uri.toURL();
            HttpURLConnection newConnection = (HttpURLConnection) url.openConnection();
            newConnection.setInstanceFollowRedirects(true);
            newConnection.setRequestMethod("POST");
            newConnection.setDoInput(true);
            newConnection.setDoOutput(true);

            newConnection.setConnectTimeout(properties.getConnectionTimeout());

            setDefaultHeaders(newConnection);

            if (newConnection instanceof HttpsURLConnection) {
                configureHttps((HttpsURLConnection) newConnection);
            }

            this.httpURLConnection = newConnection;
            return newConnection;
        } catch (IOException e) {
            log.error("Can't build connection. {}", e.getMessage());
            throw ClickHouseExceptionSpecifier.specify(e, properties.getHost(), properties.getPort());
        }
    }

    private void setDefaultHeaders(HttpURLConnection connection) {
        String httpAuthorization = properties.getHttpAuthorization();
        if (httpAuthorization != null) {
            connection.addRequestProperty("Authorization", httpAuthorization);
        }
        connection.addRequestProperty("Content-Type", "text/plain; charset=UTF-8");
    }

    private void configureHttps(HttpsURLConnection connection) throws ClickHouseException {
        if (properties.getSsl()) {
            try {
                SSLContext sslContext = getSSLContext(properties);
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
                    messageStream = new ClickHouseLZ4InputStream(new ByteArrayInputStream(bytes));
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

}
