package ru.yandex.clickhouse.http;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseExternalData;
import ru.yandex.clickhouse.except.ClickHouseException;
import ru.yandex.clickhouse.except.ClickHouseExceptionSpecifier;
import ru.yandex.clickhouse.response.ClickHouseLZ4Stream;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.util.ClickHouseLZ4OutputStream;
import ru.yandex.clickhouse.util.guava.StreamUtils;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

public class DefaultHttpConnector implements HttpConnector {
    private static final Logger log = LoggerFactory.getLogger(DefaultHttpConnector.class);

    protected final ClickHouseProperties properties;

    DefaultHttpConnector(ClickHouseProperties properties) {
        this.properties = properties;
    }

    @Override
    public void post(byte[] bytes, URI uri) throws ClickHouseException {
        InputStream is = post(new ByteArrayInputStream(bytes), uri);
        StreamUtils.close(is);
    }

    @Override
    public InputStream post(String sql, URI uri)
            throws ClickHouseException {
        byte[] bytes = sql.getBytes(StandardCharsets.UTF_8);
        return post(new ByteArrayInputStream(bytes), uri);
    }

    @Override
    public InputStream post(List<ClickHouseExternalData> externalData, URI uri) throws ClickHouseException {
        String boundaryString = UUID.randomUUID().toString();
        HttpURLConnection connection = buildConnection(uri);
        connection.setRequestProperty("Content-Type", "multipart/form-data; boundary=" + boundaryString);

        byte[] bytes = buildMultipartData(externalData, boundaryString);
        InputStream inputStream = new ByteArrayInputStream(bytes);
        return sendPostRequest(inputStream, connection);
    }

    @Override
    public InputStream post(InputStream inputStream, URI uri) throws ClickHouseException {
        HttpURLConnection connection = buildConnection(uri);
        return sendPostRequest(inputStream, connection);
    }

    @Override
    public void cleanConnections() {

    }

    @Override
    public void closeClient() {

    }

    private InputStream sendPostRequest(InputStream inputStream, HttpURLConnection connection)
            throws ClickHouseException {
        try (inputStream;
             DataOutputStream outputStream = new DataOutputStream(connection.getOutputStream())) {
            if (properties.isDecompress()) {
                ClickHouseLZ4OutputStream lz4Stream =
                        new ClickHouseLZ4OutputStream(outputStream, properties.getMaxCompressBufferSize());

                StreamUtils.copy(inputStream, lz4Stream);
                lz4Stream.flush();
            } else {
                StreamUtils.copy(inputStream, outputStream);
            }

            checkForErrorAndThrow(connection);
            return connection.getInputStream();
        } catch (IOException e) {
            log.error("Http POST request failed.", e.getMessage());
            throw ClickHouseExceptionSpecifier.specify(e, properties.getHost(), properties.getPort());
        }
    }

    private byte[] buildMultipartData(List<ClickHouseExternalData> externalData, String boundaryString) throws ClickHouseException {
        ByteArrayOutputStream requestBodyStream = new ByteArrayOutputStream();
        BufferedWriter httpRequestBodyWriter =
                new BufferedWriter(new OutputStreamWriter(requestBodyStream));

        try (requestBodyStream; httpRequestBodyWriter) {
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
            log.error("Building Multipart Body failed.", e.getMessage());
            throw ClickHouseExceptionSpecifier.specify(e, properties.getHost(), properties.getPort());
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
            connection.setRequestProperty("Content-Type", "text/plain; charset=UTF-8");
            return connection;
        } catch (IOException e) {
            log.error("Can't build connection.", e.getMessage());
            throw ClickHouseExceptionSpecifier.specify(e, properties.getHost(), properties.getPort());
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
                    log.warn("Error while read compressed stream.", e.getMessage());
                }
            }

            StreamUtils.close(messageStream);
            String chMessage = new String(bytes, StandardCharsets.UTF_8);
            throw ClickHouseExceptionSpecifier.specify(chMessage, properties.getHost(), properties.getPort());
        }
    }
}
