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

import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.List;

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
        return null;
    }

    @Override
    public InputStream post(InputStream inputStream, URI uri) throws ClickHouseException {
        HttpURLConnection connection = buildConnection(uri);

        try (inputStream; DataOutputStream outputStream = new DataOutputStream(connection.getOutputStream())) {

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
            e.printStackTrace();
            //todo
            throw new RuntimeException(e);
//        } finally {
            //todo manage connections
//            if (connection != null) {
//                connection.disconnect();
//            }
        }
    }

    @Override
    public void cleanConnections() {

    }

    @Override
    public void closeClient() throws SQLException {

    }

    private HttpURLConnection buildConnection(URI uri) {
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
            e.printStackTrace();
            //todo
            throw new RuntimeException();
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
                    log.warn("error while read compressed stream", e.getMessage());
                }
            }

            StreamUtils.close(messageStream);
            String chMessage = new String(bytes, StandardCharsets.UTF_8);
            throw ClickHouseExceptionSpecifier.specify(chMessage, properties.getHost(), properties.getPort());
        }
    }
}
