package ru.yandex.clickhouse.http.apache;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseExternalData;
import ru.yandex.clickhouse.except.ClickHouseException;
import ru.yandex.clickhouse.except.ClickHouseExceptionSpecifier;
import ru.yandex.clickhouse.except.ClickHouseUnknownException;
import ru.yandex.clickhouse.http.HttpConnector;
import ru.yandex.clickhouse.response.ClickHouseLZ4Stream;
import ru.yandex.clickhouse.response.FastByteArrayOutputStream;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.util.guava.StreamUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ApacheHttpConnectorImpl implements HttpConnector {
    private static final Logger log = LoggerFactory.getLogger(ApacheHttpConnectorImpl.class);

    private final CloseableHttpClient client;

    protected final ClickHouseProperties properties;


    public ApacheHttpConnectorImpl(ClickHouseProperties properties) {
        this.properties = properties;

        ClickHouseHttpClientBuilder clientBuilder = new ClickHouseHttpClientBuilder(this.properties);

        try {
            this.client = clientBuilder.buildClient();
        } catch (Exception e) {
            throw new IllegalStateException("cannot initialize http client", e);
        }
    }

    @Override
    public void post(InputStream stream, URI uri) throws ClickHouseException {
        //todo implement if needed
    }

    @Override
    public void post(byte[] bytes, URI uri) throws ClickHouseException {
        InputStream is = sendEntity(uri, new InputStreamEntity(new ByteArrayInputStream(bytes), -1));
        StreamUtils.close(is);
    }

    @Override
    public InputStream post(String sql, URI uri)
            throws ClickHouseException {
        HttpEntity requestEntity = new StringEntity(sql, StandardCharsets.UTF_8);
        return sendEntity(uri, requestEntity);
    }

    @Override
    public InputStream post(List<ClickHouseExternalData> externalData, URI uri)
            throws ClickHouseException {
        MultipartEntityBuilder entityBuilder = MultipartEntityBuilder.create();

        try {
            for (ClickHouseExternalData externalDataItem : externalData) {

                // clickhouse may return 400 (bad request) when chunked encoding is used with multipart request
                // so read content to byte array to avoid chunked encoding
                // TODO do not read stream into memory when this issue is fixed in clickhouse
                entityBuilder.addBinaryBody(
                        externalDataItem.getName(),
                        StreamUtils.toByteArray(externalDataItem.getContent()),
                        ContentType.APPLICATION_OCTET_STREAM,
                        externalDataItem.getName()
                );
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        HttpEntity requestEntity = entityBuilder.build();
        return sendEntity(uri, requestEntity);
    }

    private InputStream sendEntity(URI uri, HttpEntity requestEntity) throws ClickHouseException {
        HttpEntity entity = null;
        try {
            uri = followRedirects(uri);
            HttpPost post = new HttpPost(uri);
            if (properties.isDecompress()) {
                requestEntity = new LZ4EntityWrapper(requestEntity, properties.getMaxCompressBufferSize());
            }
            post.setEntity(requestEntity);
            HttpResponse response = client.execute(post);
            entity = response.getEntity();
            checkForErrorAndThrow(entity, response);

            InputStream is;
            if (entity.isStreaming()) {
                is = entity.getContent();
            } else {
                FastByteArrayOutputStream baos = new FastByteArrayOutputStream();
                entity.writeTo(baos);
                is = baos.convertToInputStream();
            }
            return is;
        } catch (ClickHouseException e) {
            throw e;
        } catch (Exception e) {
            log.info("Error during connection to {}, reporting failure to data source, message: {}",
                    properties, e.getMessage());
            EntityUtils.consumeQuietly(entity);
            throw ClickHouseExceptionSpecifier.specify(e, properties.getHost(), properties.getPort());
        }
    }

    @Override
    public void cleanConnections() {
        client.getConnectionManager().closeExpiredConnections();
        client.getConnectionManager().closeIdleConnections(2 * properties.getSocketTimeout(),
                TimeUnit.MILLISECONDS);
    }

    @Override
    public void closeClient() throws SQLException {
        try {
            client.close();
        } catch (IOException e) {
            throw new ClickHouseUnknownException("HTTP client closeClient exception",
                    e, properties.getHost(), properties.getPort());
        }
    }


    private URI followRedirects(URI uri) throws IOException, URISyntaxException {
        if (properties.isCheckForRedirects()) {
            int redirects = 0;
            while (redirects < properties.getMaxRedirects()) {
                HttpGet httpGet = new HttpGet(uri);
                HttpResponse response = client.execute(httpGet);
                if (response.getStatusLine().getStatusCode() == 307) {
                    uri = new URI(response.getHeaders("Location")[0].getValue());
                    redirects++;
                    log.info("Redirected to " + uri.getHost());
                } else {
                    break;
                }
            }
        }
        return uri;
    }

    private void checkForErrorAndThrow(HttpEntity entity, HttpResponse response)
            throws IOException, ClickHouseException {
        if (response.getStatusLine().getStatusCode() != HttpURLConnection.HTTP_OK) {
            InputStream messageStream = entity.getContent();
            byte[] bytes = StreamUtils.toByteArray(messageStream);
            if (properties.isCompress()) {
                try {
                    messageStream = new ClickHouseLZ4Stream(new ByteArrayInputStream(bytes));
                    bytes = StreamUtils.toByteArray(messageStream);
                } catch (IOException e) {
                    log.warn("error while read compressed stream {}", e.getMessage());
                }
            }
            EntityUtils.consumeQuietly(entity);
            String chMessage = new String(bytes, StandardCharsets.UTF_8);
            throw ClickHouseExceptionSpecifier.specify(chMessage, properties.getHost(), properties.getPort());
        }
    }
}
