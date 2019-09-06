package cc.blynk.clickhouse.copy;

import cc.blynk.clickhouse.except.ClickHouseException;
import cc.blynk.clickhouse.http.HttpConnector;
import cc.blynk.clickhouse.settings.ClickHouseProperties;
import cc.blynk.clickhouse.settings.ClickHouseQueryParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CopyManagerImpl implements CopyManager {

    private static final Logger log = LoggerFactory.getLogger(CopyManagerImpl.class);

    private final HttpConnector connector;
    private final ClickHouseProperties properties;

    public CopyManagerImpl(HttpConnector connector, ClickHouseProperties properties) {
        this.connector = connector;
        this.properties = properties;
    }

    @Override
    public void copyIn(String sql, InputStream from) throws ClickHouseException {
        URI uri = buildRequestUri();
        connector.post(sql, from, uri);
    }

    @Override
    public void copyIn(String sql, InputStream from, int bufferSize) throws ClickHouseException {
        URI uri = buildRequestUri();
        BufferedInputStream bufferedStream = new BufferedInputStream(from, bufferSize);
        connector.post(sql, bufferedStream, uri);
    }

    @Override
    public void copyIn(String sql, Reader from) throws ClickHouseException {
        URI uri = buildRequestUri();
        connector.post(sql, new ReaderInputStream(from), uri);
    }

    @Override
    public void copyIn(String sql, Reader from, int bufferSize) throws ClickHouseException {
        URI uri = buildRequestUri();
        ReaderInputStream wrappedReader = new ReaderInputStream(from);
        BufferedInputStream bufferedStream = new BufferedInputStream(wrappedReader, bufferSize);
        connector.post(sql, bufferedStream, uri);
    }

    @Override
    public void copyOut(String sql, OutputStream to) throws ClickHouseException {
        URI uri = buildRequestUri();
        connector.post(sql, to, uri);
    }

    @Override
    public void copyOut(String sql, Writer to) throws ClickHouseException {
        URI uri = buildRequestUri();
        connector.post(sql, new WriterOutputStream(to), uri);
    }

    private URI buildRequestUri() {
        try {
            String query = getUrlQueryParams().stream()
                    .map(pair -> String.format("%s=%s", pair.getKey(), pair.getValue()))
                    .collect(Collectors.joining("&"));

            return new URI(properties.getSsl() ? "https" : "http",
                    null,
                    properties.getHost(),
                    properties.getPort(),
                    properties.getPath() == null || properties.getPath().isEmpty() ? "/" : properties.getPath(),
                    query,
                    null
            );
        } catch (URISyntaxException e) {
            log.error("Mailformed URL: {}", e.getMessage());
            throw new IllegalStateException("illegal configuration of db");
        }
    }

    private List<AbstractMap.SimpleImmutableEntry<String, String>> getUrlQueryParams() {
        List<AbstractMap.SimpleImmutableEntry<String, String>> result = new ArrayList<>();
        Map<ClickHouseQueryParam, String> params = properties.buildQueryParams(true);

        params.put(ClickHouseQueryParam.DATABASE, properties.getDatabase());

        for (Map.Entry<ClickHouseQueryParam, String> entry : params.entrySet()) {
            String s = entry.getValue();
            if (!(s == null || s.isEmpty())) {
                result.add(new AbstractMap.SimpleImmutableEntry<>(entry.getKey().toString(), entry.getValue()));
            }
        }

        return result;
    }
}
