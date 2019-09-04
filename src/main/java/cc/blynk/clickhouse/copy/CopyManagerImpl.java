package cc.blynk.clickhouse.copy;

import cc.blynk.clickhouse.except.ClickHouseException;
import cc.blynk.clickhouse.http.HttpConnector;
import cc.blynk.clickhouse.settings.ClickHouseProperties;
import cc.blynk.clickhouse.settings.ClickHouseQueryParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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

    public void copyIn(String sql, InputStream from) throws ClickHouseException {
//        String sql = "INSERT INTO " + table + " FORMAT " + ClickHouseFormat.CSV.name() + "\n";
//        sendSqlWithStream(content, sql, additionalDBParams);
//
//        ByteArrayOutputStream out = new ByteArrayOutputStream();
//        try {
//            out.write(sql.getBytes(StandardCharsets.UTF_8));
//            StreamUtils.copy(stream, out);
//        } catch (IOException e) {
//            throw ClickHouseExceptionSpecifier.specify(e, properties.getHost(), properties.getPort());
//        }
//
//        URI uri = buildRequestUri(null, null, additionalDBParams, null, false);
//        connector.post(out.toByteArray(), uri);
    }

    public void copyIn(String sql, InputStream from, int bufferSize) throws ClickHouseException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public void copyIn(String sql, Reader from) throws ClickHouseException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public void copyOut(String sql, Reader from, int bufferSize) throws ClickHouseException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void copyOut(String sql, OutputStream to) throws ClickHouseException {
        URI uri = buildRequestUri();
        connector.post(sql, to, uri);
    }

    @Override
    public void copyOut(String sql, Writer to) throws ClickHouseException {
        URI uri = buildRequestUri();
        OutputStream out = new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                to.write(b);
            }
        };
        connector.post(sql, out, uri);
    }

    private URI buildRequestUri() {
        try {
            String query = getUrlQueryParams(
            ).stream()
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
