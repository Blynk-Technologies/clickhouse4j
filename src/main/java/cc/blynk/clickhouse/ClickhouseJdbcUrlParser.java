package cc.blynk.clickhouse;

import cc.blynk.clickhouse.settings.ClickHouseProperties;
import cc.blynk.clickhouse.settings.ClickHouseQueryParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class ClickhouseJdbcUrlParser {

    private static final Logger logger = LoggerFactory.getLogger(ClickhouseJdbcUrlParser.class);

    static final String JDBC_PREFIX = "jdbc:";
    static final String JDBC_CLICKHOUSE_PREFIX = JDBC_PREFIX + "clickhouse:";
    static final String DEFAULT_DATABASE = "default";

    private static final Pattern DB_PATH_PATTERN = Pattern.compile("/([a-zA-Z0-9_*\\-]+)");

    private ClickhouseJdbcUrlParser() {
    }

    public static ClickHouseProperties parse(String jdbcUrl, Properties defaults) throws URISyntaxException {
        if (!jdbcUrl.startsWith(JDBC_CLICKHOUSE_PREFIX)) {
            throw new URISyntaxException(jdbcUrl, "'" + JDBC_CLICKHOUSE_PREFIX + "' prefix is mandatory");
        }
        return parseClickhouseUrl(jdbcUrl.substring(JDBC_PREFIX.length()), defaults);
    }

    private static ClickHouseProperties parseClickhouseUrl(String uriString, Properties defaults)
            throws URISyntaxException {
        URI uri = new URI(uriString);
        Properties urlProperties = parseUriQueryPart(uri.getQuery(), defaults);
        ClickHouseProperties props = new ClickHouseProperties(urlProperties);
        props.setHost(uri.getHost());
        int port = uri.getPort();
        if (port == -1) {
            throw new IllegalArgumentException("Port is missing or wrong");
        }
        props.setPort(port);
        String path = uri.getPath();
        String database;
        if (props.isUsePathAsDb()) {
            if (path == null || path.isEmpty() || path.equals("/")) {
                database = defaults.getProperty(ClickHouseQueryParam.DATABASE.getKey(), DEFAULT_DATABASE);
            } else {
                Matcher m = DB_PATH_PATTERN.matcher(path);
                if (m.matches()) {
                    database = m.group(1);
                } else {
                    throw new URISyntaxException("wrong database name path: '" + path + "'", uriString);
                }
            }
            props.setDatabase(database);
        } else {
            if (props.getDatabase() == null || props.getDatabase().isEmpty()) {
                props.setDatabase(DEFAULT_DATABASE);
            }
            props.setPath((path == null || path.isEmpty()) ? "" : path);
        }
        return props;
    }

    static Properties parseUriQueryPart(String query, Properties defaults) {
        if (query == null) {
            return defaults;
        }
        Properties urlProps = new Properties(defaults);
        String[] queryKeyValues = query.split("&");
        for (String keyValue : queryKeyValues) {
            String[] keyValueTokens = keyValue.split("=");
            if (keyValueTokens.length == 2) {
                urlProps.put(keyValueTokens[0], keyValueTokens[1]);
            } else {
                logger.warn("don't know how to handle parameter pair: {}", keyValue);
            }
        }
        return urlProps;
    }
}
