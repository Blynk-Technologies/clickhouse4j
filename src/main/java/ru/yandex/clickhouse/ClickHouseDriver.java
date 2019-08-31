package ru.yandex.clickhouse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.settings.ClickHouseConnectionSettings;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;
import ru.yandex.clickhouse.util.LogProxy;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * URL Format
 * <p>
 * primitive for now
 * <p>
 * jdbc:clickhouse://host:port
 * <p>
 * for example, jdbc:clickhouse://localhost:8123
 */
public final class ClickHouseDriver implements Driver {

    private static final Logger logger = LoggerFactory.getLogger(ClickHouseDriver.class);

    private static final ConcurrentMap<ClickHouseConnectionImpl, Boolean> connections = new ConcurrentHashMap<>();

    static {
        ClickHouseDriver driver = new ClickHouseDriver();
        try {
            DriverManager.registerDriver(driver);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        logger.info("Driver registered");
    }

    @Override
    public ClickHouseConnection connect(String url, Properties info) {
        return connect(url, new ClickHouseProperties(info));
    }

    ClickHouseConnection connect(String url, ClickHouseProperties properties) {
        if (!acceptsURL(url)) {
            return null;
        }
        logger.debug("Creating connection");
        ClickHouseConnectionImpl connection = new ClickHouseConnectionImpl(url, properties);
        registerConnection(connection);
        return LogProxy.wrap(ClickHouseConnection.class, connection);
    }

    private void registerConnection(ClickHouseConnectionImpl connection) {
        connections.put(connection, Boolean.TRUE);
    }

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith(ClickhouseJdbcUrlParser.JDBC_CLICKHOUSE_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        Properties copy = new Properties(info);
        Properties properties;
        try {
            properties = ClickhouseJdbcUrlParser.parse(url, copy).asProperties();
        } catch (Exception ex) {
            properties = copy;
            logger.error("could not parse url {}", url, ex);
        }

        ClickHouseQueryParam[] querySettings = ClickHouseQueryParam.values();
        ClickHouseConnectionSettings[] connectionSettings = ClickHouseConnectionSettings.values();
        DriverPropertyInfo[] result = new DriverPropertyInfo[querySettings.length + connectionSettings.length];

        int i = 0;
        for (ClickHouseQueryParam querySetting : querySettings) {
            result[i++] = querySetting.createDriverPropertyInfo(properties);
        }

        for (ClickHouseConnectionSettings connectionSetting : connectionSettings) {
            result[i++] = connectionSetting.createDriverPropertyInfo(properties);
        }

        return result;
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    /**
     * Schedules connections cleaning at a rate. Turned off by default.
     * See https://hc.apache.org/httpcomponents-client-4.5.x/tutorial/html/connmgmt.html#d5e418
     *
     * @param rate     period when checking would be performed
     * @param timeUnit time unit of rate
     */
    void scheduleConnectionsCleaning(int rate, TimeUnit timeUnit) {
        ScheduledConnectionCleaner.INSTANCE.scheduleAtFixedRate(() -> {
            try {
                for (ClickHouseConnectionImpl connection : connections.keySet()) {
                    connection.cleanConnections();
                }
            } catch (Exception e) {
                logger.error("error evicting connections: " + e);
            }
        }, 0, rate, timeUnit);
    }

    static class ScheduledConnectionCleaner {
        static final ScheduledExecutorService INSTANCE =
                Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory());

        static class DaemonThreadFactory implements ThreadFactory {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = Executors.defaultThreadFactory().newThread(r);
                thread.setDaemon(true);
                return thread;
            }
        }
    }
}
