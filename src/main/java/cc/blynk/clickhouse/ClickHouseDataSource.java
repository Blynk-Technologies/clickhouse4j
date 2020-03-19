package cc.blynk.clickhouse;

import cc.blynk.clickhouse.http.HttpConnectorFactory;
import cc.blynk.clickhouse.settings.ClickHouseProperties;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public final class ClickHouseDataSource implements DataSource, Closeable {

    protected final String url;
    private PrintWriter printWriter;
    private int loginTimeoutSeconds = 0;
    private ClickHouseProperties properties;

    public ClickHouseDataSource(String url) {
        this(url, new ClickHouseProperties());
    }

    public ClickHouseDataSource(String url, Properties info) {
        this(url, new ClickHouseProperties(info));
    }

    public ClickHouseDataSource(String url, ClickHouseProperties properties) {
        if (url == null) {
            throw new IllegalArgumentException("Incorrect ClickHouse jdbc url. It must be not null");
        }
        this.url = url;
        try {
            this.properties = ClickhouseJdbcUrlParser.parse(url, properties.asProperties());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public ClickHouseConnection getConnection() throws SQLException {
        return ClickHouseDriver.driver.connect(url, properties);
    }

    @Override
    public ClickHouseConnection getConnection(String username, String password) throws SQLException {
        return ClickHouseDriver.driver.connect(url, properties.withCredentials(username, password));
    }

    public String getHost() {
        return properties.getHost();
    }

    public int getPort() {
        return properties.getPort();
    }

    public String getDatabase() {
        return properties.getDatabase();
    }

    public String getUrl() {
        return url;
    }

    public ClickHouseProperties getProperties() {
        return properties;
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return printWriter;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        printWriter = out;
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        loginTimeoutSeconds = seconds;
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return loginTimeoutSeconds;
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }

    @Override
    public void close() throws IOException {
        HttpConnectorFactory.shutdown();
    }
}
