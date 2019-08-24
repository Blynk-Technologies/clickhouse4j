package ru.yandex.clickhouse.integration;

import org.testng.Assert;
import org.testng.annotations.Test;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.except.ClickHouseException;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class ErrorsTest {

    private static final String DB_URL = "jdbc:clickhouse://localhost:8123";
    private static final String CLICK_HOUSE_EXCEPTION_MESSAGE = "ClickHouse exception, code: 60, host: localhost, port: 8123; Code: 60, e.displayText() = DB::Exception: Table test.table_not_exists doesn't exist.";

    @Test
    public void testWrongUser() {
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setUser("not_existing");
        DataSource dataSource = new ClickHouseDataSource(DB_URL, properties);
        try {
            Connection connection = dataSource.getConnection();
        } catch (Exception e) {
            Assert.assertEquals((getClickhouseException(e)).getErrorCode(), 192);
            return;
        }
        Assert.assertTrue(false, "didn' find correct error");
    }

    @Test(expectedExceptions = ClickHouseException.class)
    public void testTableNotExists() throws SQLException {
        ClickHouseProperties properties = new ClickHouseProperties();
        DataSource dataSource = new ClickHouseDataSource(DB_URL, properties);
        Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        statement.execute("select * from table_not_exists");
    }

    @Test
    public void testErrorDecompression() throws Exception {
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setCompress(true);
        DataSource dataSource = new ClickHouseDataSource(DB_URL, properties);
        Connection connection = dataSource.getConnection();

        connection.createStatement().execute("DROP TABLE IF EXISTS test.table_not_exists");

        PreparedStatement statement = connection.prepareStatement("INSERT INTO test.table_not_exists (d, s) VALUES (?, ?)");

        statement.setDate(1, new Date(System.currentTimeMillis()));
        statement.setInt(2, 1);
        try {
            statement.executeBatch();
        } catch (Exception e) {
            Assert.assertTrue(getClickhouseException(e).getMessage().startsWith(CLICK_HOUSE_EXCEPTION_MESSAGE));
            return;
        }
        Assert.assertTrue(false, "didn' find correct error");
    }

    private static ClickHouseException getClickhouseException(Exception e) {
        Throwable cause = e;
        Throwable throwable;

        while (cause != null) {
            throwable = cause;

            if (throwable instanceof ClickHouseException) {
                return (ClickHouseException) throwable;
            }

            cause = throwable.getCause();
        }

        throw new IllegalArgumentException("no ClickHouseException found");
    }
}
