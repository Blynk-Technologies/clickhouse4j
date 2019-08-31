package ru.yandex.clickhouse.integration;

import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import static org.testng.Assert.assertFalse;
import static org.testng.AssertJUnit.assertEquals;

public class ClickHousePreparedStatementRealTablesTest {

    private static int RECORDS = 10_000_000;
    private ClickHouseDataSource dataSource;
    private Connection connection;

    @BeforeTest
    public void setUp() throws Exception {
        ClickHouseProperties properties = new ClickHouseProperties();
        dataSource = new ClickHouseDataSource("jdbc:clickhouse://localhost:8123", properties);
        connection = dataSource.getConnection();
        connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS test");
    }

    @AfterTest
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void testBatchInsertOf1kkOfReocords() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.reporting_user_actions");
        connection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS test.reporting_user_actions"
                        + " (org_id UInt32, user_id UInt64, ts DateTime, command_code UInt16)"
                        + " ENGINE = TinyLog"
        );

        long start;
        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement ps = connection.prepareStatement("INSERT INTO test.reporting_user_actions"
                                                                              + " (org_id, user_id, ts, command_code)"
                                                                              + " VALUES (?, ?, ?, ?)");

            start = System.currentTimeMillis();
            for (int i = 0; i < RECORDS; i++) {
                ps.setInt(1, 1);
                ps.setLong(2, i);
                ps.setTimestamp(3, new Timestamp(start));
                ps.setInt(4, 3);

                ps.addBatch();
            }

            System.out.println("Time to prepare batch of " + RECORDS + " records "
                    + (System.currentTimeMillis() - start) + " ms.");

            ps.executeBatch();
            connection.commit();
        }
        System.out.println("Query sent. Total : " + (System.currentTimeMillis() - start));

        ResultSet rs = connection.createStatement().executeQuery("SELECT count() from test.reporting_user_actions");
        rs.next();

        assertEquals(rs.getInt("count()"), RECORDS);
        assertFalse(rs.next());
    }

    @Test
    public void testBatchInsertOf1kkOfReocordsWithoutCompression() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.reporting_user_actions");
        connection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS test.reporting_user_actions"
                        + " (org_id UInt32, user_id UInt64, ts DateTime, command_code UInt16)"
                        + " ENGINE = TinyLog"
        );
        dataSource.getProperties().setCompress(false);

        long start;
        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement ps = connection.prepareStatement("INSERT INTO test.reporting_user_actions"
                                                                       + " (org_id, user_id, ts, command_code)"
                                                                       + " VALUES (?, ?, ?, ?)");

            start = System.currentTimeMillis();
            for (int i = 0; i < RECORDS; i++) {
                ps.setInt(1, 1);
                ps.setLong(2, i);
                ps.setTimestamp(3, new Timestamp(start));
                ps.setInt(4, 3);

                ps.addBatch();
            }

            System.out.println("Time to prepare batch of " + RECORDS + " records "
                                       + (System.currentTimeMillis() - start) + " ms.");

            ps.executeBatch();
            connection.commit();
        }
        System.out.println("Query sent. Total : " + (System.currentTimeMillis() - start));

        ResultSet rs = connection.createStatement().executeQuery("SELECT count() from test.reporting_user_actions");
        rs.next();

        assertEquals(rs.getInt("count()"), RECORDS);
        assertFalse(rs.next());
    }

}
