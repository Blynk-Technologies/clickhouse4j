package cc.blynk.clickhouse.integration.json;

import cc.blynk.clickhouse.ClickHouseDataSource;
import cc.blynk.clickhouse.ClickHouseStatement;
import cc.blynk.clickhouse.domain.ClickHouseDataType;
import cc.blynk.clickhouse.except.ClickHouseException;
import cc.blynk.clickhouse.settings.ClickHouseProperties;
import cc.blynk.clickhouse.settings.ClickHouseQueryParam;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;

public class JsonPreparedStatementTest {

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
    public void basicJsonTest() throws Exception {
        Statement statement = connection.createStatement();
        statement.execute("DROP TABLE IF EXISTS test.json_test");

        statement.execute(
                "CREATE TABLE IF NOT EXISTS test.json_test (created DateTime, value Int32) ENGINE = TinyLog"
        );

        try (PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO test.json_test (created, value) VALUES (?, ?)")) {
            ps.setLong(1, System.currentTimeMillis() / 1000);
            ps.setInt(2, 1);
            ps.addBatch();

            ps.setLong(1, System.currentTimeMillis() / 1000);
            ps.setInt(2, 2);
            ps.addBatch();
            ps.executeBatch();
        }

        ResultSet rs = connection.createStatement().executeQuery("SELECT * FROM test.json_test FORMAT JSON");
        rs.next();

        String json = rs.getString("json");
        assertNotNull(json);
        assertFalse(rs.next());
        System.out.println(json);

        ObjectMapper objectMapper = new ObjectMapper();
        ClickHouseJsonResponse<DateTimeIntEntry> clickHouseJsonResponse =
                objectMapper.readValue(json, new TypeReference<ClickHouseJsonResponse<DateTimeIntEntry>>() {
                });

        assertNotNull(clickHouseJsonResponse);

        ClickHouseColumnHeader[] columnHeaders = clickHouseJsonResponse.getMeta();
        assertNotNull(columnHeaders);
        assertEquals(2, columnHeaders.length);

        ClickHouseColumnHeader createdHeader = columnHeaders[0];
        assertEquals("created", createdHeader.getName());
        assertEquals(ClickHouseDataType.DateTime, createdHeader.getType());

        ClickHouseColumnHeader valueHeader = columnHeaders[1];
        assertEquals("value", valueHeader.getName());
        assertEquals(ClickHouseDataType.Int32, valueHeader.getType());

        assertEquals(2, clickHouseJsonResponse.getRows());

        ClickHouseJsonResponseStatistics statistics = clickHouseJsonResponse.getStatistics();
        assertEquals(2, statistics.getRowsRead());
        assertEquals(16, statistics.getBytesRead());

        DateTimeIntEntry[] entries = clickHouseJsonResponse.getData();
        assertNotNull(entries);
        assertEquals(2, entries.length);
        assertEquals(1, entries[0].value);
        assertEquals(2, entries[1].value);
    }

    @Test
    public void basicBigJsonTest() throws Exception {
        Statement statement = connection.createStatement();
        statement.execute("DROP TABLE IF EXISTS test.json_test");

        statement.execute(
                "CREATE TABLE IF NOT EXISTS test.json_test (created DateTime, value Int32) ENGINE = TinyLog"
        );

        try (PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO test.json_test (created, value) VALUES (?, ?)")) {

            long now = System.currentTimeMillis() / 1000;
            for (int i = 0; i < 1_000_000; i++) {
                ps.setLong(1, now);
                ps.setInt(2, i);
                ps.addBatch();
            }
            ps.executeBatch();
        }

        ResultSet rs = connection.createStatement().executeQuery("SELECT * FROM test.json_test FORMAT JSON");
        rs.next();

        String json = rs.getString("json");
        assertNotNull(json);
        assertFalse(rs.next());

        ObjectMapper objectMapper = new ObjectMapper();
        ClickHouseJsonResponse<DateTimeIntEntry> clickHouseJsonResponse =
                objectMapper.readValue(json, new TypeReference<ClickHouseJsonResponse<DateTimeIntEntry>>() {
                });

        assertNotNull(clickHouseJsonResponse);

        ClickHouseColumnHeader[] columnHeaders = clickHouseJsonResponse.getMeta();
        assertNotNull(columnHeaders);
        assertEquals(2, columnHeaders.length);

        ClickHouseColumnHeader createdHeader = columnHeaders[0];
        assertEquals("created", createdHeader.getName());
        assertEquals(ClickHouseDataType.DateTime, createdHeader.getType());

        ClickHouseColumnHeader valueHeader = columnHeaders[1];
        assertEquals("value", valueHeader.getName());
        assertEquals(ClickHouseDataType.Int32, valueHeader.getType());

        assertEquals(1_000_000, clickHouseJsonResponse.getRows());

        ClickHouseJsonResponseStatistics statistics = clickHouseJsonResponse.getStatistics();
        assertEquals(1_000_000, statistics.getRowsRead());
        assertEquals(8 * 1_000_000, statistics.getBytesRead());

        DateTimeIntEntry[] entries = clickHouseJsonResponse.getData();
        assertNotNull(entries);
        assertEquals(1_000_000, entries.length);
    }

    @Test(expectedExceptions = ClickHouseException.class)
    public void testJsonThrowsError() throws Exception {
        Statement statement = connection.createStatement();
        statement.execute("DROP TABLE IF EXISTS test.json_test");

        statement.execute(
                "CREATE TABLE IF NOT EXISTS test.json_test (created DateTime, value Int32) ENGINE = TinyLog"
        );

        try (PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO test.json_test (created, value) VALUES (?, ?)")) {
            ps.setLong(1, System.currentTimeMillis() / 1000);
            ps.setInt(2, 1);
            ps.addBatch();

            ps.setLong(1, System.currentTimeMillis() / 1000);
            ps.setInt(2, 2);
            ps.addBatch();
            ps.executeBatch();
        }

        try {
            ResultSet rs = connection.createStatement().executeQuery("SELECT x FROM test.json_test FORMAT JSON");
        } catch (ClickHouseException che) {
            assertTrue(che.getMessage().contains(
                    "Code: 47, e.displayText() = DB::Exception: Missing columns: 'x' while processing query"));
            throw che;
        }
    }

    @Test
    public void basicJsonCompactTest() throws Exception {
        Statement statement = connection.createStatement();
        statement.execute("DROP TABLE IF EXISTS test.json_test");

        statement.execute(
                "CREATE TABLE IF NOT EXISTS test.json_test (created DateTime, value Int32) ENGINE = TinyLog"
        );

        try (PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO test.json_test (created, value) VALUES (?, ?)")) {
            ps.setLong(1, System.currentTimeMillis() / 1000);
            ps.setInt(2, 1);
            ps.addBatch();

            ps.setLong(1, System.currentTimeMillis() / 1000);
            ps.setInt(2, 2);
            ps.addBatch();
            ps.executeBatch();
        }

        ResultSet rs = connection.createStatement().executeQuery("SELECT * FROM test.json_test FORMAT JSONCompact");
        rs.next();

        String json = rs.getString("json");
        assertNotNull(json);
        assertFalse(rs.next());
        System.out.println(json);

        ObjectMapper objectMapper = new ObjectMapper();
        ClickHouseJsonResponse<Object[]> clickHouseJsonResponse =
                objectMapper.readValue(json, new TypeReference<ClickHouseJsonResponse<Object[]>>() {
                });

        assertNotNull(clickHouseJsonResponse);

        ClickHouseColumnHeader[] columnHeaders = clickHouseJsonResponse.getMeta();
        assertNotNull(columnHeaders);
        assertEquals(2, columnHeaders.length);

        ClickHouseColumnHeader createdHeader = columnHeaders[0];
        assertEquals("created", createdHeader.getName());
        assertEquals(ClickHouseDataType.DateTime, createdHeader.getType());

        ClickHouseColumnHeader valueHeader = columnHeaders[1];
        assertEquals("value", valueHeader.getName());
        assertEquals(ClickHouseDataType.Int32, valueHeader.getType());

        assertEquals(2, clickHouseJsonResponse.getRows());

        ClickHouseJsonResponseStatistics statistics = clickHouseJsonResponse.getStatistics();
        assertEquals(2, statistics.getRowsRead());
        assertEquals(16, statistics.getBytesRead());

        Object[] entries = clickHouseJsonResponse.getData();
        assertNotNull(entries);
        assertEquals(2, entries.length);
        assertEquals(Integer.valueOf(1), ((Object[]) entries[0])[1]);
        assertEquals(Integer.valueOf(2), ((Object[]) entries[1])[1]);
    }

    @Test
    public void jsonWith_output_format_json_quote_64bit_integers() throws Exception {
        Statement statement = connection.createStatement();
        statement.execute("DROP TABLE IF EXISTS test.json_test");

        statement.execute(
                "CREATE TABLE IF NOT EXISTS test.json_test (created DateTime, value Int32) ENGINE = TinyLog"
        );

        long now = System.currentTimeMillis() / 1000 * 1000;
        try (PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO test.json_test (created, value) VALUES (?, ?)")) {
            ps.setLong(1, now / 1000);
            ps.setInt(2, 1);
            ps.addBatch();

            ps.setLong(1, now / 1000);
            ps.setInt(2, 2);
            ps.addBatch();
            ps.executeBatch();
        }

        Map<ClickHouseQueryParam, String> params = new HashMap<>();
        params.put(ClickHouseQueryParam.OUTPUT_FORMAT_JSON_QUOTE_64BIT_INTEGERS, "false");
        ResultSet rs = ((ClickHouseStatement) connection.createStatement()).executeQuery(
                "SELECT toUnixTimestamp(created) * 1000 as created, value FROM test.json_test FORMAT JSON", params);
        rs.next();

        String json = rs.getString("json");
        assertNotNull(json);
        assertFalse(rs.next());
        System.out.println(json);

        ObjectMapper objectMapper = new ObjectMapper();
        ClickHouseJsonResponse<LongIntEntry> clickHouseJsonResponse =
                objectMapper.readValue(json, new TypeReference<ClickHouseJsonResponse<LongIntEntry>>() {
                });

        assertNotNull(clickHouseJsonResponse);

        ClickHouseColumnHeader[] columnHeaders = clickHouseJsonResponse.getMeta();
        assertNotNull(columnHeaders);
        assertEquals(2, columnHeaders.length);

        ClickHouseColumnHeader createdHeader = columnHeaders[0];
        assertEquals("created", createdHeader.getName());
        assertEquals(ClickHouseDataType.UInt64, createdHeader.getType());

        ClickHouseColumnHeader valueHeader = columnHeaders[1];
        assertEquals("value", valueHeader.getName());
        assertEquals(ClickHouseDataType.Int32, valueHeader.getType());

        assertEquals(2, clickHouseJsonResponse.getRows());

        ClickHouseJsonResponseStatistics statistics = clickHouseJsonResponse.getStatistics();
        assertEquals(2, statistics.getRowsRead());
        assertEquals(16, statistics.getBytesRead());

        LongIntEntry[] entries = clickHouseJsonResponse.getData();
        assertNotNull(entries);
        assertEquals(2, entries.length);
        assertEquals(1, entries[0].value);
        assertEquals(now, entries[0].created);

        assertEquals(2, entries[1].value);
        assertEquals(now, entries[1].created);
    }
}
