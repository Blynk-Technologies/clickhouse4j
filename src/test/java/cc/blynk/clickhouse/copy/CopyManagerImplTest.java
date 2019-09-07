package cc.blynk.clickhouse.copy;

import cc.blynk.clickhouse.ClickHouseConnection;
import cc.blynk.clickhouse.ClickHouseDataSource;
import cc.blynk.clickhouse.settings.ClickHouseProperties;
import cc.blynk.clickhouse.util.ClickHouseValueFormatter;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;

public class CopyManagerImplTest {
    private ClickHouseConnection connection;

    private final static String CSV_HEADER = "\"date\",\"date_time\",\"string\",\"int32\",\"float64\"\n";

    @Test
    public void copyInStreamTest() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS copy_manager_test.csv_stream");
        connection.createStatement().execute(
                "CREATE TABLE copy_manager_test.csv_stream (value Int32, string_value String) ENGINE = Log()"
        );

        String string = "5,6\n1,6";
        InputStream inputStream = new ByteArrayInputStream(string.getBytes(StandardCharsets.UTF_8));

        CopyManager copyManager = CopyManagerFactory.create(connection);
        String sql = "INSERT INTO copy_manager_test.csv_stream FORMAT CSV";
        copyManager.copyIn(sql, inputStream);

        ResultSet rs = connection.createStatement().executeQuery(
                "SELECT count() AS cnt, sum(value) AS sum, uniqExact(string_value) uniq " +
                        "FROM copy_manager_test.csv_stream");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getInt("cnt"), 2);
        Assert.assertEquals(rs.getLong("sum"), 6);
        Assert.assertEquals(rs.getLong("uniq"), 1);
    }

    @Test
    public void copyInStreamBufferSizeTest() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS copy_manager_test.csv_stream");
        connection.createStatement().execute(
                "CREATE TABLE copy_manager_test.csv_stream (value Int32, string_value String) ENGINE = Log()"
        );

        String string = "5,6\n1,6";
        InputStream inputStream = new ByteArrayInputStream(string.getBytes(StandardCharsets.UTF_8));

        CopyManager copyManager = CopyManagerFactory.create(connection);
        String sql = "INSERT INTO copy_manager_test.csv_stream FORMAT CSV";
        copyManager.copyIn(sql, inputStream, 1024);

        ResultSet rs = connection.createStatement().executeQuery(
                "SELECT count() AS cnt, sum(value) AS sum, uniqExact(string_value) uniq " +
                        "FROM copy_manager_test.csv_stream");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getInt("cnt"), 2);
        Assert.assertEquals(rs.getLong("sum"), 6);
        Assert.assertEquals(rs.getLong("uniq"), 1);
    }

    @Test
    public void copyInReaderTest() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS copy_manager_test.csv_stream");
        connection.createStatement().execute(
                "CREATE TABLE copy_manager_test.csv_stream (value Int32, string_value String) ENGINE = Log()"
        );

        String string = "5,6\n1,6";
        Reader reader = new StringReader(string);

        CopyManager copyManager = CopyManagerFactory.create(connection);
        String sql = "INSERT INTO copy_manager_test.csv_stream FORMAT CSV";
        copyManager.copyIn(sql, reader);

        ResultSet rs = connection.createStatement().executeQuery(
                "SELECT count() AS cnt, sum(value) AS sum, uniqExact(string_value) uniq " +
                        "FROM copy_manager_test.csv_stream");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getInt("cnt"), 2);
        Assert.assertEquals(rs.getLong("sum"), 6);
        Assert.assertEquals(rs.getLong("uniq"), 1);
    }

    @Test
    public void copyInReaderBufferSizeTest() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS copy_manager_test.csv_stream");
        connection.createStatement().execute(
                "CREATE TABLE copy_manager_test.csv_stream (value Int32, string_value String) ENGINE = Log()"
        );

        String string = "5,6\n1,6";
        Reader reader = new StringReader(string);

        CopyManager copyManager = CopyManagerFactory.create(connection);
        String sql = "INSERT INTO copy_manager_test.csv_stream FORMAT CSV";
        copyManager.copyIn(sql, reader, 1024);

        ResultSet rs = connection.createStatement().executeQuery(
                "SELECT count() AS cnt, sum(value) AS sum, uniqExact(string_value) uniq " +
                        "FROM copy_manager_test.csv_stream");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getInt("cnt"), 2);
        Assert.assertEquals(rs.getLong("sum"), 6);
        Assert.assertEquals(rs.getLong("uniq"), 1);
    }

    @Test
    public void copyOutStreamTest() throws Exception {
        String expectedCsv = initData();
        CopyManager copyManager = CopyManagerFactory.create(connection);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        copyManager.copyOut("SELECT * from copy_manager_test.insert FORMAT CSVWithNames", outputStream);
        String actual = outputStream.toString("UTF-8");
        outputStream.close();

        Assert.assertEquals(actual, expectedCsv);
    }

    @Test
    public void copyOutWriterTest() throws Exception {
        String expectedCsv = initData();
        CopyManager copyManager = CopyManagerFactory.create(connection);
        StringWriter writer = new StringWriter();
        copyManager.copyOut("SELECT * from copy_manager_test.insert FORMAT CSVWithNames", writer);
        String actual = writer.getBuffer().toString();
        writer.close();

        Assert.assertEquals(actual, expectedCsv);
    }

    private String initData() throws SQLException, ParseException {
        connection.createStatement().execute("DROP TABLE IF EXISTS copy_manager_test.insert");
        connection.createStatement().execute(
                "CREATE TABLE copy_manager_test.insert (" +
                        "date Date," +
                        "date_time DateTime," +
                        "string String," +
                        "int32 Int32," +
                        "float64 Float64" +
                        ") ENGINE = MergeTree(date, (date), 8192)"
        );

        Date date = new Date(1471008092000L);
        Timestamp dateTime = new Timestamp(1471008092000L); //2016-08-12 16:21:32
        String string = "testString";
        int int32 = Integer.MAX_VALUE;
        double float64 = 42.21;

        String dateString = ClickHouseValueFormatter.formatDate(date, connection.getTimeZone());
        String dateTimeString = ClickHouseValueFormatter.formatTimestamp(dateTime, connection.getTimeZone());

        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO copy_manager_test.insert (date, date_time, string, int32, float64) " +
                        "VALUES (?, ?, ?, ?, ?)"
        );

        statement.setDate(1, date);
        statement.setTimestamp(2, dateTime);
        statement.setString(3, string);
        statement.setInt(4, int32);
        statement.setDouble(5, float64);

        statement.execute();

        return CSV_HEADER
                + "\"" + dateString
                + "\",\"" + dateTimeString
                + "\",\"testString\",2147483647,42.21\n";
    }

    @BeforeTest
    public void setUp() throws Exception {
        ClickHouseProperties properties = new ClickHouseProperties();
        ClickHouseDataSource dataSource = new ClickHouseDataSource("jdbc:clickhouse://localhost:8123", properties);
        connection = dataSource.getConnection();
        connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS copy_manager_test");
    }

    @AfterTest
    public void tearDown() throws Exception {
        connection.createStatement().execute("DROP DATABASE copy_manager_test");
    }
}
