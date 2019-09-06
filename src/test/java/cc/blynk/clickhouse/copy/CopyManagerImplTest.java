package cc.blynk.clickhouse.copy;

import cc.blynk.clickhouse.ClickHouseConnection;
import cc.blynk.clickhouse.ClickHouseDataSource;
import cc.blynk.clickhouse.settings.ClickHouseProperties;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeMethod;
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
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

public class CopyManagerImplTest {
    private ClickHouseConnection connection;
    private DateFormat dateFormat;

    private final static String CSV_WITHOUT_NAMES_EXPECTED =
            "\"date\",\"date_time\",\"string\",\"int32\",\"float64\"\n"
                    + "\"1989-01-30\",\"2016-08-12 13:21:32\",\"testString\",2147483647,42.21\n";

    @Test
    public void copyInStreamTest() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS csv_manager_test.csv_stream");
        connection.createStatement().execute(
                "CREATE TABLE csv_manager_test.csv_stream (value Int32, string_value String) ENGINE = Log()"
        );

        String string = "5,6\n1,6";
        InputStream inputStream = new ByteArrayInputStream(string.getBytes(StandardCharsets.UTF_8));

        CopyManager copyManager = CopyManagerFactory.create(connection);
        String sql = "INSERT INTO csv_manager_test.csv_stream FORMAT CSV";
        copyManager.copyIn(sql, inputStream);

        ResultSet rs = connection.createStatement().executeQuery(
                "SELECT count() AS cnt, sum(value) AS sum, uniqExact(string_value) uniq " +
                        "FROM csv_manager_test.csv_stream");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getInt("cnt"), 2);
        Assert.assertEquals(rs.getLong("sum"), 6);
        Assert.assertEquals(rs.getLong("uniq"), 1);
    }

    @Test
    public void copyInStreamBufferSizeTest() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS csv_manager_test.csv_stream");
        connection.createStatement().execute(
                "CREATE TABLE csv_manager_test.csv_stream (value Int32, string_value String) ENGINE = Log()"
        );

        String string = "5,6\n1,6";
        InputStream inputStream = new ByteArrayInputStream(string.getBytes(StandardCharsets.UTF_8));

        CopyManager copyManager = CopyManagerFactory.create(connection);
        String sql = "INSERT INTO csv_manager_test.csv_stream FORMAT CSV";
        copyManager.copyIn(sql, inputStream, 1024);

        ResultSet rs = connection.createStatement().executeQuery(
                "SELECT count() AS cnt, sum(value) AS sum, uniqExact(string_value) uniq " +
                        "FROM csv_manager_test.csv_stream");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getInt("cnt"), 2);
        Assert.assertEquals(rs.getLong("sum"), 6);
        Assert.assertEquals(rs.getLong("uniq"), 1);
    }

    @Test
    public void copyInReaderTest() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS csv_manager_test.csv_stream");
        connection.createStatement().execute(
                "CREATE TABLE csv_manager_test.csv_stream (value Int32, string_value String) ENGINE = Log()"
        );

        String string = "5,6\n1,6";
        Reader reader = new StringReader(string);

        CopyManager copyManager = CopyManagerFactory.create(connection);
        String sql = "INSERT INTO csv_manager_test.csv_stream FORMAT CSV";
        copyManager.copyIn(sql, reader);

        ResultSet rs = connection.createStatement().executeQuery(
                "SELECT count() AS cnt, sum(value) AS sum, uniqExact(string_value) uniq " +
                        "FROM csv_manager_test.csv_stream");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getInt("cnt"), 2);
        Assert.assertEquals(rs.getLong("sum"), 6);
        Assert.assertEquals(rs.getLong("uniq"), 1);
    }

    @Test
    public void copyInReaderBufferSizeTest() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS csv_manager_test.csv_stream");
        connection.createStatement().execute(
                "CREATE TABLE csv_manager_test.csv_stream (value Int32, string_value String) ENGINE = Log()"
        );

        String string = "5,6\n1,6";
        Reader reader = new StringReader(string);

        CopyManager copyManager = CopyManagerFactory.create(connection);
        String sql = "INSERT INTO csv_manager_test.csv_stream FORMAT CSV";
        copyManager.copyIn(sql, reader, 1024);

        ResultSet rs = connection.createStatement().executeQuery(
                "SELECT count() AS cnt, sum(value) AS sum, uniqExact(string_value) uniq " +
                        "FROM csv_manager_test.csv_stream");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getInt("cnt"), 2);
        Assert.assertEquals(rs.getLong("sum"), 6);
        Assert.assertEquals(rs.getLong("uniq"), 1);
    }

    @Test
    public void copyOutStreamTest() throws Exception {
        CopyManager copyManager = CopyManagerFactory.create(connection);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        copyManager.copyOut("SELECT * from csv_manager_test.insert FORMAT CSVWithNames", outputStream);
        String actual = outputStream.toString("UTF-8");
        outputStream.close();

        Assert.assertEquals(actual, CSV_WITHOUT_NAMES_EXPECTED);
    }

    @Test
    public void copyOutWriterTest() throws Exception {
        CopyManager copyManager = CopyManagerFactory.create(connection);
        StringWriter writer = new StringWriter();
        copyManager.copyOut("SELECT * from csv_manager_test.insert FORMAT CSVWithNames", writer);
        String actual = writer.getBuffer().toString();
        writer.close();

        Assert.assertEquals(actual, CSV_WITHOUT_NAMES_EXPECTED);
    }

    @BeforeMethod
    private void initData() throws SQLException, ParseException {
        connection.createStatement().execute("DROP TABLE IF EXISTS csv_manager_test.insert");
        connection.createStatement().execute(
                "CREATE TABLE csv_manager_test.insert (" +
                        "date Date," +
                        "date_time DateTime," +
                        "string String," +
                        "int32 Int32," +
                        "float64 Float64" +
                        ") ENGINE = MergeTree(date, (date), 8192)"
        );

        Date date = new Date(dateFormat.parse("1989-01-30").getTime());
        Timestamp dateTime = new Timestamp(1471008092000L); //2016-08-12 16:21:32
        String string = "testString";
        int int32 = Integer.MAX_VALUE;
        double float64 = 42.21;

        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO csv_manager_test.insert (date, date_time, string, int32, float64) " +
                        "VALUES (?, ?, ?, ?, ?)"
        );

        statement.setDate(1, date);
        statement.setTimestamp(2, dateTime);
        statement.setString(3, string);
        statement.setInt(4, int32);
        statement.setDouble(5, float64);

        statement.execute();
    }

    @BeforeTest
    public void setUp() throws Exception {
        ClickHouseProperties properties = new ClickHouseProperties();
        ClickHouseDataSource dataSource = new ClickHouseDataSource("jdbc:clickhouse://localhost:8123", properties);
        connection = dataSource.getConnection();
        connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS csv_manager_test");
        dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        dateFormat.setTimeZone(TimeZone.getDefault());
    }

    @AfterTest
    public void tearDown() throws Exception {
        connection.createStatement().execute("DROP DATABASE csv_manager_test");
    }
}
