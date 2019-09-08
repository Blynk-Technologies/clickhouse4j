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
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;

public class CsvManagerImplTest {
    private ClickHouseConnection connection;
    private ClickHouseDataSource dataSource;

    @Test
    public void copyInStreamTest() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS csv_manager_test.csv_stream");
        connection.createStatement().execute(
                "CREATE TABLE csv_manager_test.csv_stream (value Int32, string_value String) ENGINE = Log()"
        );

        String string = "5,6\n1,6";
        InputStream inputStream = new ByteArrayInputStream(string.getBytes(StandardCharsets.UTF_8));

        CsvManager copyManager = CopyManagerFactory.createCsvManager(dataSource);
        copyManager.copyToTable("csv_manager_test.csv_stream", inputStream);

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
        String expexted = initData();

        CsvManager copyManager = CopyManagerFactory.createCsvManager(dataSource);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        copyManager.copyFromDb("select * from csv_manager_test.insert", outputStream);
        String actual = outputStream.toString("UTF-8");
        outputStream.close();

        Assert.assertEquals(actual, expexted);
    }

    private String initData() throws SQLException, ParseException {
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

        Date date = new Date(1471008092000L);
        Timestamp dateTime = new Timestamp(1471008092000L); //2016-08-12 16:21:32
        String string = "testString";
        int int32 = Integer.MAX_VALUE;
        double float64 = 42.21;

        String dateString = ClickHouseValueFormatter.formatDate(date, connection.getTimeZone());
        String dateTimeString = ClickHouseValueFormatter.formatTimestamp(dateTime, connection.getTimeZone());

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

        return "\"" + dateString
                + "\",\"" + dateTimeString
                + "\",\"testString\",2147483647,42.21\n";
    }

    @BeforeTest
    public void setUp() throws Exception {
        ClickHouseProperties properties = new ClickHouseProperties();
        dataSource = new ClickHouseDataSource("jdbc:clickhouse://localhost:8123", properties);
        connection = dataSource.getConnection();
        connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS csv_manager_test");
    }

    @AfterTest
    public void tearDown() throws Exception {
        connection.createStatement().execute("DROP DATABASE csv_manager_test");
    }
}
