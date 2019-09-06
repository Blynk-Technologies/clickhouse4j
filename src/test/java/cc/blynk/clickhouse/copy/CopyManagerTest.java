package cc.blynk.clickhouse.copy;

import cc.blynk.clickhouse.ClickHouseConnection;
import cc.blynk.clickhouse.ClickHouseDataSource;
import cc.blynk.clickhouse.settings.ClickHouseProperties;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.StringWriter;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

public class CopyManagerTest {
    private ClickHouseConnection connection;
    private DateFormat dateFormat;

    private final static String CSV_WITHOUT_NAMES_EXPECTED =
            "\"date\",\"date_time\",\"string\",\"int32\",\"float64\"\n"
                    + "\"1989-01-30\",\"2016-08-12 13:21:32\",\"testString\",2147483647,42.21\n";

    @Test
    public void copyOutStreamTest() throws Exception {
        CopyManager copyManager = connection.createCopyManager();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        copyManager.copyOut("SELECT * from test.insert FORMAT CSVWithNames", outputStream);
        String actual = outputStream.toString("UTF-8");
        outputStream.close();

        Assert.assertEquals(actual, CSV_WITHOUT_NAMES_EXPECTED);
    }

    @Test
    public void copyOutWriterTest() throws Exception {
        CopyManager copyManager = connection.createCopyManager();
        StringWriter writer = new StringWriter();
        copyManager.copyOut("SELECT * from test.insert FORMAT CSVWithNames", writer);
        String actual = writer.getBuffer().toString();
        writer.close();

        Assert.assertEquals(actual, CSV_WITHOUT_NAMES_EXPECTED);
    }

    @BeforeMethod
    private void initData() throws SQLException, ParseException {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.insert");
        connection.createStatement().execute(
                "CREATE TABLE test.insert (" +
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
                "INSERT INTO test.insert (date, date_time, string, int32, float64) VALUES (?, ?, ?, ?, ?)"
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
        connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS copy_manager_test");
        dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        dateFormat.setTimeZone(TimeZone.getDefault());
    }

    @AfterTest
    public void tearDown() throws Exception {
        connection.createStatement().execute("DROP DATABASE copy_manager_test");
    }
}