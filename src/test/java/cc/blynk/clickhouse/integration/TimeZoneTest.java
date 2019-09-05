package cc.blynk.clickhouse.integration;

import cc.blynk.clickhouse.ClickHouseConnection;
import cc.blynk.clickhouse.ClickHouseDataSource;
import cc.blynk.clickhouse.settings.ClickHouseProperties;
import cc.blynk.clickhouse.util.ClickHouseRowBinaryStream;
import cc.blynk.clickhouse.util.ClickHouseStreamCallback;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

public class TimeZoneTest {
    private ClickHouseConnection connectionServerTz;
    private ClickHouseConnection connectionManualTz;
    private long currentTime = 1000 * (System.currentTimeMillis() / 1000);

    @BeforeTest
    public void setUp() throws Exception {
        ClickHouseDataSource datasourceServerTz = new ClickHouseDataSource("jdbc:clickhouse://localhost:8123",
                                                                           new ClickHouseProperties());
        connectionServerTz = datasourceServerTz.getConnection();
        TimeZone serverTimeZone = connectionServerTz.getTimeZone();
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setUseServerTimeZone(false);
        int serverTimeZoneOffsetHours = (int) TimeUnit.MILLISECONDS.toHours(serverTimeZone.getOffset(currentTime));
        int manualTimeZoneOffsetHours = serverTimeZoneOffsetHours - 1;
        properties.setUseTimeZone("GMT" + (manualTimeZoneOffsetHours > 0 ? "+" : "")
                                          + manualTimeZoneOffsetHours + ":00");
        ClickHouseDataSource dataSourceManualTz = new ClickHouseDataSource("jdbc:clickhouse://localhost:8123",
                                                                           properties);
        connectionManualTz = dataSourceManualTz.getConnection();

        connectionServerTz.createStatement().execute("CREATE DATABASE IF NOT EXISTS test");
    }

    @Test
    public void timeZoneTest() throws Exception {

        connectionServerTz.createStatement().execute("DROP TABLE IF EXISTS test.time_zone_test");
        connectionServerTz.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS test.time_zone_test (i Int32, d DateTime) ENGINE = TinyLog"
        );

        PreparedStatement statement = connectionServerTz.prepareStatement(
                "INSERT INTO test.time_zone_test (i, d) VALUES (?, ?)");
        statement.setInt(1, 1);
        statement.setTimestamp(2, new Timestamp(currentTime));
        statement.execute();

        PreparedStatement statementUtc = connectionManualTz.prepareStatement(
                "INSERT INTO test.time_zone_test (i, d) VALUES (?, ?)");
        statementUtc.setInt(1, 2);
        statementUtc.setTimestamp(2, new Timestamp(currentTime));
        statementUtc.execute();

        ResultSet rs = connectionServerTz.createStatement().executeQuery(
                "SELECT i, d as cnt from test.time_zone_test order by i");
        // server write, server read
        rs.next();
        Assert.assertEquals(rs.getTime(2).getTime(), currentTime);

        // manual write, server read
        rs.next();
        Assert.assertEquals(rs.getTime(2).getTime(), currentTime - TimeUnit.HOURS.toMillis(1));

        ResultSet rsMan = connectionManualTz.createStatement().executeQuery(
                "SELECT i, d as cnt from test.time_zone_test order by i");
        // server write, manual read
        rsMan.next();
        Assert.assertEquals(rsMan.getTime(2).getTime(), currentTime + TimeUnit.HOURS.toMillis(1));
        // manual write, manual read
        rsMan.next();
        Assert.assertEquals(rsMan.getTime(2).getTime(), currentTime);
    }

    @Test
    public void dateTest() throws SQLException {
        ResultSet rsMan = connectionManualTz.createStatement().executeQuery("select toDate('2017-07-05')");
        ResultSet rsSrv = connectionServerTz.createStatement().executeQuery("select toDate('2017-07-05')");
        rsMan.next();
        rsSrv.next();
        // check it doesn't depend on server timezone
        Assert.assertEquals(rsMan.getDate(1), rsSrv.getDate(1));

        // check it is start of correct day in client timezone
        Assert.assertEquals(rsMan.getDate(1), new Date(117, 6, 5));
    }

    @Test
    public void dateInsertTest() throws SQLException {
        connectionServerTz.createStatement().execute("DROP TABLE IF EXISTS test.date_insert");
        connectionServerTz.createStatement().execute(
                "CREATE TABLE test.date_insert (" +
                        "i UInt8," +
                        "d Date" +
                        ") ENGINE = TinyLog"
        );


        final Date date = new Date(currentTime);
        Date localStartOfDay = withTimeAtStartOfDay(date);

        connectionServerTz.createStatement().sendRowBinaryStream(
                "INSERT INTO test.date_insert (i, d)",
                new ClickHouseStreamCallback() {
                    @Override
                    public void writeTo(ClickHouseRowBinaryStream stream) throws IOException {
                        stream.writeUInt8(1);
                        stream.writeDate(date);
                    }
                }
        );

        connectionManualTz.createStatement().sendRowBinaryStream(
                "INSERT INTO test.date_insert (i, d)",
                new ClickHouseStreamCallback() {
                    @Override
                    public void writeTo(ClickHouseRowBinaryStream stream) throws IOException {
                        stream.writeUInt8(2);
                        stream.writeDate(date);
                    }
                }
        );

        ResultSet rsMan = connectionManualTz.createStatement()
                .executeQuery("select d from test.date_insert order by i");
        ResultSet rsSrv = connectionServerTz.createStatement()
                .executeQuery("select d from test.date_insert order by i");
        rsMan.next();
        rsSrv.next();
        // inserted in server timezone
        Assert.assertEquals(rsMan.getDate(1), localStartOfDay);
        Assert.assertEquals(rsSrv.getDate(1), localStartOfDay);

        rsMan.next();
        rsSrv.next();
        // inserted in manual timezone
        Assert.assertEquals(rsMan.getDate(1), localStartOfDay);
        Assert.assertEquals(rsSrv.getDate(1), localStartOfDay);
    }

    @Test
    public void testParseColumnsWithDifferentTimeZones() throws Exception {
        connectionServerTz.createStatement().execute("DROP TABLE IF EXISTS test.fun_with_timezones");
        connectionServerTz.createStatement().execute(
                "CREATE TABLE test.fun_with_timezones (" +
                        "i         UInt8," +
                        "dt_server DateTime," +
                        "dt_berlin DateTime('Europe/Berlin')," +
                        "dt_lax    DateTime('America/Los_Angeles')" +
                        ") ENGINE = TinyLog"
        );
        connectionServerTz.createStatement().execute(
                "INSERT INTO test.fun_with_timezones (i, dt_server, dt_berlin, dt_lax) " +
                        "VALUES (42, 1557136800, 1557136800, 1557136800)");
        ResultSet rsTimestamps = connectionServerTz.createStatement().executeQuery(
                "SELECT i, toUnixTimestamp(dt_server), toUnixTimestamp(dt_berlin), toUnixTimestamp(dt_lax) " +
                        "FROM test.fun_with_timezones");
        rsTimestamps.next();
        Assert.assertEquals(rsTimestamps.getLong(2), 1557136800);
        Assert.assertEquals(rsTimestamps.getLong(3), 1557136800);
        Assert.assertEquals(rsTimestamps.getLong(4), 1557136800);

        ResultSet rsDatetimes = connectionServerTz.createStatement().executeQuery(
                "SELECT i, dt_server, dt_berlin, dt_lax " +
                        "FROM test.fun_with_timezones");
        rsDatetimes.next();
        Assert.assertEquals(rsDatetimes.getTimestamp(2).getTime(), 1557136800000L);
        Assert.assertEquals(rsDatetimes.getTimestamp(3).getTime(), 1557136800000L);
        Assert.assertEquals(rsDatetimes.getTimestamp(4).getTime(), 1557136800000L);
    }

    @Test
    public void testParseColumnsWithDifferentTimeZonesArray() throws Exception {
        connectionServerTz.createStatement().execute("DROP TABLE IF EXISTS test.fun_with_timezones_array");
        connectionServerTz.createStatement().execute(
                "CREATE TABLE test.fun_with_timezones_array (" +
                        "i         UInt8," +
                        "dt_server Array(DateTime)," +
                        "dt_berlin Array(DateTime('Europe/Berlin'))," +
                        "dt_lax    Array(DateTime('America/Los_Angeles'))" +
                        ") ENGINE = TinyLog"
        );
        connectionServerTz.createStatement().execute(
                "INSERT INTO test.fun_with_timezones_array (i, dt_server, dt_berlin, dt_lax) " +
                        "VALUES (42, [1557136800], [1557136800], [1557136800])");
        ResultSet rsTimestamps = connectionServerTz.createStatement().executeQuery(
                "SELECT i, array(toUnixTimestamp(dt_server[1])),"
                        + "array(toUnixTimestamp(dt_berlin[1])), "
                        + "array(toUnixTimestamp(dt_lax[1])) " +
                        "FROM test.fun_with_timezones_array");
        rsTimestamps.next();
        Assert.assertEquals(((long[]) rsTimestamps.getArray(2).getArray())[0], 1557136800);
        Assert.assertEquals(((long[]) rsTimestamps.getArray(3).getArray())[0], 1557136800);
        Assert.assertEquals(((long[]) rsTimestamps.getArray(4).getArray())[0], 1557136800);

        ResultSet rsDatetimes = connectionServerTz.createStatement().executeQuery(
                "SELECT i, dt_server, dt_berlin, dt_lax " +
                        "FROM test.fun_with_timezones_array");
        rsDatetimes.next();
        Assert.assertEquals(((Timestamp[]) rsDatetimes.getArray(2).getArray())[0].getTime(), 1557136800000L);
        Assert.assertEquals(((Timestamp[]) rsDatetimes.getArray(3).getArray())[0].getTime(), 1557136800000L);
        Assert.assertEquals(((Timestamp[]) rsDatetimes.getArray(4).getArray())[0].getTime(), 1557136800000L);
    }

    private static Date withTimeAtStartOfDay(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return new Date(cal.getTimeInMillis());
    }
}
