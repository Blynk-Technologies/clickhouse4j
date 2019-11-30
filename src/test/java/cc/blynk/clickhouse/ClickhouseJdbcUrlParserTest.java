package cc.blynk.clickhouse;

import cc.blynk.clickhouse.settings.ClickHouseProperties;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Properties;

import static org.testng.Assert.assertEquals;

public class ClickhouseJdbcUrlParserTest {

    @Test
    public void testParseDashes() throws Exception {
        ClickHouseProperties chProps = ClickhouseJdbcUrlParser.parse(
                "jdbc:clickhouse://foo.yandex:1337/db-name-with-dash", new Properties());
        assertEquals(chProps.getDatabase(), "db-name-with-dash");
    }

    @Test
    public void testParseTrailingSlash() throws Exception {
        ClickHouseProperties chProps = ClickhouseJdbcUrlParser.parse(
                "jdbc:clickhouse://foo.yandex:1337/", new Properties());
        assertEquals(chProps.getDatabase(), "default");
    }

    @Test
    public void testParseDbInPathAndProps() throws Exception {
        ClickHouseProperties props = new ClickHouseProperties();
        props.setDatabase("database-name");
        ClickHouseProperties chProps = ClickhouseJdbcUrlParser.parse(
                "jdbc:clickhouse://foo.yandex:1337/database-name", props.asProperties());
        assertEquals(chProps.getDatabase(), "database-name");
        assertEquals(chProps.getPath(), "");
    }

    @Test
    public void testParseDbInPathAndProps2() throws Exception {
        ClickHouseProperties props = new ClickHouseProperties();
        props.setDatabase("database-name");
        props.setUsePathAsDb(false);
        ClickHouseProperties chProps = ClickhouseJdbcUrlParser.parse(
                "jdbc:clickhouse://foo.yandex:1337/database-name", props.asProperties());
        assertEquals(chProps.getDatabase(), "database-name");
        assertEquals(chProps.getPath(), "/database-name");
    }

    @Test
    public void testParsePathDefaultDb() throws Exception {
        ClickHouseProperties props = new ClickHouseProperties();
        props.setPath("/path");
        ClickHouseProperties chProps = ClickhouseJdbcUrlParser.parse(
                "jdbc:clickhouse://foo.yandex:1337/", props.asProperties());
        assertEquals(chProps.getDatabase(), "default");
        assertEquals(chProps.getPath(), "/path");
    }

    @Test
    public void testURlIsWithoutUnnecessarySlash() throws Exception {
        ClickHouseProperties props = new ClickHouseProperties();
        ClickHouseProperties chProps = ClickhouseJdbcUrlParser.parse(
                "jdbc:clickhouse://127.0.0.1:1337/dbName", props.asProperties());

        URI uri = ClickHouseUtil.buildURI(chProps, "database=dbName&compress=1");
        assertEquals("http://127.0.0.1:1337?database=dbName&compress=1", uri.toString());
    }

    @Test
    public void testParsePathDefaultDb2() throws Exception {
        ClickHouseProperties props = new ClickHouseProperties();
        props.setPath("/path");
        props.setUsePathAsDb(false);
        ClickHouseProperties chProps = ClickhouseJdbcUrlParser.parse(
                "jdbc:clickhouse://foo.yandex:1337", props.asProperties());
        assertEquals(chProps.getDatabase(), "default");
        assertEquals(chProps.getPath(), ""); //uri takes priority
    }

    @Test
    public void testParsePathAndDb() throws Exception {
        ClickHouseProperties props = new ClickHouseProperties();
        ClickHouseProperties chProps = ClickhouseJdbcUrlParser.parse(
                "jdbc:clickhouse://foo.yandex:1337/db?database=dbname", props.asProperties());
        assertEquals(chProps.getDatabase(), "db");
        assertEquals(chProps.getPath(), "");
    }

    @Test
    public void testParsePathAndDb2() throws Exception {
        ClickHouseProperties props = new ClickHouseProperties();
        props.setUsePathAsDb(false);
        ClickHouseProperties chProps = ClickhouseJdbcUrlParser.parse(
                "jdbc:clickhouse://foo.yandex:1337/db?database=dbname", props.asProperties());
        assertEquals(chProps.getDatabase(), "dbname");
        assertEquals(chProps.getPath(), "/db");
    }

}
