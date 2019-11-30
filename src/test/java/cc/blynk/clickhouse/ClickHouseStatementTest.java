package cc.blynk.clickhouse;


import cc.blynk.clickhouse.domain.ClickHouseFormat;
import cc.blynk.clickhouse.http.HttpConnectorFactory;
import cc.blynk.clickhouse.settings.ClickHouseProperties;
import org.testng.annotations.Test;

import java.net.URI;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class ClickHouseStatementTest {

    @Test
    public void testClickhousify() {
        ClickHouseStatementImpl clickHouseStatement = new ClickHouseStatementImpl(null, null, null, 1);
        String sql = "SELECT ololo FROM ololoed;";
        assertEquals("SELECT ololo FROM ololoed FORMAT TabSeparatedWithNamesAndTypes",
                     clickHouseStatement.addFormat(sql, ClickHouseFormat.TabSeparatedWithNamesAndTypes));

        clickHouseStatement = new ClickHouseStatementImpl(null, null, null, 1);
        String sql2 = "SELECT ololo FROM ololoed";
        assertEquals("SELECT ololo FROM ololoed FORMAT TabSeparatedWithNamesAndTypes",
                     clickHouseStatement.addFormat(sql2, ClickHouseFormat.TabSeparatedWithNamesAndTypes));

        /*
        clickHouseStatement = new ClickHouseStatementImpl(null, null, null, 1);
        String sql3 = "SELECT ololo FROM ololoed FORMAT TabSeparatedWithNamesAndTypes";
        assertEquals("SELECT ololo FROM ololoed FORMAT TabSeparatedWithNamesAndTypes",
                     clickHouseStatement.addFormat(sql3, ClickHouseFormat.TabSeparatedWithNamesAndTypes));

        clickHouseStatement = new ClickHouseStatementImpl(null, null, null, 1);
        String sql4 = "SELECT ololo FROM ololoed FORMAT TabSeparatedWithNamesAndTypes;";
        assertEquals("SELECT ololo FROM ololoed FORMAT TabSeparatedWithNamesAndTypes;",
                     clickHouseStatement.addFormat(sql4, ClickHouseFormat.TabSeparatedWithNamesAndTypes));
         */

        clickHouseStatement = new ClickHouseStatementImpl(null, null, null, 1);
        String sql5 = "SHOW ololo FROM ololoed;";
        assertEquals("SHOW ololo FROM ololoed FORMAT TabSeparatedWithNamesAndTypes",
                     clickHouseStatement.addFormat(sql5, ClickHouseFormat.TabSeparatedWithNamesAndTypes));

        clickHouseStatement = new ClickHouseStatementImpl(null, null, null, 1);
        String sql6 = " show ololo FROM ololoed;".trim(); //trim is done within the statement
        assertEquals("show ololo FROM ololoed FORMAT TabSeparatedWithNamesAndTypes",
                     clickHouseStatement.addFormat(sql6, ClickHouseFormat.TabSeparatedWithNamesAndTypes));

        /*
        clickHouseStatement = new ClickHouseStatementImpl(null, null, null, 1);
        String sql7 = " show ololo FROM ololoed FORMAT CSVWithNames;".trim(); //trim is done within the statement
        assertEquals("show ololo FROM ololoed FORMAT CSVWithNames;",
                     clickHouseStatement.addFormat(sql7, ClickHouseFormat.TabSeparatedWithNamesAndTypes));
                     */
    }

    @Test
    public void testCredentials() {
        ClickHouseProperties properties = new ClickHouseProperties(new Properties());
        properties.setHost("localhost");
        ClickHouseProperties withCredentials = properties.withCredentials("test_user", "test_password");
        assertNotSame(withCredentials, properties);
        assertNull(properties.getUser());
        assertNull(properties.getPassword());
        assertEquals(withCredentials.getUser(), "test_user");
        assertEquals(withCredentials.getPassword(), "test_password");

        ClickHouseStatementImpl statement = new ClickHouseStatementImpl(
                HttpConnectorFactory.getConnector(properties), null, withCredentials, ResultSet.TYPE_FORWARD_ONLY
        );

        URI uri = statement.buildRequestUri(null, null, null, null, false);
        String query = uri.getQuery();
        assertTrue(query.contains("password=test_password"));
        assertTrue(query.contains("user=test_user"));
    }

    @Test
    public void testMaxExecutionTime() {
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setHost("localhost");
        properties.setMaxExecutionTime(20);
        ClickHouseStatementImpl statement = new ClickHouseStatementImpl(HttpConnectorFactory.getConnector(properties),
                                                                        null,
                                                                        properties,
                                                                        ResultSet.TYPE_FORWARD_ONLY);
        URI uri = statement.buildRequestUri(null, null, null, null, false);
        String query = uri.getQuery();
        assertTrue(query.contains("max_execution_time=20"), "max_execution_time param is missing in URL");

        statement.setQueryTimeout(10);
        uri = statement.buildRequestUri(null, null, null, null, false);
        query = uri.getQuery();
        assertTrue(query.contains("max_execution_time=10"), "max_execution_time param is missing in URL");
    }

    @Test
    public void testMaxMemoryUsage() {
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setHost("localhost");
        properties.setMaxMemoryUsage(41L);
        ClickHouseStatementImpl statement = new ClickHouseStatementImpl(HttpConnectorFactory.getConnector(properties),
                                                                        null,
                                                                        properties,
                                                                        ResultSet.TYPE_FORWARD_ONLY);

        URI uri = statement.buildRequestUri(null, null, null, null, false);
        String query = uri.getQuery();
        assertTrue(query.contains("max_memory_usage=41"), "max_memory_usage param is missing in URL");
    }

    @Test
    public void testAdditionalRequestParams() {
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setHost("localhost");
        ClickHouseStatementImpl statement = new ClickHouseStatementImpl(
                HttpConnectorFactory.getConnector(properties),
                null,
                properties,
                ResultSet.TYPE_FORWARD_ONLY
        );

        Map<String, String> params = new HashMap<>();
        params.put("cache_namespace", "aaaa");

        URI uri = statement.buildRequestUri(
                null,
                null,
                null,
                params,
                false
        );
        String query = uri.getQuery();
        assertTrue(query.contains("cache_namespace=aaaa"), "cache_namespace param is missing in URL");
    }

    @Test
    public void testIsSelect() {
        ClickHouseStatementImpl clickHouseStatement = new ClickHouseStatementImpl(null, null, null, 0);
        assertTrue(clickHouseStatement.isSelect("SELECT 42"));
        clickHouseStatement = new ClickHouseStatementImpl(null, null, null, 0);
        assertTrue(clickHouseStatement.isSelect("select 42"));
        clickHouseStatement = new ClickHouseStatementImpl(null, null, null, 0);
        assertFalse(clickHouseStatement.isSelect("selectfoo"));
        clickHouseStatement = new ClickHouseStatementImpl(null, null, null, 0);
        assertTrue(clickHouseStatement.isSelect("  SELECT foo"));
        clickHouseStatement = new ClickHouseStatementImpl(null, null, null, 0);
        assertTrue(clickHouseStatement.isSelect("WITH foo"));
        clickHouseStatement = new ClickHouseStatementImpl(null, null, null, 0);
        assertTrue(clickHouseStatement.isSelect("DESC foo"));
        clickHouseStatement = new ClickHouseStatementImpl(null, null, null, 0);
        assertTrue(clickHouseStatement.isSelect("EXISTS foo"));
        clickHouseStatement = new ClickHouseStatementImpl(null, null, null, 0);
        assertTrue(clickHouseStatement.isSelect("SHOW foo"));
        clickHouseStatement = new ClickHouseStatementImpl(null, null, null, 0);
        assertTrue(clickHouseStatement.isSelect("-- foo\n SELECT 42"));
        clickHouseStatement = new ClickHouseStatementImpl(null, null, null, 0);
        assertTrue(clickHouseStatement.isSelect("--foo\n SELECT 42"));
        clickHouseStatement = new ClickHouseStatementImpl(null, null, null, 0);
        assertFalse(clickHouseStatement.isSelect("- foo\n SELECT 42"));
        clickHouseStatement = new ClickHouseStatementImpl(null, null, null, 0);
        assertTrue(clickHouseStatement.isSelect("/* foo */ SELECT 42"));
        clickHouseStatement = new ClickHouseStatementImpl(null, null, null, 0);
        assertTrue(clickHouseStatement.isSelect("/*\n * foo\n*/\n SELECT 42"));
        clickHouseStatement = new ClickHouseStatementImpl(null, null, null, 0);
        assertFalse(clickHouseStatement.isSelect("/ foo */ SELECT 42"));
        clickHouseStatement = new ClickHouseStatementImpl(null, null, null, 0);
        assertFalse(clickHouseStatement.isSelect("-- SELECT baz\n UPDATE foo"));
        clickHouseStatement = new ClickHouseStatementImpl(null, null, null, 0);
        assertFalse(clickHouseStatement.isSelect("/* SELECT baz */\n UPDATE foo"));
        clickHouseStatement = new ClickHouseStatementImpl(null, null, null, 0);
        assertFalse(clickHouseStatement.isSelect("/*\n UPDATE foo"));
        clickHouseStatement = new ClickHouseStatementImpl(null, null, null, 0);
        assertFalse(clickHouseStatement.isSelect("/*"));
        clickHouseStatement = new ClickHouseStatementImpl(null, null, null, 0);
        assertFalse(clickHouseStatement.isSelect("/**/"));
        clickHouseStatement = new ClickHouseStatementImpl(null, null, null, 0);
        assertFalse(clickHouseStatement.isSelect(" --"));
    }

}
