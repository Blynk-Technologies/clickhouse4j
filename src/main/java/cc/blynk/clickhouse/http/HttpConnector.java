package cc.blynk.clickhouse.http;

import cc.blynk.clickhouse.ClickHouseExternalData;
import cc.blynk.clickhouse.except.ClickHouseException;

import java.io.InputStream;
import java.net.URI;
import java.sql.SQLException;
import java.util.List;

public interface HttpConnector {

    InputStream post(InputStream from, URI uri) throws ClickHouseException;

    InputStream post(String sql, URI uri) throws ClickHouseException;

    void post(String sql, InputStream from, URI uri) throws ClickHouseException;

    InputStream post(List<ClickHouseExternalData> externalData, URI uri) throws ClickHouseException;

    void post(String sql, List<byte[]> data, URI uri) throws ClickHouseException;

    void cleanConnections();

    void closeClient() throws SQLException;
}
