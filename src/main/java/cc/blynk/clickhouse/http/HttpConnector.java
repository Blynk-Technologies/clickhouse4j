package cc.blynk.clickhouse.http;

import cc.blynk.clickhouse.ClickHouseExternalData;
import cc.blynk.clickhouse.except.ClickHouseException;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URI;
import java.sql.SQLException;
import java.util.List;

public interface HttpConnector {

    InputStream post(String sql, URI uri) throws ClickHouseException;

    InputStream post(List<ClickHouseExternalData> externalData, URI uri) throws ClickHouseException;

    void post(byte[] bytes, URI uri) throws ClickHouseException;

    void post(byte[] sqlBytes, int batchSize, ByteArrayOutputStream data, URI uri) throws ClickHouseException;

    void cleanConnections();

    void closeClient() throws SQLException;
}
