package ru.yandex.clickhouse.http;

import ru.yandex.clickhouse.ClickHouseExternalData;
import ru.yandex.clickhouse.except.ClickHouseException;

import java.io.InputStream;
import java.net.URI;
import java.sql.SQLException;
import java.util.List;

public interface HttpConnector {

    InputStream post(String sql,
                     URI uri)
            throws ClickHouseException;

    InputStream post(List<ClickHouseExternalData> externalData,
                     URI uri)
            throws ClickHouseException;

    void post(String sql,
              InputStream content,
              URI uri)
            throws ClickHouseException;

    void cleanConnections();

    void closeClient() throws SQLException;
}
