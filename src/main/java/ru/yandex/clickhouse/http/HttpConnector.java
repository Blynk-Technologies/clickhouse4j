package ru.yandex.clickhouse.http;

import ru.yandex.clickhouse.ClickHouseExternalData;
import ru.yandex.clickhouse.except.ClickHouseException;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.util.ClickHouseFormat;
import ru.yandex.clickhouse.util.ClickHouseStreamCallback;

import java.io.InputStream;
import java.net.URI;
import java.sql.SQLException;
import java.util.List;
import java.util.TimeZone;

public interface HttpConnector {

    InputStream requestUrl(String sql,
                           List<ClickHouseExternalData> externalData,
                           URI uri)
            throws ClickHouseException;

    void sendStream(ClickHouseStreamCallback callback,
                    TimeZone timeZone,
                    ClickHouseProperties properties,
                    String sql,
                    ClickHouseFormat format,
                    URI uri)
            throws ClickHouseException;

    void sendStream(InputStream content,
                    String sql,
                    ClickHouseFormat format,
                    URI uri)
            throws ClickHouseException;

    void sendStream(List<byte[]> batchRows,
                    String sql,
                    ClickHouseFormat format,
                    URI uri)
            throws ClickHouseException;

    void cleanConnections();

    void closeClient() throws SQLException;
}
