package cc.blynk.clickhouse.copy;

import cc.blynk.clickhouse.settings.ClickHouseQueryParam;

import java.io.InputStream;
import java.sql.SQLException;
import java.util.Map;

public interface CsvCopyManager {
    void sendCSVStream(String table, InputStream content) throws SQLException;

    void sendCSVStream(String table, InputStream content,
                       Map<ClickHouseQueryParam, String> additionalDBParams)
            throws SQLException;
}
