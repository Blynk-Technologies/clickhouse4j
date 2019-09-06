package cc.blynk.clickhouse.copy;

import cc.blynk.clickhouse.settings.ClickHouseQueryParam;

import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.Map;

public interface CsvCopyManager {

    void copyIn(String table, InputStream content) throws SQLException;

    void copyIn(String table,
                InputStream content,
                Map<ClickHouseQueryParam, String> additionalDBParams)
            throws SQLException;

    void copyOut(String table, OutputStream response) throws SQLException;

    void copyOut(String table,
                 OutputStream response,
                 Map<ClickHouseQueryParam, String> additionalDBParams)
            throws SQLException;
}
