package cc.blynk.clickhouse.copy;

import cc.blynk.clickhouse.ClickHouseConnection;
import cc.blynk.clickhouse.domain.ClickHouseFormat;
import cc.blynk.clickhouse.settings.ClickHouseQueryParam;

import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.Map;

class CsvCopyManagerImpl implements CsvCopyManager {

    private final ClickHouseConnection connection;

    CsvCopyManagerImpl(ClickHouseConnection connection) {
        this.connection = connection;
    }

    @Override
    public void copyIn(String table, InputStream content) throws SQLException {
        copyIn(table, content, null);
    }

    @Override
    public void copyIn(String table,
                       InputStream content,
                       Map<ClickHouseQueryParam, String> additionalDBParams)
            throws SQLException {
        String sql = "INSERT INTO " + table + " FORMAT " + ClickHouseFormat.CSV.name();
        connection.createStatement().sendStreamSQL(content, sql, additionalDBParams);
    }

    @Override
    public void copyOut(String table, OutputStream response) throws SQLException {
        copyOut(table, response, null);
    }

    @Override
    public void copyOut(String table,
                        OutputStream response,
                        Map<ClickHouseQueryParam, String> additionalDBParams)
            throws SQLException {
        String sql = "SELECT * FROM " + table + " FORMAT " + ClickHouseFormat.CSV.name();
        connection.createStatement().sendStreamSQL(sql, response, additionalDBParams);
    }
}
