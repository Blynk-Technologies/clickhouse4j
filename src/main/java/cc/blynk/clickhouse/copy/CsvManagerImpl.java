package cc.blynk.clickhouse.copy;

import cc.blynk.clickhouse.domain.ClickHouseFormat;
import cc.blynk.clickhouse.settings.ClickHouseQueryParam;

import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.Map;

final class CsvManagerImpl implements CsvManager {

    private final CopyManager copyManager;

    CsvManagerImpl(CopyManager copyManager) {
        this.copyManager = copyManager;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyToTable(String table, InputStream content) throws SQLException {
        copyToTable(table, content, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyToTable(String table,
                            InputStream content,
                            Map<ClickHouseQueryParam, String> additionalDBParams)
            throws SQLException {
        String sql = "INSERT INTO " + table + " FORMAT " + ClickHouseFormat.CSV.name();
        copyManager.copyToDb(sql, content, additionalDBParams);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyFromDb(String sql, OutputStream response) throws SQLException {
        copyFromDb(sql, response, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyFromDb(String sql,
                           OutputStream response,
                           Map<ClickHouseQueryParam, String> additionalDBParams)
            throws SQLException {
        sql = sql + " FORMAT " + ClickHouseFormat.CSV.name();
        copyManager.copyFromDb(sql, response, additionalDBParams);
    }
}
