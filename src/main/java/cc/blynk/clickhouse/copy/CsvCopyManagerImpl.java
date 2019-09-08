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

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyToDb(String table, InputStream content) throws SQLException {
        copyToDb(table, content, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyToDb(String table,
                         InputStream content,
                         Map<ClickHouseQueryParam, String> additionalDBParams)
            throws SQLException {
        String sql = "INSERT INTO " + table + " FORMAT " + ClickHouseFormat.CSV.name();
        connection.createStatement().sendStreamSQL(content, sql, additionalDBParams);
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
        connection.createStatement().sendStreamSQL(sql, response, additionalDBParams);
    }
}
