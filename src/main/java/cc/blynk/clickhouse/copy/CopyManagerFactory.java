package cc.blynk.clickhouse.copy;

import cc.blynk.clickhouse.ClickHouseDataSource;

import java.sql.SQLException;

public final class CopyManagerFactory {

    private CopyManagerFactory() {
    }

    public static CopyManager create(ClickHouseDataSource dataSource) throws SQLException {
        return new CopyManagerImpl(dataSource);
    }

    public static CsvManager createCsvManager(ClickHouseDataSource dataSource) throws SQLException {
        CopyManager copyManager = new CopyManagerImpl(dataSource);
        return new CsvManagerImpl(copyManager);
    }
}
