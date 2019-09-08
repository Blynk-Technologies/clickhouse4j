package cc.blynk.clickhouse.copy;

import cc.blynk.clickhouse.ClickHouseConnection;
import cc.blynk.clickhouse.ClickHouseDataSource;

import java.sql.SQLException;

public final class CopyManagerFactory {

    private CopyManagerFactory() {
    }

    public static CopyManager create(ClickHouseDataSource dataSource) throws SQLException {
        return new CopyManagerImpl(dataSource);
    }

    public static CsvCopyManager createCsvCopyManager(ClickHouseConnection connection) {
        return new CsvCopyManagerImpl(connection);
    }
}
