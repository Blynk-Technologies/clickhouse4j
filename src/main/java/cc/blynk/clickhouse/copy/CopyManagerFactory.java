package cc.blynk.clickhouse.copy;

import cc.blynk.clickhouse.ClickHouseConnection;

public final class CopyManagerFactory {

    private CopyManagerFactory() {
    }

    public static CopyManager createCopyManager(ClickHouseConnection connection) {
        return new CopyManagerImpl(connection);
    }

    public static CsvCopyManager createCsvCopyManager(ClickHouseConnection connection) {
        return new CsvCopyManagerImpl(connection);
    }
}
