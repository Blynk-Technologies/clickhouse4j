package cc.blynk.clickhouse.copy;

import cc.blynk.clickhouse.ClickHouseConnection;
import cc.blynk.clickhouse.ClickHouseDataSource;
import cc.blynk.clickhouse.settings.ClickHouseQueryParam;

import java.sql.SQLException;
import java.util.Map;

public final class CopyManagerFactory {

    private CopyManagerFactory() {
    }

    public static CopyManager create(ClickHouseConnection connection) {
        return new CopyManagerImpl(connection);
    }

    public static CopyManager create(ClickHouseDataSource dataSource) throws SQLException {
        return create(dataSource.getConnection());
    }

    public static CopyManager create(ClickHouseConnection connection,
                                     Map<ClickHouseQueryParam, String> additionalDBParams) {
        return new CopyManagerImpl(connection, additionalDBParams);
    }

    public static CopyManager create(ClickHouseDataSource dataSource,
                                     Map<ClickHouseQueryParam, String> additionalDBParams)
            throws SQLException {
        return create(dataSource.getConnection(), additionalDBParams);
    }
}
