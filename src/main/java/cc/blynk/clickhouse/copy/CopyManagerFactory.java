package cc.blynk.clickhouse.copy;

import cc.blynk.clickhouse.ClickHouseConnection;
import cc.blynk.clickhouse.settings.ClickHouseQueryParam;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

public final class CopyManagerFactory {

    private CopyManagerFactory() {
    }

    public static CopyManager create(Connection connection) {
        return new CopyManagerImpl((ClickHouseConnection) connection);
    }

    public static CopyManager create(DataSource dataSource) throws SQLException {
        return create(dataSource.getConnection());
    }

    public static CopyManager create(Connection connection,
                                     Map<ClickHouseQueryParam, String> additionalDBParams) {
        return new CopyManagerImpl((ClickHouseConnection) connection, additionalDBParams);
    }

    public static CopyManager create(DataSource dataSource,
                                     Map<ClickHouseQueryParam, String> additionalDBParams)
            throws SQLException {
        return create(dataSource.getConnection(), additionalDBParams);
    }
}
