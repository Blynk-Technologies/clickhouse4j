package cc.blynk.clickhouse.http;

import cc.blynk.clickhouse.settings.ClickHouseProperties;

public interface HttpConnectorFactory {

    BaseHttpConnector getConnector(ClickHouseProperties properties);

}
