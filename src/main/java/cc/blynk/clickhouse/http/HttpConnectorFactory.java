package cc.blynk.clickhouse.http;

import cc.blynk.clickhouse.settings.ClickHouseProperties;

public final class HttpConnectorFactory {

    private HttpConnectorFactory() {
    }

    public static HttpConnector getConnector(ClickHouseProperties properties) {
        return new DefaultHttpConnector(properties);
    }
}
