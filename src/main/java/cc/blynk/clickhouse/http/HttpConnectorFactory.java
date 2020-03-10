package cc.blynk.clickhouse.http;

import cc.blynk.clickhouse.except.ClickHouseException;
import cc.blynk.clickhouse.settings.ClickHouseProperties;

public final class HttpConnectorFactory {

    private HttpConnectorFactory() {
    }

    public static HttpConnector getConnector(ClickHouseProperties properties) {
        try {
            return new AsyncHttpConnector(properties);
        } catch (ClickHouseException e) {
            throw new RuntimeException(e);
        }
    }

}
