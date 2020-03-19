package cc.blynk.clickhouse.http;

import cc.blynk.clickhouse.settings.ClickHouseProperties;

public final class DefaultConnectorFactory extends HttpConnectorFactory {

    @Override
    public HttpConnector create(ClickHouseProperties properties) {
        return new DefaultHttpConnector(properties);
    }

}
