package cc.blynk.clickhouse.http;

import cc.blynk.clickhouse.settings.ClickHouseProperties;

final class DefaultConnectorFactory extends HttpConnectorFactory {

    DefaultConnectorFactory(ClickHouseProperties properties) {
        super(properties);
    }

    @Override
    public HttpConnector create() {
        return new DefaultHttpConnector(properties);
    }

}
