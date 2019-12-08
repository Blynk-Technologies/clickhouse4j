package cc.blynk.clickhouse.http;

import cc.blynk.clickhouse.settings.ClickHouseProperties;

public final class DefaultHttpConnectorFactory implements HttpConnectorFactory {

    public DefaultHttpConnectorFactory() {
    }

    @Override
    public BaseHttpConnector getConnector(ClickHouseProperties properties) {
        return new DefaultHttpConnector(properties);
    }

}
