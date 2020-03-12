package cc.blynk.clickhouse.http;

import cc.blynk.clickhouse.settings.ClickHouseProperties;

public class DefaultConnectorFactory extends HttpConnectorFactory {

    public DefaultConnectorFactory(ClickHouseProperties properties) {
        super(properties);
    }

    @Override
    public HttpConnector create() {
        return new DefaultHttpConnector(properties);
    }

}
