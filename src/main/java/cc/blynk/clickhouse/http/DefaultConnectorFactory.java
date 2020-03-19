package cc.blynk.clickhouse.http;

import cc.blynk.clickhouse.settings.ClickHouseProperties;

public final class DefaultConnectorFactory extends HttpConnectorFactory {

    @Override
    public HttpConnector create(ClickHouseProperties properties) {
        return new DefaultHttpConnector(properties);
    }

    @Override
    public void close() {
        //empty because default connection factory uses HTTPUrlConnection that
        //is closed within connection.close()
    }
}
