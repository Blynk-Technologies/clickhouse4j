package ru.yandex.clickhouse.http;

import ru.yandex.clickhouse.settings.ClickHouseProperties;

public final class HttpConnectorFactory {

    private HttpConnectorFactory() {
    }

    public static HttpConnector getConnector(ClickHouseProperties properties) {
        return new DefaultHttpConnector(properties);
    }
}
