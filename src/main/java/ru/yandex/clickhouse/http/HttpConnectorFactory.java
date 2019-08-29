package ru.yandex.clickhouse.http;

import ru.yandex.clickhouse.settings.ClickHouseProperties;

public final class HttpConnectorFactory {

    private HttpConnectorFactory() {
    }

    //todo refactor in future for using different HTTP client implementations. Can be configured in props
    public static HttpConnector getConnector(ClickHouseProperties properties) {
        return new DefaultHttpConnector(properties);
    }
}
