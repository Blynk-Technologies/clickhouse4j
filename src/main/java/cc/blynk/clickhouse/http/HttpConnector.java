package cc.blynk.clickhouse.http;

import cc.blynk.clickhouse.except.ClickHouseException;

import java.net.HttpURLConnection;
import java.net.URI;

public interface HttpConnector {

    void close();

    HttpURLConnection buildConnection(URI uri) throws ClickHouseException;

}
