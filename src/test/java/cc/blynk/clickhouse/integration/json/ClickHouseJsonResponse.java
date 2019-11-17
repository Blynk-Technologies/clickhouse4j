package cc.blynk.clickhouse.integration.json;

/**
 * Wrapper for the JSON response from the clickhouse client.
 *
 * The Blynk Project.
 * Created by Dmitriy Dumanskiy.
 * Created on 16.11.2019.
 */
public final class ClickHouseJsonResponse<T> {

    private ClickHouseColumnHeader[] meta;

    private T[] data;

    private long rows;

    private ClickHouseJsonResponseStatistics statistics;

    public ClickHouseJsonResponse() {
    }

    public ClickHouseColumnHeader[] getMeta() {
        return meta;
    }

    public T[] getData() {
        return data;
    }

    public long getRows() {
        return rows;
    }

    public ClickHouseJsonResponseStatistics getStatistics() {
        return statistics;
    }

}
