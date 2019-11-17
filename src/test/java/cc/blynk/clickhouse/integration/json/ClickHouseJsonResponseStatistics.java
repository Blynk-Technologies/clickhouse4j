package cc.blynk.clickhouse.integration.json;

public final class ClickHouseJsonResponseStatistics {

    private double elapsed;

    private long rows_read;

    private long bytes_read;

    public ClickHouseJsonResponseStatistics() {
    }

    public double getElapsed() {
        return elapsed;
    }

    public long getRows_read() {
        return rows_read;
    }

    public long getBytes_read() {
        return bytes_read;
    }
}
