package cc.blynk.clickhouse.integration.json;

import com.fasterxml.jackson.annotation.JsonProperty;

public final class ClickHouseJsonResponseStatistics {

    private double elapsed;

    @JsonProperty("rows_read")
    private long rowsRead;

    @JsonProperty("bytes_read")
    private long bytesRead;

    public ClickHouseJsonResponseStatistics() {
    }

    public double getElapsed() {
        return elapsed;
    }

    public long getRowsRead() {
        return rowsRead;
    }

    public long getBytesRead() {
        return bytesRead;
    }
}
