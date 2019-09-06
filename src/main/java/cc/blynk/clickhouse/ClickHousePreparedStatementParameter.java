package cc.blynk.clickhouse;

import cc.blynk.clickhouse.util.ClickHouseValueFormatter;

public final class ClickHousePreparedStatementParameter {

    static final ClickHousePreparedStatementParameter NULL_PARAM =
        new ClickHousePreparedStatementParameter(null, false);

    private String stringValue;
    private boolean quoteNeeded;
    private boolean isRemoved;

    private ClickHousePreparedStatementParameter(String stringValue, boolean quoteNeeded, boolean isRemoved) {
        this.stringValue = replaceNullWithMarker(stringValue);
        this.quoteNeeded = quoteNeeded;
        this.isRemoved = isRemoved;
    }

    ClickHousePreparedStatementParameter(String stringValue, boolean quoteNeeded) {
        this(stringValue, quoteNeeded, false);
    }

    ClickHousePreparedStatementParameter() {
        this(null, false, true);
    }

    String getRegularValue() {
        if (ClickHouseValueFormatter.NULL_MARKER.equals(stringValue)) {
            return "null";
        }
        if (quoteNeeded) {
            return "'" + stringValue + "'";
        }
        return stringValue;
    }

    String getBatchValue() {
        return stringValue;
    }

    void update(String value, boolean quoteNeeded) {
        this.stringValue = replaceNullWithMarker(value);
        this.quoteNeeded = quoteNeeded;
        this.isRemoved = false;
    }

    boolean isRemoved() {
        return isRemoved;
    }

    void clean() {
        this.isRemoved = true;
    }

    private static String replaceNullWithMarker(String value) {
        return value == null
                ? ClickHouseValueFormatter.NULL_MARKER
                : value;
    }

    @Override
    public String toString() {
        return stringValue;
    }

}
