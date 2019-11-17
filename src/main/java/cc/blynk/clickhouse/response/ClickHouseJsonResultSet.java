package cc.blynk.clickhouse.response;

import cc.blynk.clickhouse.ClickHouseStatement;
import cc.blynk.clickhouse.settings.ClickHouseProperties;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLType;

public final class ClickHouseJsonResultSet extends AbstractResultSet {

    // statement result set belongs to
    protected final ClickHouseStatement statement;

    protected final ClickHouseProperties properties;

    private final String json;

    private boolean hasNext;

    public ClickHouseJsonResultSet(InputStream is,
                                   int bufferSize,
                                   ClickHouseStatement statement,
                                   ClickHouseProperties properties) throws IOException {
        this.statement = statement;
        this.properties = properties;
        this.json = readInputStreamAsString(is, bufferSize);
        if (this.json.startsWith("Code: ") || !(this.json.startsWith("{") && this.json.endsWith("}\n"))) {
            throw new IOException("ClickHouse error: " + this.json);
        }
        is.close();
        this.hasNext = true;
    }

    private static String readInputStreamAsString(InputStream is, int bufferSize) throws IOException {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        byte[] buffer = new byte[bufferSize];
        int length;
        while ((length = is.read(buffer)) != -1) {
            result.write(buffer, 0, length);
        }
        return result.toString(StandardCharsets.UTF_8.name());
    }

    @Override
    public boolean next() {
        if (hasNext) {
            hasNext = false;
            return true;
        }
        return false;
    }

    @Override
    public void close() {
    }

    @Override
    public boolean isClosed() {
        return true;
    }

    @Override
    public ResultSetMetaData getMetaData() {
        return null;
    }

    /////////////////////////////////////////////////////////

    @Override
    public String getString(String column) {
        if (column.equals("json")) {
            return this.json;
        }

        throw new RuntimeException("Only 'json' field is supported");
    }

    @Override
    public void setMaxRows(int maxRows) {
    }

    @Override
    public int getRow() {
        return 0;
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) {
        return null;
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) {
        return null;
    }

    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength) {

    }

    @Override
    public void updateObject(String columnLabel,
                             Object x,
                             SQLType targetSqlType,
                             int scaleOrLength) throws SQLException {

    }

    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType) throws SQLException {

    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType) throws SQLException {

    }
}
