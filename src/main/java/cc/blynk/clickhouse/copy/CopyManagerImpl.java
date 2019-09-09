package cc.blynk.clickhouse.copy;

import cc.blynk.clickhouse.ClickHouseConnection;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.SQLException;

final class CopyManagerImpl implements CopyManager {

    private final ClickHouseConnection connection;

    CopyManagerImpl(ClickHouseConnection connection) {
        this.connection = connection;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyToDb(String sql, InputStream from) throws SQLException {
        validate(sql, from);
        connection.createStatement().sendStreamSQL(from, sql);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyToDb(String sql, InputStream from, int bufferSize) throws SQLException {
        validate(sql, from);
        BufferedInputStream bufferedStream = new BufferedInputStream(from, Math.max(32, bufferSize));
        connection.createStatement().sendStreamSQL(bufferedStream, sql);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyToDb(String sql, Reader from) throws SQLException {
        validate(sql, from);
        ReaderInputStream inputStream = new ReaderInputStream(from);
        connection.createStatement().sendStreamSQL(inputStream, sql);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyToDb(String sql, Reader from, int bufferSize) throws SQLException {
        validate(sql, from);
        ReaderInputStream inputStream = new ReaderInputStream(from);
        BufferedInputStream bufferedStream = new BufferedInputStream(inputStream, Math.max(32, bufferSize));
        connection.createStatement().sendStreamSQL(bufferedStream, sql);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyFromDb(String sql, OutputStream to) throws SQLException {
        validate(sql, to);
        connection.createStatement().sendStreamSQL(sql, to);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyFromDb(String sql, Writer to) throws SQLException {
        validate(sql, to);
        WriterOutputStream outputStream = new WriterOutputStream(to);
        connection.createStatement().sendStreamSQL(sql, outputStream);
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

    private static void validate(String sql, Object stream) throws SQLException {
        if (sql == null) {
            throw new SQLException("SQL query is null.");
        }
        if (stream == null) {
            throw new SQLException("Stream is null.");
        }
    }
}
