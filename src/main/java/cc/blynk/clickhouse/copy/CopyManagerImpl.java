package cc.blynk.clickhouse.copy;

import cc.blynk.clickhouse.ClickHouseConnection;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.SQLException;

class CopyManagerImpl implements CopyManager {

    private final ClickHouseConnection connection;

    CopyManagerImpl(ClickHouseConnection connection) {
        this.connection = connection;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyToDb(String sql, InputStream from) throws SQLException {
        connection.createStatement().sendStreamSQL(from, sql);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyToDb(String sql, InputStream from, int bufferSize) throws SQLException {
        BufferedInputStream bufferedStream = new BufferedInputStream(from, bufferSize);
        connection.createStatement().sendStreamSQL(bufferedStream, sql);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyToDb(String sql, Reader from) throws SQLException {
        ReaderInputStream inputStream = new ReaderInputStream(from);
        connection.createStatement().sendStreamSQL(inputStream, sql);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyToDb(String sql, Reader from, int bufferSize) throws SQLException {
        ReaderInputStream inputStream = new ReaderInputStream(from);
        BufferedInputStream bufferedStream = new BufferedInputStream(inputStream, bufferSize);
        connection.createStatement().sendStreamSQL(bufferedStream, sql);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyFromDb(String sql, OutputStream to) throws SQLException {
        connection.createStatement().sendStreamSQL(sql, to);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyFromDb(String sql, Writer to) throws SQLException {
        WriterOutputStream outputStream = new WriterOutputStream(to);
        connection.createStatement().sendStreamSQL(sql, outputStream);
    }
}
