package cc.blynk.clickhouse.copy;

import cc.blynk.clickhouse.ClickHouseConnection;
import cc.blynk.clickhouse.ClickHousePreparedStatement;
import cc.blynk.clickhouse.settings.ClickHouseQueryParam;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;

final class CopyManagerImpl implements CopyManager {

    private final ClickHouseConnection connection;
    private final Map<ClickHouseQueryParam, String> additionalDBParams;

    CopyManagerImpl(Connection connection) throws SQLException {
        this(connection, null);
    }

    CopyManagerImpl(Connection connection, Map<ClickHouseQueryParam, String> additionalDBParams) throws SQLException {
        this.connection = connection.unwrap(ClickHouseConnection.class);
        this.additionalDBParams = additionalDBParams;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyToDb(String sql, InputStream from) throws SQLException {
        validate(sql, from);
        connection.createStatement().sendStreamSQL(from, sql, additionalDBParams);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyToDb(String sql, Path from) throws IOException, SQLException {
        validate(sql, from);
        try (InputStream inputStream = Files.newInputStream(from)) {
            copyToDb(sql, inputStream);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyToDb(String sql, File file) throws IOException, SQLException {
        validate(sql, file);
        copyToDb(sql, file.toPath());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyToDb(String sql, InputStream from, int bufferSize) throws SQLException {
        validate(sql, from);
        BufferedInputStream bufferedStream = new BufferedInputStream(from, Math.max(32, bufferSize));
        connection.createStatement().sendStreamSQL(bufferedStream, sql, additionalDBParams);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyToDb(String sql, Reader from) throws SQLException {
        validate(sql, from);
        ReaderInputStream inputStream = new ReaderInputStream(from);
        connection.createStatement().sendStreamSQL(inputStream, sql, additionalDBParams);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyToDb(String sql, Reader from, int bufferSize) throws SQLException {
        validate(sql, from);
        ReaderInputStream inputStream = new ReaderInputStream(from);
        BufferedInputStream bufferedStream = new BufferedInputStream(inputStream, Math.max(32, bufferSize));
        connection.createStatement().sendStreamSQL(bufferedStream, sql, additionalDBParams);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyFromDb(String sql, OutputStream to) throws SQLException {
        validate(sql, to);
        connection.createStatement().sendStreamSQL(sql, to, additionalDBParams);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyFromDb(String sql, Writer to) throws SQLException {
        validate(sql, to);
        WriterOutputStream outputStream = new WriterOutputStream(to);
        connection.createStatement().sendStreamSQL(sql, outputStream, additionalDBParams);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyFromDb(String sql, Path to) throws IOException, SQLException {
        validate(sql, to);
        try (OutputStream os = Files.newOutputStream(to, TRUNCATE_EXISTING)) {
            copyFromDb(sql, os);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyFromDb(String sql, File to) throws IOException, SQLException {
        validate(sql, to);
        copyFromDb(sql, to.toPath());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyToDb(ClickHousePreparedStatement preparedStatement, InputStream from) throws SQLException {
        String sql = preparedStatement.asSql();
        copyToDb(sql, from);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyToDb(ClickHousePreparedStatement preparedStatement, Path path) throws IOException, SQLException {
        String sql = preparedStatement.asSql();
        copyToDb(sql, path);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyToDb(ClickHousePreparedStatement preparedStatement, File file) throws IOException, SQLException {
        String sql = preparedStatement.asSql();
        copyToDb(sql, file);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyToDb(ClickHousePreparedStatement preparedStatement, InputStream from, int bufferSize)
            throws SQLException {
        String sql = preparedStatement.asSql();
        copyToDb(sql, from, bufferSize);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyToDb(ClickHousePreparedStatement preparedStatement, Reader from) throws SQLException {
        String sql = preparedStatement.asSql();
        copyToDb(sql, from);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyToDb(ClickHousePreparedStatement preparedStatement, Reader from, int bufferSize)
            throws SQLException {
        String sql = preparedStatement.asSql();
        copyToDb(sql, from, bufferSize);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyFromDb(ClickHousePreparedStatement preparedStatement, OutputStream to) throws SQLException {
        String sql = preparedStatement.asSql();
        copyFromDb(sql, to);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyFromDb(ClickHousePreparedStatement preparedStatement, Writer to) throws SQLException {
        String sql = preparedStatement.asSql();
        copyFromDb(sql, to);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyFromDb(ClickHousePreparedStatement preparedStatement, Path to) throws IOException, SQLException {
        String sql = preparedStatement.asSql();
        copyFromDb(sql, to);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyFromDb(ClickHousePreparedStatement preparedStatement, File to) throws IOException, SQLException {
        String sql = preparedStatement.asSql();
        copyFromDb(sql, to);
    }

    /**
     * {@inheritDoc}
     */
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
