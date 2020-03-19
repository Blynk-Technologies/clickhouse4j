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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;

final class CopyManagerImpl implements CopyManager {

    private final Connection connection; //to allow a delegation of the connection closing
    private final ClickHouseConnection unwrappedConnection;
    private final Map<ClickHouseQueryParam, String> additionalDBParams;

    CopyManagerImpl(Connection connection) throws SQLException {
        this(connection, null);
    }

    CopyManagerImpl(Connection connection, Map<ClickHouseQueryParam, String> additionalDBParams) throws SQLException {
        this.connection = connection;
        this.unwrappedConnection = connection.unwrap(ClickHouseConnection.class);
        this.additionalDBParams = additionalDBParams;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyToDb(String sql, InputStream from) throws SQLException {
        validate(sql, from);
        unwrappedConnection.createStatement().sendStreamSQL(from, sql, additionalDBParams);
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
        unwrappedConnection.createStatement().sendStreamSQL(bufferedStream, sql, additionalDBParams);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyToDb(String sql, Reader from) throws SQLException {
        validate(sql, from);
        ReaderInputStream inputStream = new ReaderInputStream(from);
        unwrappedConnection.createStatement().sendStreamSQL(inputStream, sql, additionalDBParams);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyToDb(String sql, Reader from, int bufferSize) throws SQLException {
        validate(sql, from);
        ReaderInputStream inputStream = new ReaderInputStream(from);
        BufferedInputStream bufferedStream = new BufferedInputStream(inputStream, Math.max(32, bufferSize));
        unwrappedConnection.createStatement().sendStreamSQL(bufferedStream, sql, additionalDBParams);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyFromDb(String sql, OutputStream to) throws SQLException {
        validate(sql, to);
        unwrappedConnection.createStatement().sendStreamSQL(sql, to, additionalDBParams);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyFromDb(String sql, Writer to) throws SQLException {
        validate(sql, to);
        WriterOutputStream outputStream = new WriterOutputStream(to);
        unwrappedConnection.createStatement().sendStreamSQL(sql, outputStream, additionalDBParams);
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
    public void copyToDb(PreparedStatement preparedStatement, InputStream from) throws SQLException {
        copyToDb(getSql(preparedStatement), from);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyToDb(PreparedStatement preparedStatement, Path path) throws IOException, SQLException {
        copyToDb(getSql(preparedStatement), path);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyToDb(PreparedStatement preparedStatement, File file) throws IOException, SQLException {
        copyToDb(getSql(preparedStatement), file);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyToDb(PreparedStatement preparedStatement, InputStream from, int bufferSize)
            throws SQLException {
        copyToDb(getSql(preparedStatement), from, bufferSize);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyToDb(PreparedStatement preparedStatement, Reader from) throws SQLException {
        copyToDb(getSql(preparedStatement), from);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyToDb(PreparedStatement preparedStatement, Reader from, int bufferSize)
            throws SQLException {
        copyToDb(getSql(preparedStatement), from, bufferSize);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyFromDb(PreparedStatement preparedStatement, OutputStream to) throws SQLException {
        copyFromDb(getSql(preparedStatement), to);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyFromDb(PreparedStatement preparedStatement, Writer to) throws SQLException {
        copyFromDb(getSql(preparedStatement), to);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyFromDb(PreparedStatement preparedStatement, Path to) throws IOException, SQLException {
        copyFromDb(getSql(preparedStatement), to);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyFromDb(PreparedStatement preparedStatement, File to) throws IOException, SQLException {
        copyFromDb(getSql(preparedStatement), to);
    }

    private String getSql(PreparedStatement ps) throws SQLException {
        ClickHousePreparedStatement clickHouseStatement = ps.unwrap(ClickHousePreparedStatement.class);
        return clickHouseStatement.asSql();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws Exception {
        connection.close(); //allow to delegate connection closing
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
