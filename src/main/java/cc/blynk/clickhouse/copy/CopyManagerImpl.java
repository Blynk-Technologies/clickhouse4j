package cc.blynk.clickhouse.copy;

import cc.blynk.clickhouse.ClickHouseConnection;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.SQLException;

public class CopyManagerImpl implements CopyManager {

    private final ClickHouseConnection clickHouseConnection;

    public CopyManagerImpl(ClickHouseConnection clickHouseConnection) {
        this.clickHouseConnection = clickHouseConnection;
    }

    @Override
    public void copyIn(String sql, InputStream from) throws SQLException {
        clickHouseConnection.createStatement().sendStreamSQL(from, sql);
    }

    @Override
    public void copyIn(String sql, InputStream from, int bufferSize) throws SQLException {
        BufferedInputStream bufferedStream = new BufferedInputStream(from, bufferSize);
        clickHouseConnection.createStatement().sendStreamSQL(bufferedStream, sql);
    }

    @Override
    public void copyIn(String sql, Reader from) throws SQLException {
        ReaderInputStream inputStream = new ReaderInputStream(from);
        clickHouseConnection.createStatement().sendStreamSQL(inputStream, sql);
    }

    @Override
    public void copyIn(String sql, Reader from, int bufferSize) throws SQLException {
        ReaderInputStream inputStream = new ReaderInputStream(from);
        BufferedInputStream bufferedStream = new BufferedInputStream(inputStream, bufferSize);
        clickHouseConnection.createStatement().sendStreamSQL(bufferedStream, sql);
    }

    @Override
    public void copyOut(String sql, OutputStream to) throws SQLException {
        clickHouseConnection.createStatement().sendStreamSQL(sql, to);
    }

    @Override
    public void copyOut(String sql, Writer to) throws SQLException {
        WriterOutputStream outputStream = new WriterOutputStream(to);
        clickHouseConnection.createStatement().sendStreamSQL(sql, outputStream);
    }
}
