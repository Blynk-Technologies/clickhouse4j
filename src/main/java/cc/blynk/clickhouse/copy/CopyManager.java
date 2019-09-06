package cc.blynk.clickhouse.copy;

import cc.blynk.clickhouse.except.ClickHouseException;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.SQLException;

public interface CopyManager {

    void copyIn(String sql, InputStream from) throws SQLException;

    void copyIn(String sql, InputStream from, int bufferSize) throws SQLException;

    void copyIn(String sql, Reader from) throws SQLException;

    void copyIn(String sql, Reader from, int bufferSize) throws SQLException;

    void copyOut(String sql, OutputStream to) throws SQLException;

    void copyOut(String sql, Writer to) throws SQLException;
}
