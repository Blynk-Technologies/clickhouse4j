package cc.blynk.clickhouse.copy;

import cc.blynk.clickhouse.except.ClickHouseException;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;

public interface CopyManager {

    void copyIn(String sql, InputStream from) throws ClickHouseException;

    void copyIn(String sql, InputStream from, int bufferSize) throws ClickHouseException;

    void copyIn(String sql, Reader from) throws ClickHouseException;

    void copyIn(String sql, Reader from, int bufferSize) throws ClickHouseException;

    void copyOut(String sql, OutputStream to) throws ClickHouseException;

    void copyOut(String sql, Writer to) throws ClickHouseException;
}
