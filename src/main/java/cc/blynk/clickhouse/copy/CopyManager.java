package cc.blynk.clickhouse.copy;

import cc.blynk.clickhouse.except.ClickHouseException;

import java.io.OutputStream;
import java.io.Writer;

public interface CopyManager {

    void copyOut(String sql, OutputStream to) throws ClickHouseException;

    void copyOut(String sql, Writer to) throws ClickHouseException;
}
