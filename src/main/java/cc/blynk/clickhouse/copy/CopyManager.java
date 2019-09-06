package cc.blynk.clickhouse.copy;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.SQLException;

public interface CopyManager {

    /**
     * Insert data from stream to DB. Type of data in stream configured in SQL Query.
     * For example:
     * <pre>
     *     CopyManager copyManager = CopyManagerFactory.createCopyManager(connection);
     *     InputStream is = ...
     *     String sql = "INSERT INTO default.my_table FORMAT CSV";
     *     copyManager.copyIn(sql, is);
     *
     *     //Data will be written to stream in CSV format
     * </pre>
     *
     * @param sql  - SQL INSERT Query
     * @param from - InputStream with data
     * @throws SQLException - insertion error
     * @see cc.blynk.clickhouse.domain.ClickHouseFormat
     */
    void copyIn(String sql, InputStream from) throws SQLException;

    /**
     * Insert data from stream to DB. Type of data in stream configured in SQL Query.
     * For example:
     * <pre>
     *     CopyManager copyManager = CopyManagerFactory.createCopyManager(connection);
     *     InputStream is = ...
     *     String sql = "INSERT INTO default.my_table FORMAT CSV";
     *     copyManager.copyIn(sql, is, 1024);
     *
     *     //Data will be written to stream in CSV format
     * </pre>
     *
     * @param sql        - SQL INSERT Query
     * @param from       - InputStream with data
     * @param bufferSize - buffer size
     * @throws SQLException - insertion error
     * @see cc.blynk.clickhouse.domain.ClickHouseFormat
     */
    void copyIn(String sql, InputStream from, int bufferSize) throws SQLException;

    /**
     * Insert data from reader to DB. Type of data in stream configured in SQL Query.
     * For example:
     * <pre>
     *     CopyManager copyManager = CopyManagerFactory.createCopyManager(connection);
     *     Reader reader = ...
     *     String sql = "INSERT INTO default.my_table FORMAT CSV";
     *     copyManager.copyIn(sql, reader);
     *
     *     //Data will be written to stream in CSV format
     * </pre>
     *
     * @param sql  - SQL INSERT Query
     * @param from - Reader with data
     * @throws SQLException - insertion error
     * @see cc.blynk.clickhouse.domain.ClickHouseFormat
     */
    void copyIn(String sql, Reader from) throws SQLException;

    /**
     * Insert data from reader to DB. Type of data in stream configured in SQL Query.
     * For example:
     * <pre>
     *     CopyManager copyManager = CopyManagerFactory.createCopyManager(connection);
     *     Reader is = ...
     *     String sql = "INSERT INTO default.my_table FORMAT CSV";
     *     copyManager.copyIn(sql, reader, 1024);
     *
     *     //Data will be written to stream in CSV format
     * </pre>
     *
     * @param sql        - SQL INSERT Query
     * @param from       - Reader with data
     * @param bufferSize - buffer size
     * @throws SQLException - insertion error
     * @see cc.blynk.clickhouse.domain.ClickHouseFormat
     */
    void copyIn(String sql, Reader from, int bufferSize) throws SQLException;

    /**
     * Load data from db by SQL Query and write it to OutputStream in format defined in SQL query
     * For example:
     * <pre>
     *     CopyManager copyManager = CopyManagerFactory.createCopyManager(connection);
     *     OutputStream os = ...
     *     String sql = "SELECT * from default.my_table FORMAT CSVWithNames";
     *     copyManager.copyOut(sql, os);
     *
     *     // Data will be readed in CSV format
     *     // First line will contains column names in CSV format
     * </pre>
     *
     * @param sql - SQL SELECT Query
     * @param to  - OutputStream to write data
     * @throws SQLException - loading error
     * @see cc.blynk.clickhouse.domain.ClickHouseFormat
     */
    void copyOut(String sql, OutputStream to) throws SQLException;

    /**
     * Load data from db by SQL Query and write it to Writer in format defined in SQL query
     * For example:
     * <pre>
     *     CopyManager copyManager = CopyManagerFactory.createCopyManager(connection);
     *     Writer writer = ...
     *     String sql = "SELECT * from default.my_table FORMAT CSVWithNames";
     *     copyManager.copyOut(sql, writer);
     *
     *     // Data will be readed in CSV format
     *     // First line will contains column names in CSV format
     * </pre>
     *
     * @param sql - SQL SELECT Query
     * @param to  - Writer to write data
     * @throws SQLException - loading error
     * @see cc.blynk.clickhouse.domain.ClickHouseFormat
     */
    void copyOut(String sql, Writer to) throws SQLException;
}
