package cc.blynk.clickhouse.copy;

import cc.blynk.clickhouse.settings.ClickHouseQueryParam;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.SQLException;
import java.util.Map;

public interface CopyManager {

    /**
     * Inserts the data from the stream to the DB.
     * The type and structure of the data from the stream specified in the SQL Query.
     * For example, in case you want to send the data in the CSV format to the DB you need to:
     * <pre>
     *     InputStream sourceInputStream = ...
     *     String sql = "INSERT INTO default.my_table FORMAT CSV";
     *     CopyManager copyManager = CopyManagerFactory.create(connection);
     *     copyManager.copyToDb(sql, sourceInputStream);
     * </pre>
     *
     * @param sql - SQL INSERT Query
     * @param from - InputStream with data
     * @throws SQLException - insertion error
     * @see cc.blynk.clickhouse.domain.ClickHouseFormat for possible formats of the data
     */
    void copyToDb(String sql, InputStream from) throws SQLException;

    /**
     * Inserts the data from the stream to the DB.
     * The type and structure of the data from the stream specified in the SQL Query.
     * For example, in case you want to send the data in the CSV format to the DB you need to:
     * <pre>
     *     InputStream sourceInputStream = ...
     *     String sql = "INSERT INTO default.my_table FORMAT CSV";
     *     CopyManager copyManager = CopyManagerFactory.create(connection);
     *     copyManager.copyToDb(sql, sourceInputStream);
     * </pre>
     *
     * @param sql                - SQL INSERT Query
     * @param from               - InputStream with data
     * @param additionalDBParams - specific DB params for request
     * @throws SQLException - insertion error
     * @see cc.blynk.clickhouse.domain.ClickHouseFormat for possible formats of the data
     */
    void copyToDb(String sql,
                  InputStream from,
                  Map<ClickHouseQueryParam, String> additionalDBParams)
            throws SQLException;

    /**
     * Inserts the data from the stream to the DB.
     * The type and structure of the data from the stream specified in the SQL Query.
     * For example, in case you want to send the data in the CSV format to the DB you need to:
     * <pre>
     *     InputStream sourceInputStream = ...
     *     String sql = "INSERT INTO default.my_table FORMAT CSV";
     *     CopyManager copyManager = CopyManagerFactory.create(connection);
     *     copyManager.copyToDb(sql, sourceInputStream, 1024);
     * </pre>
     *
     * @param sql - SQL INSERT Query
     * @param from - InputStream with data
     * @param bufferSize - buffer size for the data transfer
     * @throws SQLException - insertion error
     * @see cc.blynk.clickhouse.domain.ClickHouseFormat for possible formats of the data
     */
    void copyToDb(String sql, InputStream from, int bufferSize) throws SQLException;

    /**
     * Inserts the data from the stream to the DB.
     * The type and structure of the data from the stream specified in the SQL Query.
     * For example, in case you want to send the data in the CSV format to the DB you need to:
     * <pre>
     *     InputStream sourceInputStream = ...
     *     String sql = "INSERT INTO default.my_table FORMAT CSV";
     *     CopyManager copyManager = CopyManagerFactory.create(connection);
     *     copyManager.copyToDb(sql, sourceInputStream, 1024);
     * </pre>
     *
     * @param sql                - SQL INSERT Query
     * @param from               - InputStream with data
     * @param additionalDBParams - specific DB params for request
     * @param bufferSize         - buffer size for the data transfer
     * @throws SQLException - insertion error
     * @see cc.blynk.clickhouse.domain.ClickHouseFormat for possible formats of the data
     */
    void copyToDb(String sql,
                  InputStream from,
                  Map<ClickHouseQueryParam, String> additionalDBParams,
                  int bufferSize)
            throws SQLException;

    /**
     * Inserts the data from the stream to the DB.
     * The type and structure of the data from the stream specified in the SQL Query.
     * For example, in case you want to send the data in the CSV format to the DB you need to:
     * <pre>
     *     Reader sourceReader = ...
     *     String sql = "INSERT INTO default.my_table FORMAT CSV";
     *     CopyManager copyManager = CopyManagerFactory.create(connection);
     *     copyManager.copyToDb(sql, sourceReader);
     *
     *     //  The data from the Reader will be forwarded to the DB in CSV format
     * </pre>
     *
     * @param sql  - SQL INSERT Query
     * @param from - Reader with data
     * @throws SQLException - insertion error
     * @see cc.blynk.clickhouse.domain.ClickHouseFormat for possible formats of the data
     */
    void copyToDb(String sql, Reader from) throws SQLException;

    /**
     * Inserts the data from the stream to the DB.
     * The type and structure of the data from the stream specified in the SQL Query.
     * For example, in case you want to send the data in the CSV format to the DB you need to:
     * <pre>
     *     Reader sourceReader = ...
     *     String sql = "INSERT INTO default.my_table FORMAT CSV";
     *     CopyManager copyManager = CopyManagerFactory.create(connection);
     *     copyManager.copyToDb(sql, sourceReader);
     *
     *     //  The data from the Reader will be forwarded to the DB in CSV format
     * </pre>
     *
     * @param sql                - SQL INSERT Query
     * @param from               - Reader with data
     * @param additionalDBParams - specific DB params for request
     * @throws SQLException - insertion error
     * @see cc.blynk.clickhouse.domain.ClickHouseFormat for possible formats of the data
     */
    void copyToDb(String sql,
                  Reader from,
                  Map<ClickHouseQueryParam, String> additionalDBParams)
            throws SQLException;

    /**
     * Inserts the data from the stream to the DB.
     * The type and structure of the data from the stream specified in the SQL Query.
     * For example, in case you want to send the data in the CSV format to the DB you need to:
     * <pre>
     *     Reader sourceReader = ...
     *     String sql = "INSERT INTO default.my_table FORMAT CSV";
     *     CopyManager copyManager = CopyManagerFactory.create(connection);
     *     copyManager.copyToDb(sql, sourceReader);
     *
     *     //  The data from the Reader will be forwarded to the DB in CSV format
     * </pre>
     *
     * @param sql  - SQL INSERT Query
     * @param from - Reader with data
     * @param bufferSize - buffer size for the data transfer
     * @throws SQLException - insertion error
     * @see cc.blynk.clickhouse.domain.ClickHouseFormat for possible formats of the data
     */
    void copyToDb(String sql, Reader from, int bufferSize) throws SQLException;

    /**
     * Inserts the data from the stream to the DB.
     * The type and structure of the data from the stream specified in the SQL Query.
     * For example, in case you want to send the data in the CSV format to the DB you need to:
     * <pre>
     *     Reader sourceReader = ...
     *     String sql = "INSERT INTO default.my_table FORMAT CSV";
     *     CopyManager copyManager = CopyManagerFactory.create(connection);
     *     copyManager.copyToDb(sql, sourceReader);
     *
     *     //  The data from the Reader will be forwarded to the DB in CSV format
     * </pre>
     *
     * @param sql                - SQL INSERT Query
     * @param from               - Reader with data
     * @param additionalDBParams - specific DB params for request
     * @param bufferSize         - buffer size for the data transfer
     * @throws SQLException - insertion error
     * @see cc.blynk.clickhouse.domain.ClickHouseFormat for possible formats of the data
     */
    void copyToDb(String sql,
                  Reader from,
                  Map<ClickHouseQueryParam, String> additionalDBParams,
                  int bufferSize)
            throws SQLException;

    /**
     * Loads the data from the DB using the SQL Query and writes it to the OutputStream
     * in the format defined in the SQL query.
     * For example, in case you want to select the data from the DB and directly write
     * it to the file in the csv format with the headers line and without creating intermediate
     * objects in the application space:
     * <pre>
     *     FileOutputStream fos = ...
     *     String sql = "SELECT * from default.my_table FORMAT CSVWithNames";
     *     CopyManager copyManager = CopyManagerFactory.create(connection);
     *     copyManager.copyFromDb(sql, fos);
     * </pre>
     *
     * @param sql - SQL SELECT Query
     * @param to - OutputStream to write data
     * @throws SQLException - loading error
     * @see cc.blynk.clickhouse.domain.ClickHouseFormat for possible formats of the data
     */
    void copyFromDb(String sql, OutputStream to) throws SQLException;

    /**
     * Loads the data from the DB using the SQL Query and writes it to the OutputStream
     * in the format defined in the SQL query.
     * For example, in case you want to select the data from the DB and directly write
     * it to the file in the csv format with the headers line and without creating intermediate
     * objects in the application space:
     * <pre>
     *     FileOutputStream fos = ...
     *     String sql = "SELECT * from default.my_table FORMAT CSVWithNames";
     *     CopyManager copyManager = CopyManagerFactory.create(connection);
     *     copyManager.copyFromDb(sql, fos);
     * </pre>
     *
     * @param sql                - SQL SELECT Query
     * @param to                 - OutputStream to write data
     * @param additionalDBParams - specific DB params for request
     * @throws SQLException - loading error
     * @see cc.blynk.clickhouse.domain.ClickHouseFormat for possible formats of the data
     */
    void copyFromDb(String sql,
                    OutputStream to,
                    Map<ClickHouseQueryParam, String> additionalDBParams)
            throws SQLException;


    /**
     * Loads the data from the DB using the SQL Query and writes it to the OutputStream
     * in the format defined in the SQL query.
     * For example, in case you want to select the data from the DB and directly write
     * it to the file in the csv format with the headers line and without creating intermediate
     * objects in the application space:
     * <pre>
     *     FileOutputStream fos = ...
     *     String sql = "SELECT * from default.my_table FORMAT CSVWithNames";
     *     CopyManager copyManager = CopyManagerFactory.create(connection);
     *     copyManager.copyFromDb(sql, fos);
     * </pre>
     *
     * @param sql - SQL SELECT Query
     * @param to - Writer to write the data to
     * @throws SQLException - loading error
     * @see cc.blynk.clickhouse.domain.ClickHouseFormat for possible formats of the data
     */
    void copyFromDb(String sql, Writer to) throws SQLException;

    /**
     * Loads the data from the DB using the SQL Query and writes it to the OutputStream
     * in the format defined in the SQL query.
     * For example, in case you want to select the data from the DB and directly write
     * it to the file in the csv format with the headers line and without creating intermediate
     * objects in the application space:
     * <pre>
     *     FileOutputStream fos = ...
     *     String sql = "SELECT * from default.my_table FORMAT CSVWithNames";
     *     CopyManager copyManager = CopyManagerFactory.create(connection);
     *     copyManager.copyFromDb(sql, fos);
     * </pre>
     *
     * @param sql                - SQL SELECT Query
     * @param to                 - Writer to write the data to
     * @param additionalDBParams - specific DB params for request
     * @throws SQLException - loading error
     * @see cc.blynk.clickhouse.domain.ClickHouseFormat for possible formats of the data
     */
    void copyFromDb(String sql,
                    Writer to,
                    Map<ClickHouseQueryParam, String> additionalDBParams)
            throws SQLException;
}
