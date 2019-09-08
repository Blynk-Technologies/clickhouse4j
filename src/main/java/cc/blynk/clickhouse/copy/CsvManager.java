package cc.blynk.clickhouse.copy;

import cc.blynk.clickhouse.settings.ClickHouseQueryParam;

import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.Map;

public interface CsvManager {

    /**
     * Insert CSV data from content stream to DB
     *
     * @param sql     - SQL INSERT Query without FORMAT
     * @param content - Stream with CSV data
     * @throws SQLException - insertion error
     */
    void copyToDb(String sql, InputStream content) throws SQLException;

    /**
     * Insert CSV with header as a first row data from content stream to DB
     *
     * @param sql     - SQL INSERT Query without FORMAT
     * @param content - Stream with CSV data
     * @throws SQLException - insertion error
     */
    void copyToDbWithNames(String sql, InputStream content) throws SQLException;

    /**
     * Insert CSV data from content stream to DB
     *
     * @param sql                - SQL INSERT Query without FORMAT
     * @param content            - Stream with CSV data
     * @param additionalDBParams - specific DB params for request
     * @throws SQLException - insertion error
     */
    void copyToDb(String sql,
                  InputStream content,
                  Map<ClickHouseQueryParam, String> additionalDBParams)
            throws SQLException;

    /**
     * Insert CSV with header as a first row data from content stream to DB
     *
     * @param sql                - SQL INSERT Query without FORMAT
     * @param content            - Stream with CSV data
     * @param additionalDBParams - specific DB params for request
     * @throws SQLException - insertion error
     */
    void copyToDbWithNames(String sql,
                           InputStream content,
                           Map<ClickHouseQueryParam, String> additionalDBParams)
            throws SQLException;

    /**
     * Insert CSV data from content stream to table
     *
     * @param table   - DB table for insertion
     * @param content - Stream with CSV data
     * @throws SQLException - insertion error
     */
    void copyToTable(String table, InputStream content) throws SQLException;

    /**
     * Insert CSV with header as a first row data from content stream to table
     *
     * @param table   - DB table for insertion
     * @param content - Stream with CSV data
     * @throws SQLException - insertion error
     */
    void copyToTableWithNames(String table, InputStream content) throws SQLException;

    /**
     * Insert CSV data from content stream to table
     *
     * @param table              - DB table for insertion
     * @param content            - Stream with CSV data
     * @param additionalDBParams - specific DB params for request
     * @throws SQLException - insertion error
     * @see ClickHouseQueryParam for more info adout additionalDBParams
     */
    void copyToTable(String table,
                     InputStream content,
                     Map<ClickHouseQueryParam, String> additionalDBParams)
            throws SQLException;

    /**
     * Insert CSV with header as a first row data from content stream to table
     *
     * @param table              - DB table for insertion
     * @param content            - Stream with CSV data
     * @param additionalDBParams - specific DB params for request
     * @throws SQLException - insertion error
     * @see ClickHouseQueryParam for more info adout additionalDBParams
     */
    void copyToTableWithNames(String table,
                              InputStream content,
                              Map<ClickHouseQueryParam, String> additionalDBParams)
            throws SQLException;

    /**
     * Load data from db by SQL Query and write it ro response stream in CSV format
     *
     * @param sql      - SQL SELECT query for data selection, without any FORMAT section
     * @param response - stream for response data
     * @throws SQLException - loading error
     */
    void copyFromDb(String sql, OutputStream response) throws SQLException;

    /**
     * Load data from db by SQL Query and write it ro response stream in CSV with header as a first row format
     *
     * @param sql      - SQL SELECT query for data selection, without any FORMAT section
     * @param response - stream for response data
     * @throws SQLException - loading error
     */
    void copyFromDbWithNames(String sql, OutputStream response) throws SQLException;

    /**
     * Load data from db and write it ro response stream in CSV format
     *
     * @param sql                - SQL SELECT query for data selection, without any FORMAT section
     * @param response           - stream for response data
     * @param additionalDBParams - specific DB params for request
     * @throws SQLException - loading error
     * @see ClickHouseQueryParam for more info adout additionalDBParams
     */
    void copyFromDb(String sql,
                    OutputStream response,
                    Map<ClickHouseQueryParam, String> additionalDBParams)
            throws SQLException;

    /**
     * Load data from db and write it ro response stream in CSV with header as a first row format
     *
     * @param sql                - SQL SELECT query for data selection, without any FORMAT section
     * @param response           - stream for response data
     * @param additionalDBParams - specific DB params for request
     * @throws SQLException - loading error
     * @see ClickHouseQueryParam for more info adout additionalDBParams
     */
    void copyFromDbWithNames(String sql,
                             OutputStream response,
                             Map<ClickHouseQueryParam, String> additionalDBParams)
            throws SQLException;
}
