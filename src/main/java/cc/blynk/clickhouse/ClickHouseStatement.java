package cc.blynk.clickhouse;

import cc.blynk.clickhouse.settings.ClickHouseQueryParam;
import cc.blynk.clickhouse.util.ClickHouseRowBinaryInputStream;
import cc.blynk.clickhouse.util.ClickHouseStreamCallback;

import java.io.InputStream;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

public interface ClickHouseStatement extends Statement {

    ClickHouseRowBinaryInputStream executeQueryClickhouseRowBinaryStream(String sql) throws SQLException;

    ClickHouseRowBinaryInputStream executeQueryClickhouseRowBinaryStream(
            String sql,
            Map<ClickHouseQueryParam, String> additionalDBParams
    ) throws SQLException;

    ClickHouseRowBinaryInputStream executeQueryClickhouseRowBinaryStream(
            String sql,
            Map<ClickHouseQueryParam, String> additionalDBParams,
            Map<String, String> additionalRequestParams) throws SQLException;

    ResultSet executeQuery(String sql, Map<ClickHouseQueryParam, String> additionalDBParams) throws SQLException;

    ResultSet executeQuery(String sql,
                           Map<ClickHouseQueryParam, String> additionalDBParams,
                           List<ClickHouseExternalData> externalData) throws SQLException;

    ResultSet executeQuery(String sql,
                           Map<ClickHouseQueryParam,
                           String> additionalDBParams,
                           List<ClickHouseExternalData> externalData,
                           Map<String, String> additionalRequestParams) throws SQLException;

    void sendStream(InputStream content,
                    String table,
                    Map<ClickHouseQueryParam, String> additionalDBParams) throws SQLException;

    void sendStream(InputStream content, String table) throws SQLException;

    void sendRowBinaryStream(String sql,
                             Map<ClickHouseQueryParam, String> additionalDBParams,
                             ClickHouseStreamCallback callback) throws SQLException;

    void sendRowBinaryStream(String sql, ClickHouseStreamCallback callback) throws SQLException;

    void sendNativeStream(String sql,
                          Map<ClickHouseQueryParam, String> additionalDBParams,
                          ClickHouseStreamCallback callback) throws SQLException;

    void sendNativeStream(String sql, ClickHouseStreamCallback callback) throws SQLException;

    void sendStreamSQL(InputStream content, String sql) throws SQLException;

    void sendStreamSQL(InputStream content, String sql,
                       Map<ClickHouseQueryParam, String> additionalDBParams) throws SQLException;

    void sendStreamSQL(String sql, OutputStream response) throws SQLException;

    void sendStreamSQL(String sql, OutputStream response,
                       Map<ClickHouseQueryParam, String> additionalDBParams) throws SQLException;
}
