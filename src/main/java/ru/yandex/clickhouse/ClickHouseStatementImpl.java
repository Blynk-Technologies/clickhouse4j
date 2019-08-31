package ru.yandex.clickhouse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.domain.ClickHouseFormat;
import ru.yandex.clickhouse.except.ClickHouseException;
import ru.yandex.clickhouse.except.ClickHouseExceptionSpecifier;
import ru.yandex.clickhouse.http.HttpConnector;
import ru.yandex.clickhouse.response.ClickHouseLZ4Stream;
import ru.yandex.clickhouse.response.ClickHouseResultSet;
import ru.yandex.clickhouse.response.ClickHouseScrollableResultSet;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;
import ru.yandex.clickhouse.util.ClickHouseRowBinaryInputStream;
import ru.yandex.clickhouse.util.ClickHouseRowBinaryStream;
import ru.yandex.clickhouse.util.ClickHouseStreamCallback;
import ru.yandex.clickhouse.util.Utils;
import ru.yandex.clickhouse.util.guava.StreamUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.stream.Collectors;

public class ClickHouseStatementImpl implements ClickHouseStatement {

    private static final Logger log = LoggerFactory.getLogger(ClickHouseStatementImpl.class);

    private final HttpConnector httpConnector;

    protected final ClickHouseProperties properties;

    private final ClickHouseConnection connection;

    private ClickHouseResultSet currentResult;

    private ClickHouseRowBinaryInputStream currentRowBinaryResult;

    private int currentUpdateCount = -1;

    private int queryTimeout;

    private boolean isQueryTimeoutSet = false;

    private int maxRows;

    private boolean closeOnCompletion;

    private final boolean isResultSetScrollable;

    private volatile String queryId;

    /**
     * Current database name may be changed by {@link java.sql.Connection#setCatalog(String)}
     * between creation of this object and query execution, but javadoc does not allow
     * {@code setCatalog} influence on already created statements.
     */
    private final String initialDatabase;

    private static final String[] selectKeywords = new String[]{"SELECT", "WITH", "SHOW", "DESC", "EXISTS"};
    private static final String databaseKeyword = "CREATE DATABASE";

    ClickHouseStatementImpl(HttpConnector connector, ClickHouseConnection connection,
                            ClickHouseProperties properties, int resultSetType) {
        this.connection = connection;
        this.properties = properties == null ? new ClickHouseProperties() : properties;
        this.initialDatabase = this.properties.getDatabase();
        this.isResultSetScrollable = (resultSetType != ResultSet.TYPE_FORWARD_ONLY);

        this.httpConnector = connector;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        return executeQuery(sql, null);
    }

    /**
     * Adding  FORMAT TabSeparatedWithNamesAndTypes if not added
     * adds format only to select queries
     */
    private static String addFormatIfAbsent(final String sql, ClickHouseFormat format) {
        String cleanSQL = sql.trim();
        if (!isSelect(cleanSQL)) {
            return cleanSQL;
        }
        if (ClickHouseFormat.containsFormat(cleanSQL)) {
            return cleanSQL;
        }
        StringBuilder sb = new StringBuilder();
        int idx = cleanSQL.endsWith(";")
                ? cleanSQL.length() - 1
                : cleanSQL.length();
        sb.append(cleanSQL.substring(0, idx))
                .append(" FORMAT ")
                .append(format.name())
                .append(';');
        return sb.toString();
    }

    static boolean isSelect(String sql) {
        for (int i = 0; i < sql.length(); i++) {
            String nextTwo = sql.substring(i, Math.min(i + 2, sql.length()));
            if ("--".equals(nextTwo)) {
                i = Math.max(i, sql.indexOf("\n", i));
            } else if ("/*".equals(nextTwo)) {
                i = Math.max(i, sql.indexOf("*/", i));
            } else if (Character.isLetter(sql.charAt(i))) {
                String trimmed = sql.substring(i, Math.min(sql.length(), Math.max(i, sql.indexOf(" ", i))));
                for (String keyword : selectKeywords) {
                    if (trimmed.regionMatches(true, 0, keyword, 0, keyword.length())) {
                        return true;
                    }
                }
                return false;
            }
        }
        return false;
    }

    @Override
    public ResultSet executeQuery(String sql,
                                  Map<ClickHouseQueryParam, String> additionalDBParams) throws SQLException {
        return executeQuery(sql, additionalDBParams, null);
    }

    @Override
    public ClickHouseRowBinaryInputStream executeQueryClickhouseRowBinaryStream(String sql) throws SQLException {
        return executeQueryClickhouseRowBinaryStream(sql, null);
    }

    @Override
    public ResultSet executeQuery(String sql,
                                  Map<ClickHouseQueryParam, String> additionalDBParams,
                                  List<ClickHouseExternalData> externalData) throws SQLException {
        return executeQuery(sql, additionalDBParams, externalData, null);
    }

    @Override
    public ResultSet executeQuery(String sql,
                                  Map<ClickHouseQueryParam, String> additionalDBParams,
                                  List<ClickHouseExternalData> externalData,
                                  Map<String, String> additionalRequestParams) throws SQLException {

        // forcibly disable extremes for ResultSet queries
        if (additionalDBParams == null || additionalDBParams.isEmpty()) {
            additionalDBParams = new EnumMap<>(ClickHouseQueryParam.class);
        } else {
            additionalDBParams = new EnumMap<>(additionalDBParams);
        }
        additionalDBParams.put(ClickHouseQueryParam.EXTREMES, "0");

        InputStream is = getInputStream(sql, additionalDBParams, externalData, additionalRequestParams);

        try {
            if (isSelect(sql)) {
                currentUpdateCount = -1;
                currentResult = createResultSet(properties.isCompress()
                                ? new ClickHouseLZ4Stream(is) : is, properties.getBufferSize(),
                        extractDBName(sql),
                        extractTableName(sql),
                        extractWithTotals(sql),
                        this,
                        getConnection().getTimeZone(),
                        properties
                );
                currentResult.setMaxRows(maxRows);
                return currentResult;
            } else {
                currentUpdateCount = 0;
                StreamUtils.close(is);
                return null;
            }
        } catch (Exception e) {
            StreamUtils.close(is);
            throw ClickHouseExceptionSpecifier.specify(e, properties.getHost(), properties.getPort());
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        InputStream is = null;
        try {
            is = getInputStream(sql, null, null, null);
            //noinspection StatementWithEmptyBody
        } finally {
            StreamUtils.close(is);
        }
        return 1;
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        // currentResult is stored here. InputString and currentResult will be closed on this.closeClient()
        executeQuery(sql);
        return isSelect(sql);
    }

    @Override
    public void close() throws SQLException {
        if (currentResult != null) {
            currentResult.close();
        }

        if (currentRowBinaryResult != null) {
            StreamUtils.close(currentRowBinaryResult);
        }
    }

    @Override
    public int getMaxFieldSize() {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) {

    }

    @Override
    public int getMaxRows() {
        return maxRows;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        if (max < 0) {
            throw new SQLException(String.format("Illegal maxRows value: %d", max));
        }
        maxRows = max;
    }

    @Override
    public void setEscapeProcessing(boolean enable) {

    }

    @Override
    public int getQueryTimeout() {
        return queryTimeout;
    }

    @Override
    public void setQueryTimeout(int seconds) {
        queryTimeout = seconds;
        isQueryTimeoutSet = true;
    }

    @Override
    public ClickHouseRowBinaryInputStream executeQueryClickhouseRowBinaryStream(
            String sql,
            Map<ClickHouseQueryParam, String> additionalDBParams) throws SQLException {
        return executeQueryClickhouseRowBinaryStream(sql, additionalDBParams, null);
    }

    @Override
    public void cancel() throws SQLException {
        if (this.queryId == null || isClosed()) {
            return;
        }

        executeQuery(String.format("KILL QUERY WHERE query_id='%s'", queryId));
    }

    @Override
    public SQLWarning getWarnings() {
        return null;
    }

    @Override
    public void clearWarnings() {

    }

    @Override
    public void setCursorName(String name) {

    }

    @Override
    public ResultSet getResultSet() {
        return currentResult;
    }

    @Override
    public int getUpdateCount() {
        return currentUpdateCount;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        if (currentResult != null) {
            currentResult.close();
            currentResult = null;
        }
        currentUpdateCount = -1;
        return false;
    }

    @Override
    public void setFetchDirection(int direction) {

    }

    @Override
    public int getFetchDirection() {
        return 0;
    }

    @Override
    public void setFetchSize(int rows) {

    }

    @Override
    public int getFetchSize() {
        return 0;
    }

    @Override
    public int getResultSetConcurrency() {
        return 0;
    }

    @Override
    public int getResultSetType() {
        return 0;
    }

    @Override
    public void addBatch(String sql) {

    }

    @Override
    public void clearBatch() {

    }

    @Override
    public int[] executeBatch() throws SQLException {
        return new int[0];
    }

    @Override
    public ClickHouseConnection getConnection() {
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() {
        return null;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) {
        return 0;
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) {
        return 0;
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) {
        return 0;
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) {
        return false;
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) {
        return false;
    }

    @Override
    public boolean execute(String sql, String[] columnNames) {
        return false;
    }

    @Override
    public int getResultSetHoldability() {
        return 0;
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void setPoolable(boolean poolable) {

    }

    @Override
    public boolean isPoolable() {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isAssignableFrom(getClass());
    }

    static String clickhousifySql(String sql) {
        return addFormatIfAbsent(sql, ClickHouseFormat.TabSeparatedWithNamesAndTypes);
    }

    @Override
    public ClickHouseRowBinaryInputStream executeQueryClickhouseRowBinaryStream(
            String sql,
            Map<ClickHouseQueryParam, String> additionalDBParams,
            Map<String, String> additionalRequestParams) throws SQLException {
        InputStream is = getInputStream(
                addFormatIfAbsent(sql, ClickHouseFormat.RowBinary),
                additionalDBParams,
                null,
                additionalRequestParams
        );
        try {
            if (isSelect(sql)) {
                currentUpdateCount = -1;
                currentRowBinaryResult = new ClickHouseRowBinaryInputStream(properties.isCompress()
                                                                                    ? new ClickHouseLZ4Stream(is) : is,
                                                                            getConnection().getTimeZone(),
                                                                            properties);
                return currentRowBinaryResult;
            } else {
                currentUpdateCount = 0;
                StreamUtils.close(is);
                return null;
            }
        } catch (Exception e) {
            StreamUtils.close(is);
            throw ClickHouseExceptionSpecifier.specify(e, properties.getHost(), properties.getPort());
        }
    }

    private String extractTableName(String sql) {
        String s = extractDBAndTableName(sql);
        if (s.contains(".")) {
            return s.substring(s.indexOf(".") + 1);
        } else {
            return s;
        }
    }

    private String extractDBName(String sql) {
        String s = extractDBAndTableName(sql);
        if (s.contains(".")) {
            return s.substring(0, s.indexOf("."));
        } else {
            return properties.getDatabase();
        }
    }

    private String extractDBAndTableName(String sql) {
        if (sql.regionMatches(true, 0, "select", 0, "select".length())) {
            String withoutStrings = Utils.retainUnquoted(sql, '\'');
            int fromIndex = withoutStrings.indexOf("from");
            if (fromIndex == -1) {
                fromIndex = withoutStrings.indexOf("FROM");
            }
            if (fromIndex != -1) {
                String fromFrom = withoutStrings.substring(fromIndex);
                String fromTable = fromFrom.substring("from".length()).trim();
                return fromTable.split(" ")[0];
            }
        }
        if (sql.regionMatches(true, 0, "desc", 0, "desc".length())) {
            return "system.columns";
        }
        if (sql.regionMatches(true, 0, "show", 0, "show".length())) {
            return "system.tables";
        }
        return "system.unknown";
    }

    private boolean extractWithTotals(String sql) {
        if (sql.regionMatches(true, 0, "select", 0, "select".length())) {
            String withoutStrings = Utils.retainUnquoted(sql, '\'');
            return withoutStrings.toLowerCase().contains(" with totals");
        }
        return false;
    }

    @Override
    public void sendRowBinaryStream(String sql, ClickHouseStreamCallback callback) throws SQLException {
        sendRowBinaryStream(sql, null, callback);
    }

    @Override
    public void sendRowBinaryStream(String sql, Map<ClickHouseQueryParam, String> additionalDBParams,
                                    ClickHouseStreamCallback callback) throws SQLException {
        URI uri = buildRequestUri(null, null, additionalDBParams, null, false);
        sql = sql + " FORMAT " + ClickHouseFormat.RowBinary.name();
        sendStream(sql, callback, uri);
    }

    @Override
    public void sendNativeStream(String sql, ClickHouseStreamCallback callback) throws SQLException {
        sendNativeStream(sql, null, callback);
    }

    @Override
    public void sendNativeStream(String sql, Map<ClickHouseQueryParam, String> additionalDBParams,
                                 ClickHouseStreamCallback callback) throws SQLException {
        URI uri = buildRequestUri(null, null, additionalDBParams, null, false);
        sql = sql + " FORMAT " + ClickHouseFormat.Native.name();
        sendStream(sql, callback, uri);
    }

    private void sendStream(String sql, ClickHouseStreamCallback callback, URI uri) throws ClickHouseException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes((sql + "\n").getBytes(StandardCharsets.UTF_8));
        try {
            TimeZone timeZone = getConnection().getTimeZone();
            ClickHouseRowBinaryStream stream = new ClickHouseRowBinaryStream(out, timeZone, properties);
            callback.writeTo(stream);
        } catch (IOException e) {
            throw new ClickHouseException(0, e, null, 0);
        }

        httpConnector.post(out.toByteArray(), uri);
    }

    @Override
    public void sendStream(InputStream content, String table) throws ClickHouseException {
        sendStream(content, table, null);
    }

    @Override
    public void sendStream(InputStream stream, String table,
                           Map<ClickHouseQueryParam, String> additionalDBParams)
            throws ClickHouseException {
        String sql = "INSERT INTO " + table + " FORMAT " + ClickHouseFormat.TabSeparated + "\n";

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(sql.getBytes(StandardCharsets.UTF_8));
        try {
            out.writeBytes(stream.readAllBytes());
        } catch (IOException e) {
            throw new ClickHouseException(0, e, null, 0);
        }

        URI uri = buildRequestUri(null, null, additionalDBParams, null, false);
        httpConnector.post(out.toByteArray(), uri);
    }

    @Override
    public void sendCSVStream(InputStream content, String table, Map<ClickHouseQueryParam, String> additionalDBParams) throws ClickHouseException {
        String query = "INSERT INTO " + table;
        //todo finish
        //sendStream(new InputStreamEntity(content, -1), query, ClickHouseFormat.CSV, additionalDBParams);
    }

    @Override
    public void sendCSVStream(InputStream content, String table) throws ClickHouseException {
        sendCSVStream(content, table, null);
    }

    @Override
    public void sendStreamSQL(InputStream content, String sql,
                              Map<ClickHouseQueryParam, String> additionalDBParams) throws ClickHouseException {
        //todo finish
        //sendStreamSQL(new InputStreamEntity(content, -1), sql, additionalDBParams);
    }

    @Override
    public void sendStreamSQL(InputStream content, String sql) throws ClickHouseException {
        //todo finish
        //sendStreamSQL(new InputStreamEntity(content, -1), sql, null);
    }

    void sendStream(List<byte[]> batchRows, String sql, Map<ClickHouseQueryParam, String> additionalDBParams)
            throws ClickHouseException {
        URI uri = buildRequestUri(null, null, additionalDBParams, null, false);

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        sql = sql + " FORMAT " + ClickHouseFormat.TabSeparated.name() + "\n";
        out.writeBytes(sql.getBytes(StandardCharsets.UTF_8));

        batchRows.forEach(out::writeBytes);
        httpConnector.post(out.toByteArray(), uri);
    }

    public void closeOnCompletion() {
        closeOnCompletion = true;
    }

    public boolean isCloseOnCompletion() {
        return closeOnCompletion;
    }

    private ClickHouseResultSet createResultSet(InputStream is,
                                                int bufferSize,
                                                String db,
                                                String table,
                                                boolean usesWithTotals,
                                                ClickHouseStatement statement,
                                                TimeZone timezone,
                                                ClickHouseProperties properties) throws IOException {
        if (isResultSetScrollable) {
            return new ClickHouseScrollableResultSet(is,
                    bufferSize,
                    db,
                    table,
                    usesWithTotals,
                    statement,
                    timezone,
                    properties);
        } else {
            return new ClickHouseResultSet(is,
                    bufferSize,
                    db, table,
                    usesWithTotals,
                    statement,
                    timezone,
                    properties);
        }
    }

    private Map<ClickHouseQueryParam, String> addQueryIdTo(Map<ClickHouseQueryParam, String> parameters) {
        if (this.queryId != null) {
            return parameters;
        }

        String queryId = parameters.get(ClickHouseQueryParam.QUERY_ID);
        if (queryId == null) {
            this.queryId = UUID.randomUUID().toString();
            parameters.put(ClickHouseQueryParam.QUERY_ID, this.queryId);
        } else {
            this.queryId = queryId;
        }

        return parameters;
    }

    private InputStream getInputStream(
            String sql,
            Map<ClickHouseQueryParam, String> additionalClickHouseDBParams,
            List<ClickHouseExternalData> externalData,
            Map<String, String> additionalRequestParams
    ) throws ClickHouseException {
        sql = clickhousifySql(sql);
        log.debug("Executing SQL: {}", sql);

        additionalClickHouseDBParams = addQueryIdTo(
                additionalClickHouseDBParams == null
                        ? new EnumMap<>(ClickHouseQueryParam.class)
                        : additionalClickHouseDBParams);

        boolean ignoreDatabase = sql.trim().regionMatches(true, 0, databaseKeyword, 0, databaseKeyword.length());
        URI uri;
        if (externalData == null || externalData.isEmpty()) {
            uri = buildRequestUri(
                    null,
                    null,
                    additionalClickHouseDBParams,
                    additionalRequestParams,
                    ignoreDatabase
            );
            log.debug("Request url: {}", uri);
            return httpConnector.post(sql, uri);
        } else {
            // write sql in query params when there is external data
            // as it is impossible to pass both external data and sql in body
            // TODO move sql to request body when it is supported in clickhouse
            uri = buildRequestUri(
                    sql,
                    externalData,
                    additionalClickHouseDBParams,
                    additionalRequestParams,
                    ignoreDatabase
            );
            log.debug("Request url: {}", uri);
            return httpConnector.post(externalData, uri);
        }
    }

    URI buildRequestUri(
            String sql,
            List<ClickHouseExternalData> externalData,
            Map<ClickHouseQueryParam, String> additionalClickHouseDBParams,
            Map<String, String> additionalRequestParams,
            boolean ignoreDatabase
    ) {
        try {
            String query = getUrlQueryParams(
                    sql,
                    externalData,
                    additionalClickHouseDBParams,
                    additionalRequestParams,
                    ignoreDatabase
            ).stream()
                    .map(pair -> String.format("%s=%s", pair.getKey(), pair.getValue()))
                    .collect(Collectors.joining("&"));

            return new URI(properties.getSsl() ? "https" : "http",
                           null,
                           properties.getHost(),
                           properties.getPort(),
                           properties.getPath() == null || properties.getPath().isEmpty() ? "/" : properties.getPath(),
                           query,
                           null
            );
        } catch (URISyntaxException e) {
            log.error("Mailformed URL: {}", e.getMessage());
            throw new IllegalStateException("illegal configuration of db");
        }
    }

    private List<SimpleImmutableEntry<String, String>> getUrlQueryParams(
            String sql,
            List<ClickHouseExternalData> externalData,
            Map<ClickHouseQueryParam, String> additionalClickHouseDBParams,
            Map<String, String> additionalRequestParams,
            boolean ignoreDatabase
    ) {
        List<SimpleImmutableEntry<String, String>> result = new ArrayList<>();

        if (sql != null) {
            result.add(new SimpleImmutableEntry<>("query", sql));
        }

        if (externalData != null) {
            for (ClickHouseExternalData externalDataItem : externalData) {
                String name = externalDataItem.getName();
                String format = externalDataItem.getFormat();
                String types = externalDataItem.getTypes();
                String structure = externalDataItem.getStructure();

                if (format != null && !format.isEmpty()) {
                    result.add(new SimpleImmutableEntry<>(name + "_format", format));
                }
                if (types != null && !types.isEmpty()) {
                    result.add(new SimpleImmutableEntry<>(name + "_types", types));
                }
                if (structure != null && !structure.isEmpty()) {
                    result.add(new SimpleImmutableEntry<>(name + "_structure", structure));
                }
            }
        }

        Map<ClickHouseQueryParam, String> params = properties.buildQueryParams(true);
        if (!ignoreDatabase) {
            params.put(ClickHouseQueryParam.DATABASE, initialDatabase);
        }

        if (additionalClickHouseDBParams != null && !additionalClickHouseDBParams.isEmpty()) {
            params.putAll(additionalClickHouseDBParams);
        }

        setStatementPropertiesToParams(params);

        for (Map.Entry<ClickHouseQueryParam, String> entry : params.entrySet()) {
            String s = entry.getValue();
            if (!(s == null || s.isEmpty())) {
                result.add(new SimpleImmutableEntry<>(entry.getKey().toString(), entry.getValue()));
            }
        }

        if (additionalRequestParams != null) {
            for (Map.Entry<String, String> entry : additionalRequestParams.entrySet()) {
                String s = entry.getValue();
                if (!(s == null || s.isEmpty())) {
                    result.add(new SimpleImmutableEntry<>(entry.getKey(), entry.getValue()));
                }
            }
        }


        return result;
    }

    private void setStatementPropertiesToParams(Map<ClickHouseQueryParam, String> params) {
        if (maxRows > 0) {
            params.put(ClickHouseQueryParam.MAX_RESULT_ROWS, String.valueOf(maxRows));
            params.put(ClickHouseQueryParam.RESULT_OVERFLOW_MODE, "break");
        }
        if (isQueryTimeoutSet) {
            params.put(ClickHouseQueryParam.MAX_EXECUTION_TIME, String.valueOf(queryTimeout));
        }
    }

}
