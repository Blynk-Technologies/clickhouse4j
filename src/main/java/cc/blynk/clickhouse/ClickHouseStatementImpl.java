package cc.blynk.clickhouse;

import cc.blynk.clickhouse.domain.ClickHouseFormat;
import cc.blynk.clickhouse.except.ClickHouseException;
import cc.blynk.clickhouse.except.ClickHouseExceptionSpecifier;
import cc.blynk.clickhouse.http.HttpConnector;
import cc.blynk.clickhouse.response.AbstractResultSet;
import cc.blynk.clickhouse.response.ClickHouseJsonResultSet;
import cc.blynk.clickhouse.response.ClickHouseResultSet;
import cc.blynk.clickhouse.response.ClickHouseScrollableResultSet;
import cc.blynk.clickhouse.settings.ClickHouseProperties;
import cc.blynk.clickhouse.settings.ClickHouseQueryParam;
import cc.blynk.clickhouse.util.ClickHouseRowBinaryInputStream;
import cc.blynk.clickhouse.util.ClickHouseRowBinaryStream;
import cc.blynk.clickhouse.util.ClickHouseStreamCallback;
import cc.blynk.clickhouse.util.Utils;
import cc.blynk.clickhouse.util.guava.StreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

public class ClickHouseStatementImpl implements ClickHouseStatement {

    private static final Logger log = LoggerFactory.getLogger(ClickHouseStatementImpl.class);

    final HttpConnector httpConnector;

    protected final ClickHouseProperties properties;

    private final ClickHouseConnection connection;

    private AbstractResultSet currentResult;

    private ClickHouseRowBinaryInputStream currentRowBinaryResult;

    private int currentUpdateCount = -1;

    private int queryTimeout = -1;

    private int maxRows;

    private boolean closeOnCompletion;

    private final boolean isResultSetScrollable;

    private volatile String queryId;

    private Boolean isSelect;

    private ClickHouseFormat selectFormat;

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
    String addFormat(String cleanSQL, ClickHouseFormat format) {
        StringBuilder sb = new StringBuilder();
        int idx = cleanSQL.endsWith(";")
                ? cleanSQL.length() - 1
                : cleanSQL.length();
        sb.append(cleanSQL, 0, idx)
                .append(" FORMAT ")
                .append(format.name());
        return sb.toString();
    }

    boolean isSelect(String sql) {
        if (this.isSelect == null) {
            this.isSelect = detectQueryType(sql);
        }
        return this.isSelect;
    }

    private static boolean detectQueryType(String sql) {
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
                                  Map<String, String> additionalRequestParams) throws SQLException {
        InputStream is = sendRequest(sql, additionalDBParams, additionalRequestParams);

        try {
            if (this.isSelect) {
                currentUpdateCount = -1;
                currentResult = createResultSet(is,
                        properties.getBufferSize(),
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
        try (InputStream is = sendRequest(sql, null, null)) {
            //we have to read fully, just in case
            StreamUtils.toByteArray(is);
        } catch (IOException ioe) {
            log.error("Error on executeUpdate() for {}.", sql, ioe);
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
    }

    @Override
    public ClickHouseRowBinaryInputStream executeQueryClickhouseRowBinaryStream(
            String sql,
            Map<ClickHouseQueryParam, String> additionalDBParams) throws SQLException {
        return executeQueryClickhouseRowBinaryStream(sql, additionalDBParams, null);
    }

    @Override
    public void cancel() throws SQLException {
        if (this.queryId != null && !isClosed()) {
            executeQuery("KILL QUERY WHERE query_id='" + this.queryId + "'");
        }
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

    @Override
    public ClickHouseRowBinaryInputStream executeQueryClickhouseRowBinaryStream(
            String sql,
            Map<ClickHouseQueryParam, String> additionalDBParams,
            Map<String, String> additionalRequestParams) throws SQLException {
        String cleanSql = sql.trim();

        this.isSelect = detectQueryType(cleanSql);
        this.selectFormat = ClickHouseFormat.detectFormat(cleanSql);
        if (this.isSelect && this.selectFormat == null) {
            cleanSql = addFormat(cleanSql, ClickHouseFormat.RowBinary);
        }

        InputStream is = sendRequest(cleanSql, additionalDBParams, additionalRequestParams);

        try {
            if (this.isSelect) {
                currentUpdateCount = -1;
                currentRowBinaryResult = new ClickHouseRowBinaryInputStream(is,
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
        URI uri = buildRequestUri(null, additionalDBParams, null, false);
        sql = sql + " FORMAT " + ClickHouseFormat.RowBinary;
        sendStream(sql, callback, uri);
    }

    @Override
    public void sendNativeStream(String sql, ClickHouseStreamCallback callback) throws SQLException {
        sendNativeStream(sql, null, callback);
    }

    @Override
    public void sendNativeStream(String sql, Map<ClickHouseQueryParam, String> additionalDBParams,
                                 ClickHouseStreamCallback callback) throws SQLException {
        URI uri = buildRequestUri(null, additionalDBParams, null, false);
        sql = sql + " FORMAT " + ClickHouseFormat.Native;
        sendStream(sql, callback, uri);
    }

    private void sendStream(String sql, ClickHouseStreamCallback callback, URI uri) throws ClickHouseException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            TimeZone timeZone = getConnection().getTimeZone();
            ClickHouseRowBinaryStream stream = new ClickHouseRowBinaryStream(out, timeZone, properties);
            callback.writeTo(stream);

            InputStream in = new ByteArrayInputStream(out.toByteArray());
            httpConnector.post(sql, in, uri);
        } catch (IOException e) {
            throw ClickHouseExceptionSpecifier.specify(e, properties.getHost(), properties.getPort());
        }
    }

    @Override
    public void sendStream(InputStream content, String table) throws ClickHouseException {
        sendStream(content, table, null);
    }

    @Override
    public void sendStream(InputStream stream, String table,
                           Map<ClickHouseQueryParam, String> additionalDBParams)
            throws ClickHouseException {
        String sql = "INSERT INTO " + table + " FORMAT " + ClickHouseFormat.TabSeparated;
        URI uri = buildRequestUri(null, additionalDBParams, null, false);
        httpConnector.post(sql, stream, uri);
    }

    @Override
    public void sendStreamSQL(InputStream content, String sql) throws ClickHouseException {
        sendStreamSQL(content, sql, null);
    }

    @Override
    public void sendStreamSQL(InputStream content, String sql,
                              Map<ClickHouseQueryParam, String> additionalDBParams) throws ClickHouseException {
        URI uri = buildRequestUri(null, additionalDBParams, null, false);
        httpConnector.post(sql, content, uri);
    }

    public void sendStreamSQL(String sql, OutputStream responseContent) {
        sendStreamSQL(sql, responseContent, null);
    }

    @Override
    public void sendStreamSQL(String sql, OutputStream responseContent,
                              Map<ClickHouseQueryParam, String> additionalDBParams) {
        URI uri = buildRequestUri(null, additionalDBParams, null, false);
        try (InputStream is = httpConnector.post(sql, uri)) {
            StreamUtils.copy(is, responseContent);
        } catch (Exception e) {
            log.error("Error on sendStreamSQL()", e);
        }
    }

    public void closeOnCompletion() {
        closeOnCompletion = true;
    }

    public boolean isCloseOnCompletion() {
        return closeOnCompletion;
    }

    private AbstractResultSet createResultSet(InputStream is,
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
        }

        if (this.selectFormat == ClickHouseFormat.JSON || this.selectFormat == ClickHouseFormat.JSONCompact) {
            return new ClickHouseJsonResultSet(
                    is,
                    bufferSize,
                    statement,
                    properties
            );
        }

        return new ClickHouseResultSet(is,
                                       bufferSize,
                                       db,
                                       table,
                                       usesWithTotals,
                                       statement,
                                       timezone,
                                       properties);
    }

    private void addQueryIdTo(Map<ClickHouseQueryParam, String> parameters) {
        if (this.queryId == null && properties.isEnableQueryId()) {
            String queryId = parameters.get(ClickHouseQueryParam.QUERY_ID);
            if (queryId == null) {
                this.queryId = ClickHouseUtil.generateQueryId();
                parameters.put(ClickHouseQueryParam.QUERY_ID, this.queryId);
            } else {
                this.queryId = queryId;
            }
        }
    }

    private InputStream sendRequest(
            String sql,
            Map<ClickHouseQueryParam, String> additionalClickHouseDBParams,
            Map<String, String> additionalRequestParams
    ) throws ClickHouseException {
        String cleanSql = sql.trim();

        this.isSelect = detectQueryType(cleanSql);
        this.selectFormat = ClickHouseFormat.detectFormat(cleanSql);
        if (this.isSelect && this.selectFormat == null) {
            cleanSql = addFormat(cleanSql, ClickHouseFormat.TabSeparatedWithNamesAndTypes);
        }

        if (additionalClickHouseDBParams == null) {
            additionalClickHouseDBParams = new EnumMap<>(ClickHouseQueryParam.class);
        }
        addQueryIdTo(additionalClickHouseDBParams);

        boolean ignoreDatabase = cleanSql.regionMatches(true, 0, databaseKeyword, 0, databaseKeyword.length());
        URI uri = buildRequestUri(
                    null,
                    additionalClickHouseDBParams,
                    additionalRequestParams,
                    ignoreDatabase
            );
        log.debug("Executing SQL: \"{}\", url: {}", cleanSql, uri);
        return httpConnector.post(cleanSql, uri);
    }

    URI buildRequestUri(
            String sql,
            Map<ClickHouseQueryParam, String> additionalClickHouseDBParams,
            Map<String, String> additionalRequestParams,
            boolean ignoreDatabase) {
        try {
            String queryParams = toParamsString(
                    getUrlQueryParams(sql,
                                      additionalClickHouseDBParams,
                                      additionalRequestParams,
                                      ignoreDatabase,
                                      this.properties,
                                      this.initialDatabase,
                                      this.maxRows,
                                      this.queryTimeout)
            );

            return ClickHouseUtil.buildURI(this.properties, queryParams);
        } catch (URISyntaxException e) {
            log.error("Mailformed URL: {}", e.getMessage());
            throw new IllegalStateException("illegal configuration of db");
        }
    }

    private static String toParamsString(List<SimpleImmutableEntry<String, String>> queryParams) {
        if (queryParams.isEmpty()) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (SimpleImmutableEntry<String, String> pair : queryParams) {
            sb.append(pair.getKey()).append("=").append(pair.getValue())
                    .append("&");
        }
        sb.setLength(sb.length() - 1); //remove last &
        return sb.toString();
    }

    private static List<SimpleImmutableEntry<String, String>> getUrlQueryParams(
            String sql,
            Map<ClickHouseQueryParam, String> additionalClickHouseDBParams,
            Map<String, String> additionalRequestParams,
            boolean ignoreDatabase,
            ClickHouseProperties properties,
            String initialDatabase,
            int maxRows,
            int queryTimeout) {
        List<SimpleImmutableEntry<String, String>> result = new ArrayList<>();

        if (sql != null) {
            result.add(new SimpleImmutableEntry<>("query", sql));
        }

        Map<ClickHouseQueryParam, String> params = properties.buildQueryParams(true);
        if (!ignoreDatabase && !ClickhouseJdbcUrlParser.DEFAULT_DATABASE.equals(initialDatabase)) {
            params.put(ClickHouseQueryParam.DATABASE, initialDatabase);
        }

        if (additionalClickHouseDBParams != null && !additionalClickHouseDBParams.isEmpty()) {
            params.putAll(additionalClickHouseDBParams);
        }

        if (maxRows > 0) {
            params.put(ClickHouseQueryParam.MAX_RESULT_ROWS, String.valueOf(maxRows));
            params.put(ClickHouseQueryParam.RESULT_OVERFLOW_MODE, "break");
        }
        if (queryTimeout > 0) {
            params.put(ClickHouseQueryParam.MAX_EXECUTION_TIME, String.valueOf(queryTimeout));
        }

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

}
