package cc.blynk.clickhouse;

import cc.blynk.clickhouse.domain.ClickHouseFormat;
import cc.blynk.clickhouse.http.HttpConnector;
import cc.blynk.clickhouse.settings.ClickHouseProperties;
import cc.blynk.clickhouse.settings.ClickHouseQueryParam;
import cc.blynk.clickhouse.util.ClickHouseArrayUtil;
import cc.blynk.clickhouse.util.ClickHouseValueFormatter;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class ClickHousePreparedStatementImpl extends ClickHouseStatementImpl
        implements ClickHousePreparedStatement {

    static final String PARAM_MARKER = "?";

    private static final Pattern VALUES = Pattern.compile("(?i)VALUES[\\s]*\\(");

    private final TimeZone dateTimeZone;
    private final TimeZone dateTimeTimeZone;
    private final String sql;
    private final List<String> sqlParts;
    private final ClickHousePreparedStatementParameter[] binds;
    private final String[][] parameterList;
    private List<byte[]> batchRows = new ArrayList<>();

    ClickHousePreparedStatementImpl(HttpConnector connector,
                                    ClickHouseConnection connection,
                                    ClickHouseProperties properties,
                                    String sql,
                                    TimeZone serverTimeZone,
                                    int resultSetType) {
        super(connector, connection, properties, resultSetType);
        this.sql = sql;
        PreparedStatementParser parser = PreparedStatementParser.parse(sql);
        this.parameterList = parser.getParameters();
        this.sqlParts = parser.getParts();
        int numParams = countNonConstantParams();
        this.binds = new ClickHousePreparedStatementParameter[numParams];
        dateTimeTimeZone = serverTimeZone;
        if (properties.isUseServerTimeZoneForDates()) {
            dateTimeZone = serverTimeZone;
        } else {
            dateTimeZone = TimeZone.getDefault();
        }
    }

    @Override
    public void clearParameters() {
        Arrays.fill(binds, null);
    }

    private String buildSql() throws SQLException {
        if (sqlParts.size() == 1) {
            return sqlParts.get(0);
        }
        checkBinded();
        StringBuilder sb = new StringBuilder(sqlParts.get(0));
        for (int i = 1, p = 0; i < sqlParts.size(); i++) {
            String pValue = getParameter(i - 1);
            if (PARAM_MARKER.equals(pValue)) {
                sb.append(binds[p++].getRegularValue());
            } else {
                sb.append(pValue);
            }
            sb.append(sqlParts.get(i));
        }
        return sb.toString();
    }

    private void checkBinded() throws SQLException {
        int i = 0;
        for (Object b : binds) {
            ++i;
            if (b == null) {
                throw new SQLException("Not all parameters binded (placeholder " + i + " is undefined)");
            }
        }
    }

    @Override
    public boolean execute() throws SQLException {
        return super.execute(buildSql());
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return super.executeQuery(buildSql());
    }

    @Override
    public void clearBatch() {
        batchRows.clear();
    }

    @Override
    public ResultSet executeQuery(Map<ClickHouseQueryParam, String> additionalDBParams) throws SQLException {
        return super.executeQuery(buildSql(), additionalDBParams);
    }

    @Override
    public ResultSet executeQuery(Map<ClickHouseQueryParam, String> additionalDBParams,
                                  List<ClickHouseExternalData> externalData) throws SQLException {
        return super.executeQuery(buildSql(), additionalDBParams, externalData);
    }

    @Override
    public int executeUpdate() throws SQLException {
        return super.executeUpdate(buildSql());
    }

    private void setBind(int parameterIndex, String bind, boolean quote) {
        setBind(parameterIndex, new ClickHousePreparedStatementParameter(bind, quote));
    }

    private void setBind(int parameterIndex, ClickHousePreparedStatementParameter parameter) {
        binds[parameterIndex - 1] = parameter;
    }

    private void setNull(int parameterIndex) {
        setBind(parameterIndex, ClickHousePreparedStatementParameter.NULL_PARAM);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        setNull(parameterIndex);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setBind(parameterIndex,
                x ? ClickHousePreparedStatementParameter.TRUE_PARAM
                  : ClickHousePreparedStatementParameter.FALSE_PARAM);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        setBind(parameterIndex, ClickHouseValueFormatter.formatByte(x), false);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        setBind(parameterIndex, ClickHouseValueFormatter.formatShort(x), false);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        setBind(parameterIndex, ClickHouseValueFormatter.formatInt(x), false);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        setBind(parameterIndex, ClickHouseValueFormatter.formatLong(x), false);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        setBind(parameterIndex, ClickHouseValueFormatter.formatFloat(x), false);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        setBind(parameterIndex, ClickHouseValueFormatter.formatDouble(x), false);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        setBind(parameterIndex, ClickHouseValueFormatter.formatBigDecimal(x), false);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        setBind(parameterIndex, ClickHouseValueFormatter.formatString(x), x != null);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        setBind(parameterIndex, ClickHouseValueFormatter.formatBytes(x), true);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        if (x != null) {
            String bind = ClickHouseValueFormatter.formatDate(x, dateTimeZone);
            setBind(parameterIndex, bind, true);
        } else {
            setNull(parameterIndex);
        }
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        if (x != null) {
            String bind = ClickHouseValueFormatter.formatTime(x, dateTimeTimeZone);
            setBind(parameterIndex, bind, true);
        } else {
            setNull(parameterIndex);
        }
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        if (x != null) {
            String bind = ClickHouseValueFormatter.formatTimestamp(x, dateTimeTimeZone);
            setBind(parameterIndex, bind, true);
        } else {
            setNull(parameterIndex);
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    @Deprecated
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setArray(int parameterIndex, Collection collection) throws SQLException {
        setBind(parameterIndex, ClickHouseArrayUtil.toString(collection, dateTimeZone, dateTimeTimeZone),
                false);
    }

    @Override
    public void setArray(int parameterIndex, Object[] array) throws SQLException {
        setBind(parameterIndex, ClickHouseArrayUtil.toString(array, dateTimeZone, dateTimeTimeZone), false);
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        setBind(parameterIndex, ClickHouseArrayUtil.arrayToString(x.getArray(), dateTimeZone, dateTimeTimeZone),
                false);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        if (x != null) {
            String bind = ClickHouseValueFormatter.formatObject(x, dateTimeZone, dateTimeTimeZone);
            boolean needQuoting = ClickHouseValueFormatter.needsQuoting(x);
            ClickHousePreparedStatementParameter bindParam =
                    new ClickHousePreparedStatementParameter(bind, needQuoting);
            setBind(parameterIndex, bindParam);
        } else {
            setNull(parameterIndex);
        }
    }

    @Override
    public void addBatch() throws SQLException {
        checkBinded();
        StringBuilder sb;
        for (int i = 0, p = 0; i < parameterList.length; i++) {
            sb = new StringBuilder();
            String[] batchParams = parameterList[i];
            int batchParamsLength = batchParams.length;
            for (int j = 0; j < batchParamsLength; j++) {
                String batchVal = batchParams[j];
                if (PARAM_MARKER.equals(batchVal)) {
                    ClickHousePreparedStatementParameter param = binds[p++];
                    batchVal = param.getBatchValue();
                }
                sb.append(batchVal);
                char appendChar = j < batchParamsLength - 1 ? '\t' : '\n';
                sb.append(appendChar);
            }
            byte[] batchBytes = sb.toString().getBytes(UTF_8);
            batchRows.add(batchBytes);
        }
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return executeBatch(null);
    }

    @Override
    public int[] executeBatch(Map<ClickHouseQueryParam, String> additionalDBParams) throws SQLException {
        Matcher matcher = VALUES.matcher(sql);
        if (!matcher.find()) {
            throw new SQLSyntaxErrorException(
                    "Query must be like 'INSERT INTO [db.]table [(c1, c2, c3)] VALUES (?, ?, ?)'. " +
                            "Got: " + sql
            );
        }
        int valuePosition = matcher.start();
        String insertSql = sql.substring(0, valuePosition);

        URI uri = buildRequestUri(null, null, additionalDBParams, null, false);
        insertSql = insertSql + " FORMAT " + ClickHouseFormat.TabSeparated.name();

        httpConnector.post(insertSql, batchRows, uri);
        int[] result = new int[batchRows.size()];
        Arrays.fill(result, 1);
        batchRows = new ArrayList<>();
        return result;
    }


    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        ResultSet currentResult = getResultSet();
        if (currentResult != null) {
            return currentResult.getMetaData();
        }
        if (!isSelect(sql)) {
            return null;
        }
        ResultSet myRs = executeQuery(Collections.singletonMap(
                ClickHouseQueryParam.MAX_RESULT_ROWS, "0"));
        return myRs != null ? myRs.getMetaData() : null;
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        setNull(parameterIndex, sqlType);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        setString(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    private int countNonConstantParams() {
        int count = 0;
        for (String[] pList : parameterList) {
            for (String aPList : pList) {
                if (PARAM_MARKER.equals(aPList)) {
                    count += 1;
                }
            }
        }
        return count;
    }

    private String getParameter(int paramIndex) {
        for (int i = 0, count = paramIndex; i < parameterList.length; i++) {
            String[] pList = parameterList[i];
            count -= pList.length;
            if (count < 0) {
                return pList[pList.length + count];
            }
        }
        return null;
    }

    @Override
    public String asSql() {
        try {
            return buildSql();
        } catch (SQLException e) {
            return sql;
        }
    }
}
