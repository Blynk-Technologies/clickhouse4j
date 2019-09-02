package cc.blynk.clickhouse;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;


/**
 * Unit tests for {@link PreparedStatementParser}
 */
public class PreparedStatementParserTest {

    @Test
    public void testNullSafety() {
        try {
            PreparedStatementParser.parse(null);
            Assert.fail();
        } catch (IllegalArgumentException iae) { /* expected */ }
    }

    @Test
    public void testParseSimple() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (a, b) VALUES (?, ?)");
        assertMatchParams(new String[][] {{"?", "?"}}, s);
    }

    @Test
    public void testParseConstantSimple() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (a, b) VALUES (?, 'foo')");
        assertMatchParams(new String[][] {{"?", "'foo'"}}, s);
    }

    @Test
    public void testParseSimpleWhitespaceValueMode() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "    INSERT\t INTO t(a, b)    VALUES(?, ?)");
        assertMatchParams(new String[][] {{"?", "?"}}, s);
    }

    @Test
    public void testParseConstantSimpleInt() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (a, b) VALUES (?, 42)");
        assertMatchParams(new String[][] {{"?", "42"}}, s);
    }

    @Test
    public void testParseConstantSimpleIntTrailingWhitespace() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (a, b) VALUES (?,42 )");
        assertMatchParams(new String[][] {{"?", "42"}}, s);
    }

    @Test
    public void testParseConstantSimpleIntTrailingLeadingWhitespace() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (a, b) VALUES (?, 42 )");
        assertMatchParams(new String[][] {{"?", "42"}}, s);
    }

    @Test
    public void testParseParentheses() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (a, b) VALUES ((?), ('foo'))");
        assertMatchParams(new String[][] {{"?", "'foo'"}}, s);
    }

    @Test
    public void testParseParenthesesInQuotes() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (a, b) VALUES ((?), ('foo)))'))");
        assertMatchParams(new String[][] {{"?", "'foo)))'"}}, s);
    }

    @Test
    public void testParseEscapedQuote() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (a, b) VALUES (?, 'foo\\'bar')");
        assertMatchParams(new String[][] {{"?", "'foo\\'bar'"}}, s);
    }

    @Test
    public void testParseEscapedQuoteBroken() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (a, b) VALUES (?, 'foo\'bar')");
        assertEquals(0, s.getParameters().length); // Is this expected?
    }

    @Test
    public void testParseQuestionMarkInQuotes() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (a, b) VALUES ('?', 'foo')");
        assertMatchParams(new String[][] {{"'?'", "'foo'"}}, s);
    }

    @Test
    public void testParseQuestionMarkAndMoreInQuotes() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (a, b) VALUES ('? foo ?', 'bar')");
        assertMatchParams(new String[][] {{"'? foo ?'", "'bar'"}}, s);
    }

    @Test
    public void testParseEscapedQuestionMark() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (a, b) VALUES (\\?, 'foo')");
        assertMatchParams(new String[][] {{"'foo'"}}, s);
    }

    @Test
    public void testNoCommasQuestionMarks() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (a, b) VALUES (foo ? bar ?)");
        String[][] matrix = s.getParameters();
        assertEquals(matrix.length, 1);
        assertEquals(matrix[0].length, 1);
    }

    @Test
    public void testParseIgnoreInsert() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (foo, ?) VALUES (?, 'bar')");
        assertMatchParams(new String[][] {{"?", "'bar'"}}, s);
    }

    @Test
    public void testDoubleComma() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (a, b) VALUES (?, 'bar',, ?, , ?)");
        assertMatchParams(new String[][] {{"?", "'bar'", "?", "?"}}, s);
    }

    @Test
    public void testDoubleSingleQuote() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (a) VALUES ('')");
        assertMatchParams(new String[][] {{"''"}}, s);
    }

    @Test
    public void testInsertNumbers() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (foo, bar, baz) VALUES (42, 23, '42')");
        assertMatchParams(new String[][] {{"42", "23", "'42'"}}, s);
    }

    @Test
    public void testInsertBoolean() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (foo, bar) VALUES (TRUE, false)");
        assertMatchParams(new String[][] {{"1", "0"}}, s);
    }

    @Test
    public void testMultiParams() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (a, b) VALUES (?, ?), (?, ?)");
        assertMatchParams(
            new String[][] {
                {"?", "?" },
                {"?", "?" }
            },
            s);
    }

    @Test
    public void testMultiParamsWithConstants() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (a, b) VALUES (?, 'foo'), ('bar', ?)");
        assertMatchParams(
            new String[][] {
                {"?", "'foo'" },
                {"'bar'", "?" }
            },
            s);
    }

    @Test
    public void testValuesModeDoubleQuotes() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (\"foo.bar\") VALUES (?)");
        assertMatchParams(new String[][] {{"?"}}, s);
        assertEquals(s.getParts().get(0), "INSERT INTO t (\"foo.bar\") VALUES (");
    }

    @Test
    public void testValuesModeDoubleQuotesValues() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (\"foo.bar\") VALUES (\"baz\")");
        assertMatchParams(new String[][] {{"\"baz\""}}, s);
        assertEquals(s.getParts().get(0), "INSERT INTO t (\"foo.bar\") VALUES (");
    }

    @Test
    public void testValuesModeSingleQuotes() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t ('foo.bar') VALUES (?)");
        assertMatchParams(new String[][] {{"?"}}, s);
        assertEquals(s.getParts().get(0), "INSERT INTO t ('foo.bar') VALUES (");
    }

    @Test
    public void testValuesModeSingleQuotesValues() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t ('foo.bar') VALUES ('baz')");
        assertMatchParams(new String[][] {{"'baz'"}}, s);
        assertEquals(s.getParts().get(0), "INSERT INTO t ('foo.bar') VALUES (");
    }

    @Test
    public void testParseInsertSelect() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (a, b) SELECT x, y");
        assertEquals(s.getParts().get(0), "INSERT INTO t (a, b) SELECT x, y");
        assertEquals(s.getParameters().length, 0);
    }

    @Test
    public void testParseInsertSelectParams() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (a, b) SELECT x FROM u WHERE y = ? AND z = ?");
        assertEquals(s.getParts().get(0),
            "INSERT INTO t (a, b) SELECT x FROM u WHERE y = ");
        assertEquals(" AND z = ", s.getParts().get(1));
        assertMatchParams(new String[][] {{"?",  "?"}}, s);
    }

    @Test
    public void testParseSelectGroupBy() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "SELECT SUM(x) FROM t WHERE y = ? GROUP BY ? HAVING COUNT(z) > ? ORDER BY z DESC");
        assertEquals("SELECT SUM(x) FROM t WHERE y = ",
            s.getParts().get(0));
        assertEquals(s.getParts().get(1), " GROUP BY ");
        assertEquals(s.getParts().get(2), " HAVING COUNT(z) > ");
        assertEquals(s.getParts().get(3), " ORDER BY z DESC");
        assertMatchParams(new String[][] {{"?", "?", "?"}}, s);
    }

    @Test
    public void testParseWithComment1() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "select a --what is it?\nfrom t where a = ? and b = 1");
        assertEquals( s.getParts().get(0), "select a --what is it?\nfrom t where a = ");
        assertEquals(s.getParts().get(1), " and b = 1");
        assertMatchParams(new String[][] {{"?"}}, s);
    }

    @Test
    public void testParseWithComment2() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "select a /*what is it?*/ from t where a = ? and b = 1");
        assertMatchParts(new String[] {
            "select a /*what is it?*/ from t where a = ",
            " and b = 1"},
            s);
        assertMatchParams(new String[][] {{"?"}}, s);
    }

    @Test
    public void testParseSelectStar() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "SELECT * FROM tbl");
        assertMatchParts(new String[] {"SELECT * FROM tbl"}, s);
        assertEquals(s.getParameters().length, 0);
    }

    @Test
    public void testParseSelectStarParam() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "SELECT * FROM tbl WHERE t = ?");
        assertMatchParts(new String[] {"SELECT * FROM tbl WHERE t = ", ""}, s);
        assertMatchParams(new String[][] {{"?"}}, s);
    }

    @Test
    public void testParseSelectEscapedGarbage() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "SELECT 'a\\'\\\\sdfasdf?adsf\\\\' as `sadf\\`?` FROM tbl WHERE t = ? AND r = ? ORDER BY 1");
        assertMatchParts(new String[] {
            "SELECT 'a\\'\\\\sdfasdf?adsf\\\\' as `sadf\\`?` FROM tbl WHERE t = ",
            " AND r = ",
            " ORDER BY 1"},
            s);
        assertMatchParams(new String[][] {{"?", "?"}}, s);
    }

    @Test
    public void testRegularParam() throws Exception {
        // Test inspired by MetaBase test cases
        PreparedStatementParser s = PreparedStatementParser.parse(
            "SELECT count(*) AS `count` FROM `foo`.`bar` "
          + "WHERE (`foo`.`bar`.`id` <= 32 "
          + "AND (`foo`.`bar`.`name` like ?))");
        assertMatchParams(new String[][] {{"?"}}, s);
        assertEquals(
            s.getParts().get(0),
            "SELECT count(*) AS `count` FROM `foo`.`bar` "
          + "WHERE (`foo`.`bar`.`id` <= 32 "
          + "AND (`foo`.`bar`.`name` like ");
        assertEquals(
            s.getParts().get(1),
            "))");
    }

    @Test
    public void testRegularParamWhitespace() throws Exception {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "SELECT count(*) AS `count` FROM `foo`.`bar` "
          + "WHERE (`foo`.`bar`.`id` <= 32 "
          + "AND (`foo`.`bar`.`name` like ?   ))");
        assertMatchParams(new String[][] {{"?"}}, s);
        assertEquals(
            s.getParts().get(0),
            "SELECT count(*) AS `count` FROM `foo`.`bar` "
          + "WHERE (`foo`.`bar`.`id` <= 32 "
          + "AND (`foo`.`bar`.`name` like ");
        assertEquals(
            s.getParts().get(1),
            "   ))");
    }

    @Test
    public void testRegularParamInFunction() throws Exception {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "SELECT count(*) AS `count` FROM `foo`.`bar` "
          + "WHERE toMonday(`foo`.`bar`.`date`) = toMonday(?)");
        assertMatchParams(new String[][] {{"?"}}, s);
        assertEquals(
            s.getParts().get(0),
            "SELECT count(*) AS `count` FROM `foo`.`bar` "
          + "WHERE toMonday(`foo`.`bar`.`date`) = toMonday(");
        assertEquals(
            s.getParts().get(1),
            ")");
    }

    @Test
    public void testNullValuesSelect() throws Exception {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "SELECT 1 FROM foo WHERE bar IN (?, NULL)");
        String[][] params = s.getParameters();
        assertEquals(params.length, 1);
        assertEquals(params[0].length, 1);
        assertEquals(params[0][0], "?");
    }

    @Test
    public void testNullValuesInsert() throws Exception {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO test.prep_nullable_value (s, i, f) VALUES "
          + "(?, NULL, ?), (NULL, null , ?)");
        assertMatchParams(new String[][] {
            {"?", "\\N", "?"},
            {"\\N", "\\N", "?"}},
            s);
    }

    @Test
    public void testParamLastCharacter() throws Exception {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "SELECT * FROM decisions "
          + "PREWHERE userID = ? "
          + "AND eventDate >= toDate(?) "
          + "AND eventDate <= toDate(?) "
          + "ORDER BY time DESC LIMIT ?, ?");
        assertMatchParams(new String[][] {{"?", "?", "?", "?", "?"}}, s);
        assertEquals(s.getParts().size(), 6);
        assertEquals(s.getParts().get(0), "SELECT * FROM decisions PREWHERE userID = ");
        assertEquals(s.getParts().get(1), " AND eventDate >= toDate(");
        assertEquals(s.getParts().get(2), ") AND eventDate <= toDate(");
        assertEquals(s.getParts().get(3), ") ORDER BY time DESC LIMIT ");
        assertEquals(s.getParts().get(4), ", ");
        assertEquals(s.getParts().get(5), "");
    }
    
    @Test
    public void testSingleAndBackMixedQuotes() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "SELECT '`' as `'` WHERE 0 = ?");
        assertMatchParams(new String[][] {{"?"}}, s);
        assertEquals(s.getParts().get(0), "SELECT '`' as `'` WHERE 0 = ");
    }


    @Test
    public void testInsertValuesFunctions() throws Exception {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO foo(id, src, dst) "
          + "VALUES (?, IPv4ToIPv6(toIPv4(?)), IPv4ToIPv6(toIPv4(?)))");
        assertMatchParams(new String[][] {{ "?", "?", "?" }}, s);
        assertMatchParts(new String[] {
            "INSERT INTO foo(id, src, dst) VALUES (",
            ", IPv4ToIPv6(toIPv4(",
            ")), IPv4ToIPv6(toIPv4(",
            ")))"}, s);
    }

    @Test
    public void testMultiLineValues() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO table1\n"
          + "\t(foo, bar)\r\n"
          + "\t\tVALUES\n"
          + "(?, ?) , \r\n"
          + "\t(?,?),(?,?)\n");
        assertTrue(s.isValuesMode());
        assertMatchParams(new String[][] {{"?", "?"}, {"?", "?"}, {"?", "?"}}, s);
        assertEquals(s.getParts().get(0),
            "INSERT INTO table1\n"
          + "\t(foo, bar)\r\n"
          + "\t\tVALUES\n"
          + "(");
        assertEquals(7, s.getParts().size());
        assertEquals(s.getParts().get(0),
            "INSERT INTO table1\n"
          + "\t(foo, bar)\r\n"
          + "\t\tVALUES\n"
          + "(");
        assertEquals(s.getParts().get(1), ", ");
        assertEquals(s.getParts().get(2),
            ") , \r\n"
          + "\t(");
        assertEquals(s.getParts().get(3), ",");
        assertEquals(s.getParts().get(4), "),(");
        assertEquals(s.getParts().get(5), ",");
        assertEquals(s.getParts().get(6), ")\n");
    }

    private static void assertMatchParts(String[] expected, PreparedStatementParser stmt) {
        List<String> parts = stmt.getParts();
        assertEquals( parts.size(), expected.length);
        for (int i = 0; i < expected.length; i++) {
            assertEquals(parts.get(i), expected[i]);
        }
    }

    private static void assertMatchParams(String[][] expected, PreparedStatementParser stmt) {
        String[][] actual = stmt.getParameters();
        if (expected.length != actual.length) {
            assertEquals(formatParamsList(actual), formatParams(expected));
        }
        if (expected.length == 0 && actual.length == 0) {
            return;
        }
        for (int i = 0; i < expected.length; i++) {
            String[] expRow = expected[i];
            String[] actRow = actual[i];
            assertEquals(actRow.length, expRow.length);
            for (int j = 0; j < expRow.length; j++) {
                assertEquals(actRow[j], expRow[j]);
            }
        }
    }

    private static String formatParamsList(String[][] params) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < params.length; i++) {
            sb.append("row ")
              .append(i)
              .append(": ")
              .append(formatRow(params[i]))
              .append("\n");
        }
        return sb.length() > 1 ?
            sb.substring(0, sb.length() - 1)
            : null;
    }

    private static String formatParams(String[][] params) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < params.length; i++) {
            sb.append("row ")
              .append(i)
              .append(": ")
              .append(formatRow(params[i]))
              .append("\n");
        }
        return sb.substring(0, sb.length() - 1);
    }

    private static String formatRow(String[] paramGroup) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < paramGroup.length; i++) {
            sb.append(i)
              .append(": ")
              .append(paramGroup[i])
              .append(", ");
        }
        return sb.substring(0,  sb.length() - 2);
    }

}
