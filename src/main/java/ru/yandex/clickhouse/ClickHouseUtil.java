package ru.yandex.clickhouse;


import com.google.common.escape.Escaper;
import com.google.common.escape.Escapers;

public final class ClickHouseUtil {

    private ClickHouseUtil() {
    }

    private final static Escaper CLICKHOUSE_ESCAPER = Escapers.builder()
        .addEscape('\\', "\\\\")
        .addEscape('\n', "\\n")
        .addEscape('\t', "\\t")
        .addEscape('\b', "\\b")
        .addEscape('\f', "\\f")
        .addEscape('\r', "\\r")
        .addEscape('\0', "\\0")
        .addEscape('\'', "\\'")
        .addEscape('`', "\\`")
        .build();

    public static String escape(String s) {
        if (s == null) {
            return "\\N";
        }
        return CLICKHOUSE_ESCAPER.escape(s);
    }

    static String quoteIdentifier(String s) {
        if (s == null) {
            throw new IllegalArgumentException("Can't quote null as identifier");
        }
        String escaped = CLICKHOUSE_ESCAPER.escape(s);
        return '`' + escaped + '`';
    }

}
