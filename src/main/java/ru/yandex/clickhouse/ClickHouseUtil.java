package ru.yandex.clickhouse;

import java.util.Map;

public final class ClickHouseUtil {

    private static final Map<Character, String> CHARS_MAP = Map.of(
            '\\', "\\\\",
            '\n', "\\n",
            '\t', "\\t",
            '\b', "\\b",
            '\f', "\\f",
            '\r', "\\r",
            '\0', "\\0",
            '\'', "\\'",
            '`', "\\`"
    );

    private ClickHouseUtil() {
    }

    public static String escape(String s) {
        if (s == null) {
            return "\\N";
        }

        StringBuilder builder = new StringBuilder();

        for (int i = 0; i < s.length(); i++) {
            char currentChar = s.charAt(i);
            if (CHARS_MAP.containsKey(currentChar)) {
                String replacement = CHARS_MAP.get(currentChar);
                builder.append(replacement);
            } else {
                builder.append(currentChar);
            }
        }

        return builder.toString();
    }

    static String quoteIdentifier(String s) {
        if (s == null) {
            throw new IllegalArgumentException("Can't quote null as identifier");
        }
        String escaped = escape(s);
        return '`' + escaped + '`';
    }

}
