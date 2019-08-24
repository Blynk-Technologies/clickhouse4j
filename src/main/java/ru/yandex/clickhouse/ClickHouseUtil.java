package ru.yandex.clickhouse;

public final class ClickHouseUtil {

    private ClickHouseUtil() {
    }

    public static String escape(String s) {
        if (s == null) {
            return "\\N";
        }

        StringBuilder builder = new StringBuilder();

        for (int i = 0; i < s.length(); i++) {
            char currentChar = s.charAt(i);
            String replacement = escape(currentChar);
            if (replacement != null) {
                builder.append(replacement);
            } else {
                builder.append(currentChar);
            }
        }

        return builder.toString();
    }

    private static String escape(char c) {
        switch (c) {
            case '\\':
                return "\\\\";
            case '\n':
                return "\\n";
            case '\t':
                return "\\t";
            case '\b':
                return "\\b";
            case '\f':
                return "\\f";
            case '\r':
                return "\\r";
            case '\0':
                return "\\0";
            case '\'':
                return "\\'";
            case '`':
                return "\\`";
            default :
                return null;
        }
    }

    static String quoteIdentifier(String s) {
        if (s == null) {
            throw new IllegalArgumentException("Can't quote null as identifier");
        }
        String escaped = escape(s);
        return '`' + escaped + '`';
    }

}
