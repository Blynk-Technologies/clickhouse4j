package ru.yandex.clickhouse;

public final class ClickHouseUtil {

    private ClickHouseUtil() {
    }

    public static String escape(String string) {
        if (string == null) {
            return "\\N";
        }

        for (int i = 0; i < string.length(); i++) {
            //fast path, if no chars for escape - do no nothing
            if (escape(string.charAt(i)) != null) {
                //if escape char found - slow path
                return escapeSlow(string, i);
            }
        }
        return string;
    }

    private static String escapeSlow(String s, int index) {
        StringBuilder builder = new StringBuilder();
        builder.append(s, 0, index);

        for (int i = index; i < s.length(); i++) {
            char currentChar = s.charAt(i);
            String replacement = escape(currentChar);
            builder.append(replacement);
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
                return String.valueOf(c);
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
