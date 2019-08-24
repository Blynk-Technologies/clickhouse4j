package ru.yandex.clickhouse.util;

import java.util.ArrayList;
import java.util.List;

public final class Utils {

    private Utils() {
    }

    public static boolean startsWithIgnoreCase(String haystack, String pattern) {
        return haystack.substring(0, pattern.length()).equalsIgnoreCase(pattern);
    }

    public static String retainUnquoted(String haystack, char quoteChar) {
        StringBuilder sb = new StringBuilder();
        String[] split = splitWithoutEscaped(haystack, quoteChar);
        for (int i = 0; i < split.length; i++) {
            String s = split[i];
            if ((i & 1) == 0) {
                sb.append(s);
            }
        }
        return sb.toString();
    }

    /**
     * Does not take into account escaped separators
     *
     * @param str  the String to parse, may be null
     * @param separatorChar  the character used as the delimiter
     * @return string array
     */
    private static String[] splitWithoutEscaped(String str, char separatorChar) {
        int len = str.length();
        if (len == 0) {
            return new String[0];
        }
        List<String> list = new ArrayList<>();
        int i = 0;
        int start = 0;
        while (i < len) {
            if (str.charAt(i) == '\\') {
                i += 2;
            } else if (str.charAt(i) == separatorChar) {
                list.add(str.substring(start, i));
                start = ++i;
            } else {
                i++;
            }
        }
        list.add(str.substring(start, i));
        return list.toArray(new String[0]);
    }

}
