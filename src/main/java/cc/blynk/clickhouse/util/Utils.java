package cc.blynk.clickhouse.util;

import net.jpountz.lz4.LZ4Factory;

import java.util.ArrayList;
import java.util.List;

public final class Utils {

    protected static final LZ4Factory factory = LZ4Factory.fastestInstance();

    private Utils() {
    }

    public static String retainUnquoted(String haystack, char quoteChar) {
        if (haystack.isEmpty()) {
            return "";
        }

        //quick path, nothing to remove
        int startIndex = haystack.indexOf(quoteChar);
        if (startIndex == -1) {
            return haystack;
        }

        StringBuilder sb = new StringBuilder();
        List<String> split = splitWithoutEscaped(haystack, startIndex, quoteChar);
        for (int i = 0; i < split.size(); i += 2) {
            String s = split.get(i);
            sb.append(s);
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
    private static List<String> splitWithoutEscaped(String str, int start, char separatorChar) {
        List<String> list = new ArrayList<>();
        int i = start;
        start = 0;
        int strLen = str.length();
        while (i < strLen) {
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
        return list;
    }

}
