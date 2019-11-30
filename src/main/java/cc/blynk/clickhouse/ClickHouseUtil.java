package cc.blynk.clickhouse;

import cc.blynk.clickhouse.settings.ClickHouseProperties;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.SecureRandom;
import java.util.Base64;

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

    public static URI buildURI(ClickHouseProperties properties, String queryParams) throws URISyntaxException {
        return new URI(properties.getSsl() ? "https" : "http",
                       null,
                       properties.getHost(),
                       properties.getPort(),
                       properties.getPath(),
                       queryParams,
                       null
        );
    }

    private static String escapeSlow(String s, int index) {
        StringBuilder builder = new StringBuilder();
        builder.append(s, 0, index);

        for (int i = index; i < s.length(); i++) {
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

    //threadsafe
    private static final SecureRandom secureRandom = new SecureRandom();
    //threadsafe
    private static final Base64.Encoder base64Encoder = Base64.getUrlEncoder();

    public static String generateQueryId() {
        byte[] randomBytes = new byte[16];
        secureRandom.nextBytes(randomBytes);
        return base64Encoder.encodeToString(randomBytes);
    }

}
