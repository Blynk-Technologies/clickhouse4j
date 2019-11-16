package cc.blynk.clickhouse.domain;

/**
 * Input / Output formats supported by ClickHouse
 * <p>
 * Note that the sole existence of a format in this enumeration does not mean
 * that its use is supported for any operation with this JDBC driver. When in
 * doubt, just omit any specific format and let the driver take care of it.
 * <p>
 *
 * @see <a href="https://clickhouse.yandex/docs/en/interfaces/formats">ClickHouse Reference Documentation</a>
 *
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 */
public enum ClickHouseFormat {

    TabSeparated,
    TabSeparatedRaw,
    TabSeparatedWithNames,
    TabSeparatedWithNamesAndTypes,
    CSV,
    CSVWithNames,
    Values,
    Vertical,
    JSON,
    JSONCompact,
    JSONEachRow,
    TSKV,
    Pretty,
    PrettyCompact,
    PrettyCompactMonoBlock,
    PrettyNoEscapes,
    PrettySpace,
    Protobuf,
    RowBinary,
    Native,
    Null,
    XML,
    CapnProto;

    private static final ClickHouseFormat[] values = values();

    public static boolean containsFormat(String statement) {
        ClickHouseFormat format = detectFormat(statement);
        return format != null;
    }

    public static ClickHouseFormat detectFormat(String statement) {
        if (statement != null && !statement.isEmpty()) {
            // TODO Proper parsing of comments etc.
            String s = statement.replaceAll("[;\\s]", "");
            for (ClickHouseFormat format : values) {
                if (s.endsWith(format.name())) {
                    return format;
                }
            }
        }
        return null;
    }

}
