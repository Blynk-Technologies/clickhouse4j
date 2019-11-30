package cc.blynk.clickhouse.settings;


import java.sql.DriverPropertyInfo;
import java.util.Properties;

public enum ClickHouseConnectionSettings {

    ASYNC("async", false),
    BUFFER_SIZE("buffer_size", 65536),
    APACHE_BUFFER_SIZE("apache_buffer_size", 65536),
    SOCKET_TIMEOUT("socket_timeout", 30000),
    CONNECTION_TIMEOUT("connection_timeout", 10 * 1000),
    SSL("ssl", false),
    SSL_ROOT_CERTIFICATE("sslrootcert", ""),
    SSL_MODE("sslmode", "strict"),
    USE_PATH_AS_DB("use_path_as_db", true),
    PATH("path", ""),
    CHECK_FOR_REDIRECTS("check_for_redirects", false),
    MAX_REDIRECTS("max_redirects", 5),
    DATA_TRANSFER_TIMEOUT("dataTransferTimeout", 10000),
    KEEP_ALIVE_TIMEOUT("keepAliveTimeout", 30 * 1000),
    TIME_TO_LIVE_MILLIS("timeToLiveMillis", 60 * 1000),
    DEFAULT_MAX_PER_ROUTE("defaultMaxPerRoute", 500),
    MAX_TOTAL("maxTotal", 10000),
    USE_OBJECTS_IN_ARRAYS("use_objects_in_arrays", false),
    MAX_COMPRESS_BUFFER_SIZE("maxCompressBufferSize", 1024 * 1024),
    USE_SERVER_TIME_ZONE("use_server_time_zone", true),
    USE_TIME_ZONE("use_time_zone", ""),
    USE_SERVER_TIME_ZONE_FOR_DATES("use_server_time_zone_for_dates", false);

    private final String key;
    private final Object defaultValue;
    private final Class<?> clazz;

    ClickHouseConnectionSettings(String key, Object defaultValue) {
        this.key = key;
        this.defaultValue = defaultValue;
        this.clazz = defaultValue.getClass();
    }

    public String getKey() {
        return key;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    public Class<?> getClazz() {
        return clazz;
    }

    public DriverPropertyInfo createDriverPropertyInfo(Properties properties) {
        DriverPropertyInfo propertyInfo = new DriverPropertyInfo(key, driverPropertyValue(properties));
        propertyInfo.required = false;
        propertyInfo.choices = driverPropertyInfoChoices();
        return propertyInfo;
    }

    private String[] driverPropertyInfoChoices() {
        return clazz == Boolean.class || clazz == Boolean.TYPE ? new String[]{"true", "false"} : null;
    }

    private String driverPropertyValue(Properties properties) {
        String value = properties.getProperty(key);
        if (value == null) {
            value = defaultValue == null ? null : defaultValue.toString();
        }
        return value;
    }
}
