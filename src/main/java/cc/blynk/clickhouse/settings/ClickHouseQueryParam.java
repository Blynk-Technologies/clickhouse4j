package cc.blynk.clickhouse.settings;


import java.sql.DriverPropertyInfo;
import java.util.Properties;

public enum ClickHouseQueryParam {

    ADD_HTTP_CORS_HEADER("add_http_cors_header", false, Boolean.class),

    AGGREGATION_MEMORY_EFFICIENT_MERGE_THREADS("aggregation_memory_efficient_merge_threads", null, Long.class),

    BACKGROUND_POOL_SIZE("background_pool_size", null, Long.class),

    AUTHORIZATION("authorization", null, String.class),

    COMPILE("compile", false, Boolean.class),

    COMPRESS("compress", true, Boolean.class),

    CONNECT_TIMEOUT("connect_timeout", null, Integer.class),

    CONNECT_TIMEOUT_WITH_FAILOVER_MS("connect_timeout_with_failover_ms", null, Integer.class),

    CONNECTIONS_WITH_FAILOVER_MAX_TRIES("connections_with_failover_max_tries", null, Long.class),

    COUNT_DISTINCT_IMPLEMENTATION("count_distinct_implementation", null, String.class),

    DATABASE("database", null, String.class),

    DECOMPRESS("decompress", false, Boolean.class),

    DISTRIBUTED_AGGREGATION_MEMORY_EFFICIENT("distributed_aggregation_memory_efficient", false, Boolean.class),

    DISTRIBUTED_CONNECTIONS_POOL_SIZE("distributed_connections_pool_size", null, Long.class),

    DISTRIBUTED_DIRECTORY_MONITOR_SLEEP_TIME_MS("distributed_directory_monitor_sleep_time_ms", null, Long.class),

    DISTRIBUTED_GROUP_BY_NO_MERGE("distributed_group_by_no_merge", false, Boolean.class),

    DISTRIBUTED_PRODUCT_MODE("distributed_product_mode", null, String.class),

    ENABLE_HTTP_COMPRESSION("enable_http_compression", false, Boolean.class),

    EXTREMES("extremes", false, Boolean.class),

    FORCE_INDEX_BY_DATE("force_index_by_date", false, Boolean.class),

    FORCE_PRIMARY_KEY("force_primary_key", false, Boolean.class),

    GLOBAL_SUBQUERIES_METHOD("global_subqueries_method", null, String.class),

    GROUP_BY_TWO_LEVEL_THRESHOLD("group_by_two_level_threshold", null, Long.class),

    GROUP_BY_TWO_LEVEL_THRESHOLD_BYTES("group_by_two_level_threshold_bytes", null, Long.class),

    HTTP_NATIVE_COMPRESSION_DISABLE_CHECKSUMMING_ON_DECOMPRESS(
            "http_native_compression_disable_checksumming_on_decompress",
            null,
            Boolean.class
    ),

    HTTP_ZLIB_COMPRESSION_LEVEL("http_zlib_compression_level", null, Long.class),

    INPUT_FORMAT_SKIP_UNKNOWN_FIELDS("input_format_skip_unknown_fields", false, Boolean.class),

    INPUT_FORMAT_VALUES_INTERPRET_EXPRESSIONS("input_format_values_interpret_expressions", true, Boolean.class),

    INSERT_DEDUPLICATE("insert_deduplicate", null, Boolean.class),

    INSERT_DISTRIBUTED_SYNC("insert_distributed_sync", null, Boolean.class),

    INSERT_QUORUM("insert_quorum", null, Long.class),

    INSERT_QUORUM_TIMEOUT("insert_quorum_timeout", null, Long.class),

    INTERACTIVE_DELAY("interactive_delay", null, Long.class),

    LOAD_BALANCING("load_balancing", null, String.class),

    LOG_QUERIES("log_queries", false, Boolean.class),

    LOG_QUERIES_CUT_TO_LENGTH("log_queries_cut_to_length", null, Long.class),

    MARK_CACHE_MIN_LIFETIME("mark_cache_min_lifetime", null, Long.class),
    /**
     * https://clickhouse.yandex/reference_en.html#max_block_size
     */
    MAX_BLOCK_SIZE("max_block_size", null, Integer.class),

    MAX_BYTES_BEFORE_EXTERNAL_GROUP_BY("max_bytes_before_external_group_by", null, Long.class),

    MAX_BYTES_BEFORE_EXTERNAL_SORT("max_bytes_before_external_sort", null, Long.class),

    MAX_COMPRESS_BLOCK_SIZE("max_compress_block_size", null, Long.class),

    MAX_CONCURRENT_QUERIES_FOR_USER("max_concurrent_queries_for_user", null, Long.class),

    MAX_DISTRIBUTED_CONNECTIONS("max_distributed_connections", null, Long.class),

    MAX_DISTRIBUTED_PROCESSING_THREADS("max_distributed_processing_threads", null, Long.class),
    /**
     * https://clickhouse.yandex/reference_en.html#max_execution_time
     */
    MAX_EXECUTION_TIME("max_execution_time", null, Integer.class),

    MAX_INSERT_BLOCK_SIZE("max_insert_block_size", null, Long.class),

    /**
     * @see <a href="https://clickhouse.yandex/reference_en.html#max_memory_usage">max_memory_usage</a>
     */
    MAX_MEMORY_USAGE("max_memory_usage", null, Long.class),

    /**
     * @see <a href="https://clickhouse.yandex/docs/en/operations/settings/query_complexity/#max-memory-usage-for-user">
     *     max_memory_usage_for_user</a>
     */
    MAX_MEMORY_USAGE_FOR_USER("max_memory_usage_for_user", null, Long.class
    ),

    /**
     * @see "https://clickhouse.yandex/docs/en/operations/settings/query_complexity/#max-memory-usage-for-all-queries"
     */
    MAX_MEMORY_USAGE_FOR_ALL_QUERIES("max_memory_usage_for_all_queries", null, Long.class),

    //dbms/include/DB/Interpreters/Settings.h
    MAX_PARALLEL_REPLICAS("max_parallel_replicas", null, Integer.class),

    MAX_PARTITIONS_PER_INSERT_BLOCK("max_partitions_per_insert_block", null, Integer.class),

    MAX_READ_BUFFER_SIZE("max_read_buffer_size", null, Long.class),

    MAX_RESULT_ROWS("max_result_rows", null, Integer.class),
    /**
     * https://clickhouse.yandex/reference_en.html#max_rows_to_group_by
     */
    MAX_ROWS_TO_GROUP_BY("max_rows_to_group_by", null, Integer.class),

    MAX_STREAMS_TO_MAX_THREADS_RATIO("max_streams_to_max_threads_ratio", null, Double.class),
    /**
     * https://clickhouse.yandex/reference_en.html#max_threads
     */
    MAX_THREADS("max_threads", null, Integer.class),

    MAX_QUERY_SIZE("max_query_size", null, Long.class),

    MAX_AST_ELEMENTS("max_ast_elements", null, Long.class),

    MEMORY_TRACKER_FAULT_PROBABILITY("memory_tracker_fault_probability", null, Double.class),

    MERGE_TREE_COARSE_INDEX_GRANULARITY("merge_tree_coarse_index_granularity", null, Long.class),

    MERGE_TREE_MAX_ROWS_TO_USE_CACHE("merge_tree_max_rows_to_use_cache", null, Long.class),

    MERGE_TREE_MIN_ROWS_FOR_CONCURRENT_READ("merge_tree_min_rows_for_concurrent_read", null, Long.class),

    MERGE_TREE_MIN_ROWS_FOR_SEEK("merge_tree_min_rows_for_seek", null, Long.class),

    MERGE_TREE_UNIFORM_READ_DISTRIBUTION("merge_tree_uniform_read_distribution", true, Boolean.class),

    MIN_BYTES_TO_USE_DIRECT_IO("min_bytes_to_use_direct_io", null, Long.class),

    MIN_COMPRESS_BLOCK_SIZE("min_compress_block_size", null, Long.class),

    MIN_COUNT_TO_COMPILE("min_count_to_compile", null, Long.class),

    MIN_INSERT_BLOCK_SIZE_BYTES("min_insert_block_size_bytes", null, Long.class),

    MIN_INSERT_BLOCK_SIZE_ROWS("min_insert_block_size_rows", null, Long.class),

    NETWORK_COMPRESSION_METHOD("network_compression_method", null, String.class),

    OPTIMIZE_MIN_EQUALITY_DISJUNCTION_CHAIN_LENGTH("optimize_min_equality_disjunction_chain_length", null, Long.class),

    OPTIMIZE_MOVE_TO_PREWHERE("optimize_move_to_prewhere", true, Boolean.class),

    OUTPUT_FORMAT_JSON_QUOTE_64BIT_INTEGERS("output_format_json_quote_64bit_integers", true, Boolean.class),

    OUTPUT_FORMAT_PRETTY_MAX_ROWS("output_format_pretty_max_rows", null, Long.class),

    OUTPUT_FORMAT_WRITE_STATISTICS("output_format_write_statistics", true, Boolean.class),

    PARALLEL_REPLICAS_COUNT("parallel_replicas_count", null, Long.class),

    PARALLEL_REPLICA_OFFSET("parallel_replica_offset", null, Long.class),

    PASSWORD("password", null, String.class),

    POLL_INTERVAL("poll_interval", null, Long.class),

    PRIORITY("priority", null, Integer.class),
    /**
     * https://clickhouse.yandex/reference_en.html#Settings profiles
     */
    PROFILE("profile", null, String.class),

    RECEIVE_TIMEOUT("receive_timeout", null, Integer.class),

    READ_BACKOFF_MAX_THROUGHPUT("read_backoff_max_throughput", null, Long.class),

    READ_BACKOFF_MIN_EVENTS("read_backoff_min_events", null, Long.class),

    READ_BACKOFF_MIN_INTERVAL_BETWEEN_EVENTS_MS("read_backoff_min_interval_between_events_ms", null, Long.class),

    READ_BACKOFF_MIN_LATENCY_MS("read_backoff_min_latency_ms", null, Long.class),

    REPLACE_RUNNING_QUERY("replace_running_query", false, Boolean.class),

    REPLICATION_ALTER_COLUMNS_TIMEOUT("replication_alter_columns_timeout", null, Long.class),

    REPLICATION_ALTER_PARTITIONS_SYNC("replication_alter_partitions_sync", null, Long.class),

    RESHARDING_BARRIER_TIMEOUT("resharding_barrier_timeout", null, Long.class),

    RESULT_OVERFLOW_MODE("result_overflow_mode", null, String.class),

    SELECT_SEQUENTIAL_CONSISTENCY("select_sequential_consistency", null, Long.class),

    SEND_TIMEOUT("send_timeout", null, Integer.class),

    SESSION_CHECK("session_check", false, Boolean.class),

    SESSION_ID("session_id", null, String.class),

    SESSION_TIMEOUT("session_timeout", null, Long.class),

    SKIP_UNAVAILABLE_SHARDS("skip_unavailable_shards", false, Boolean.class),

    STRICT_INSERT_DEFAULTS("strict_insert_defaults", false, Boolean.class),

    TABLE_FUNCTION_REMOTE_MAX_ADDRESSES("table_function_remote_max_addresses", null, Long.class),

    TOTALS_AUTO_THRESHOLD("totals_auto_threshold", null, Double.class),
    /**
     * https://clickhouse.yandex/reference_en.html#WITH TOTALS modifier
     */
    TOTALS_MODE("totals_mode", null, String.class),

    ENABLE_QUERY_ID("enable_query_id", false, Boolean.class),
    QUERY_ID("query_id", null, String.class),

    QUEUE_MAX_WAIT_MS("queue_max_wait_ms", null, Integer.class),

    QUOTA_KEY("quota_key", null, String.class),

    use_client_time_zone("use_client_time_zone", false, Boolean.class),


    USE_UNCOMPRESSED_CACHE("use_uncompressed_cache", true, Boolean.class),

    USER("user", null, String.class),

    PREFERRED_BLOCK_SIZE_BYTES("preferred_block_size_bytes", null, Long.class),

    ENABLE_OPTIMIZE_PREDICATE_EXPRESSION("enable_optimize_predicate_expression", null, Boolean.class);

    private final String key;
    private final Object defaultValue;
    private final Class<?> clazz;

    <T> ClickHouseQueryParam(String key, T defaultValue, Class<T> clazz) {
        this.key = key;
        this.defaultValue = defaultValue;
        this.clazz = clazz;
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

    @Override
    public String toString() {
        return name().toLowerCase();
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
