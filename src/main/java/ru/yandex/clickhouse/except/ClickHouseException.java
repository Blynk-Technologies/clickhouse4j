package ru.yandex.clickhouse.except;

import java.sql.SQLException;

public class ClickHouseException extends SQLException {

    public ClickHouseException(int code, Throwable cause, String host, int port) {
        super("ClickHouse exception, code: " + code + ", host: " + host + ", port: " + port + "; "
                + (cause == null ? "" : cause.getMessage()), null, code, cause);
    }

    private ClickHouseException(int code, String message, Throwable cause, String host, int port) {
        super("ClickHouse exception, message: " + message + ", host: " + host + ", port: " + port + "; "
                      + (cause == null ? "" : cause.getMessage()), null, code, cause);
    }

    public ClickHouseException(ClickHouseErrorCode errorCode, Throwable cause, String host, int port) {
        this(errorCode.code, cause, host, port);
    }

    public ClickHouseException(ClickHouseErrorCode errorCode, String message, Throwable cause, String host, int port) {
        this(errorCode.code, message, cause, host, port);
    }
}
