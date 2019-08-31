package ru.yandex.clickhouse.except;

final class ClickHouseUnknownException extends ClickHouseException {

    ClickHouseUnknownException(Throwable cause, String host, int port) {
        super(ClickHouseErrorCode.UNKNOWN_EXCEPTION, cause, host, port);
    }

    ClickHouseUnknownException(String message, Throwable cause, String host, int port) {
        super(ClickHouseErrorCode.UNKNOWN_EXCEPTION, message, cause, host, port);
    }

}
