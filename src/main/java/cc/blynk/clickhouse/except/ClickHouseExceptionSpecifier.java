package cc.blynk.clickhouse.except;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;


/**
 * Specify ClickHouse exception to ClickHouseException and fill it with a vendor code.
 */

public final class ClickHouseExceptionSpecifier {

    private static final Logger log = LoggerFactory.getLogger(ClickHouseExceptionSpecifier.class);

    private ClickHouseExceptionSpecifier() {
    }

    public static ClickHouseException specify(Throwable cause, String host, int port) {
        return specify(cause != null ? cause.getMessage() : null, cause, host, port);
    }

    public static ClickHouseException specify(Throwable cause) {
        return specify(cause != null ? cause.getMessage() : null, cause, "unknown", -1);
    }

    public static ClickHouseException specify(String clickHouseMessage, String host, int port) {
        return specify(clickHouseMessage, null, host, port);
    }

    public static ClickHouseException specify(String clickHouseMessage) {
        return specify(clickHouseMessage, "unknown", -1);
    }

    /**
     * Here we expect the ClickHouse error message to be of the following format:
     * "Code: 10, e.displayText() = DB::Exception: ...".
     */
    private static ClickHouseException specify(String clickHouseMessage, Throwable cause, String host, int port) {
        if ((clickHouseMessage == null || clickHouseMessage.isEmpty()) && cause != null) {
            return getException(cause, host, port);
        }

        try {
            int code;
            if (clickHouseMessage.startsWith("Poco::Exception. Code: 1000, ")) {
                code = ClickHouseErrorCode.POCO_EXCEPTION;
            } else {
                // Code: 175, e.displayText() = DB::Exception:
                code = getErrorCode(clickHouseMessage);
            }
            // ошибку в изначальном виде все-таки укажем
            Throwable messageHolder = cause != null ? cause : new Throwable(clickHouseMessage);
            if (code == -1) {
                return getException(messageHolder, host, port);
            }

            return new ClickHouseException(code, messageHolder, host, port);
        } catch (Exception e) {
            log.error("Unsupported ClickHouse error format, please fix ClickHouseExceptionSpecifier,"
                              + " message: {}, error: {}", clickHouseMessage, e.getMessage());
            return new ClickHouseUnknownException(clickHouseMessage, cause, host, port);
        }
    }

    private static int getErrorCode(String errorMessage) {
        int startIndex = errorMessage.indexOf(' ');
        int endIndex = startIndex == -1 ? -1 : errorMessage.indexOf(',', startIndex);

        if (startIndex == -1 || endIndex == -1) {
            return -1;
        }

        try {
            return Integer.parseInt(errorMessage.substring(startIndex + 1, endIndex));
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private static ClickHouseException getException(Throwable cause, String host, int port) {
        if (cause instanceof SocketTimeoutException) {
            // if we've got SocketTimeoutException, we'll say that the query is not good.
            // This is not the same as SOCKET_TIMEOUT of clickhouse
            // but it actually could be a failing ClickHouse
            return new ClickHouseException(ClickHouseErrorCode.TIMEOUT_EXCEEDED, cause, host, port);
        } else if (cause instanceof InterruptedIOException || cause instanceof ConnectException) {
        // couldn't connect to ClickHouse during connectTimeout
            return new ClickHouseException(ClickHouseErrorCode.NETWORK_ERROR, cause, host, port);
        } else {
            return new ClickHouseUnknownException(cause, host, port);
        }
    }

}
