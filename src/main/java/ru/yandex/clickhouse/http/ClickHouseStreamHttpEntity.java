package ru.yandex.clickhouse.http;

import org.apache.http.entity.AbstractHttpEntity;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.util.ClickHouseRowBinaryStream;
import ru.yandex.clickhouse.util.ClickHouseStreamCallback;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;
import java.util.TimeZone;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 */
class ClickHouseStreamHttpEntity extends AbstractHttpEntity {

    private final ClickHouseStreamCallback callback;
    private final TimeZone timeZone;
    private final ClickHouseProperties properties;

    ClickHouseStreamHttpEntity(ClickHouseStreamCallback callback,
                               TimeZone timeZone,
                               ClickHouseProperties properties) {
        Objects.requireNonNull(callback);
        this.timeZone = timeZone;
        this.callback = callback;
        this.properties = properties;
    }

    @Override
    public boolean isRepeatable() {
        return true;
    }

    @Override
    public long getContentLength() {
        return -1;
    }

    @Override
    public InputStream getContent() throws UnsupportedOperationException {
        return null;
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
        ClickHouseRowBinaryStream stream = new ClickHouseRowBinaryStream(out, timeZone, properties);
        callback.writeTo(stream);
    }

    @Override
    public boolean isStreaming() {
        return false;
    }
}
