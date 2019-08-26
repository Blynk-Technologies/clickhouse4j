package ru.yandex.clickhouse.http.apache;

import org.apache.http.entity.AbstractHttpEntity;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

class BatchHttpEntity extends AbstractHttpEntity {
    private final List<byte[]> rows;

    BatchHttpEntity(List<byte[]> rows) {
        this.rows = rows;
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
    public InputStream getContent() throws IllegalStateException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeTo(OutputStream outputStream) throws IOException {
        for (byte[] row : rows) {
            outputStream.write(row);
        }
    }

    @Override
    public boolean isStreaming() {
        return false;
    }
}
