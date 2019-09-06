package cc.blynk.clickhouse.copy;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

class ReaderInputStream extends InputStream {

    private final Reader reader;

    ReaderInputStream(Reader reader) {
        this.reader = reader;
    }

    @Override
    public int read() throws IOException {
        return reader.read();
    }
}
