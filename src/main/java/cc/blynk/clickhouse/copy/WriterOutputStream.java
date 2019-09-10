package cc.blynk.clickhouse.copy;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;

final class WriterOutputStream extends OutputStream {

    private final Writer writer;

    WriterOutputStream(Writer writer) {
        this.writer = writer;
    }

    @Override
    public void write(int b) throws IOException {
        writer.write(b);
    }
}
