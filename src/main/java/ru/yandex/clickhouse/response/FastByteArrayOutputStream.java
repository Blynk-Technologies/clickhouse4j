package ru.yandex.clickhouse.response;


import java.io.OutputStream;
import java.util.Arrays;

/**
 * Not synchronized quick version of {@link java.io.ByteArrayOutputStream}
 */

public final class FastByteArrayOutputStream extends OutputStream {

    /**
     * The buffer where data is stored.
     */
    private byte[] buf;

    /**
     * The number of valid bytes in the buffer.
     */
    private int count;

    /**
     * Creates a new byte array output stream. The buffer capacity is
     * initially 32 bytes, though its size increases if necessary.
     */
    public FastByteArrayOutputStream() {
	    this(1024);
    }

    /**
     * Creates a new byte array output stream, with a buffer capacity of
     * the specified size, in bytes.
     *
     * @param   size   the initial size.
     * @exception  IllegalArgumentException if size is negative.
     */
    private FastByteArrayOutputStream(int size) {
        super();
        if (size < 0) {
            throw new IllegalArgumentException("Negative initial size: " + size);
        }
        buf = new byte[size];
    }

    private int ensureCapacity(int datalen) {
        int newcount = count + datalen;
        if (newcount > buf.length) {
                buf = Arrays.copyOf(buf, Math.max(buf.length << 1, newcount));
        }
        return newcount;
    }


    /**
     * Writes the specified byte to this byte array output stream.
     *
     * @param   b   the byte to be written.
     */
    @Override
    public void write(int b) {
        int newcount = ensureCapacity(1);
        buf[count] = (byte)b;
        count = newcount;
    }

    /**
     * Writes <code>len</code> bytes from the specified byte array
     * starting at offset <code>off</code> to this byte array output stream.
     *
     * @param   b     the data.
     * @param   off   the start offset in the data.
     * @param   len   the number of bytes to write.
     */
    @Override
    public void write(byte[] b, int off, int len) {
        if (off < 0 || off > b.length || len < 0 || off + len > b.length || off + len < 0) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }
        int newcount = ensureCapacity(len);
        System.arraycopy(b, off, buf, count, len);
        count = newcount;
    }

    /**
     * Returns the current size of the buffer.
     *
     * @return  the value of the <code>count</code> field, which is the number
     *          of valid bytes in this output stream.
     */
    public int size() {
	    return count;
    }


    /**
     * Closing a FastByteArrayOutputStream has no effect. The methods in
     * this class can be called after the stream has been closed without
     */
    @Override
    public void close() {
    }

    /**
     * Creates InputStream using the same data that is written into this stream with no copying in memory
     * @return a input stream contained all bytes recorded in a current stream
     */
    public FastByteArrayInputStream convertToInputStream() {
        return new FastByteArrayInputStream(buf, count); 
    }

}

