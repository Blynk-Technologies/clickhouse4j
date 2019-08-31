package ru.yandex.clickhouse.response;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class ByteFragment {

    final byte[] buf;
    protected final int start;
    final int len;
    private static final ByteFragment EMPTY = new ByteFragment(new byte[0], 0, 0);

    public ByteFragment(byte[] buf, int start, int len) {
        this.buf = buf;
        this.start = start;
        this.len = len;
    }

    private final static byte[] reverse;

    static ByteFragment fromString(String str) {
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        return new ByteFragment(bytes, 0, bytes.length);
    }

    // "\0" =>  0
    // "\r" => 13
    // "\n" => 10
    // "\\" => 92
    // "\'" => 39
    // "\b" =>  8
    // "\f" => 12
    // "\t" =>  9
    //null
    // "\N" =>  0
    private static final byte[] convert = {
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,      // 0.. 9
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,      //10..19
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,      //20..29
            -1, -1, -1, -1, -1, -1, -1, -1, -1, 39,      //30..39
            -1, -1, -1, -1, -1, -1, -1, -1, 0, -1,      //40..49
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,      //50..59
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,      //60..69
            -1, -1, -1, -1, -1, -1, -1, -1, 0, -1,      //70..79
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,      //80..89
            -1, -1, 92, -1, -1, -1, -1, -1, 8, -1,      //90..99
            -1, -1, 12, -1, -1, -1, -1, -1, -1, -1,     //100..109
            10, -1, -1, -1, 13, -1, 9, -1, -1, -1,     //110..119
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,     //120..129
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    };

    public boolean isNull() {
        // \N
        return len == 2 && buf[start] == '\\' && buf[start + 1] == 'N';
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("ByteFragment{[");
        for (byte b1 : buf) {
            if (b1 == '\t') {
                b.append("<TAB>");
            } else {
                b.append((char) b1);
            }
        }
        b.append(']');
        b.append(", start=").append(start).append(", len=").append(len).append('}');
        return b.toString();
    }

    String asString() {
        return new String(buf, start, len, StandardCharsets.UTF_8);
    }

    static {
        reverse = new byte[convert.length];
        for (int i = 0; i < convert.length; i++) {
            reverse[i] = -1;
            byte c = convert[i];
            if (c != -1) {
                reverse[c] = (byte) i;
            }
        }
    }

    static void escape(byte[] bytes, OutputStream stream) throws IOException {
        for (byte b : bytes) {
            if (b < 0 || b >= reverse.length) {
                stream.write(b);
            } else {
                byte converted = reverse[b];
                if (converted != -1) {
                    stream.write(92);
                    stream.write(converted);
                } else {
                    stream.write(b);
                }
            }
        }
    }

    int getLen() {
        return len;
    }

    String asString(boolean unescape) {
        if (unescape) {
            if (isNull()) {
                return null;
            }
            return new String(unescape(), StandardCharsets.UTF_8);
        } else {
            return asString();
        }
    }
    // [0xb6][0xfe][0x7][0x1][0xd8][0xd6][0x94][0x80][0x5]\0   html.
    //  [-74,-2,7,1,-40,-42,-108,-128,5,0]   real value
    //  [-74,-2,7,1,-40,-42,-108,-128,5,92,48]   parsed value

    private int count(byte sep) {
        int res = 0;
        for (int i = start; i < start + len; i++) {
            if (buf[i] == sep) {
                res++;
            }
        }
        return res;
    }

    ByteArrayInputStream asStream() {
        return new ByteArrayInputStream(buf, start, len);
    }

    ByteFragment[] split(byte sep) {
        StreamSplitter ss = new StreamSplitter(this, sep);
        int c = count(sep) + 1;
        ByteFragment[] res = new ByteFragment[c];
        try {
            int i = 0;
            ByteFragment next;
            while ((next = ss.next()) != null) {
                res[i++] = next;
            }
        } catch (IOException ignore) {
        }
        if (res[c - 1] == null) {
            res[c - 1] = ByteFragment.EMPTY;
        }
        return res;
    }

    byte[] unescape() {
        int resLen = 0;

        boolean prevSlash = false;
        for (int i = start; i < start + len; i++) {
            if (prevSlash) {
                resLen++;
                prevSlash = false;
            } else {
                if (buf[i] == 92) { // slash character
                    prevSlash = true;
                } else {
                    resLen++;
                }
            }
        }

        if (resLen == len) {
            return getBytesCopy();
        }
        byte[] res = new byte[resLen];
        int index = 0;
        prevSlash = false;
        for (int i = start; i < start + len; i++) {
            if (prevSlash) {
                prevSlash = false;
                res[index++] = convert[buf[i]];

            } else {
                if (buf[i] == 92) { // slash character
                    prevSlash = true;
                } else {
                    res[index++] = buf[i];
                }
            }
        }
        return res;
    }

    private byte[] getBytesCopy() {
        byte[] bytes = new byte[len];
        System.arraycopy(buf, start, bytes, 0, len);
        return bytes;
    }

    public int length() {
        return len;
    }

    int charAt(int i) {
        return buf[start + i];
    }


    ByteFragment subseq(int start, int len) {
        if (start < 0 || start + len > this.len) {
            throw new IllegalArgumentException("arg start,len="
                                                       + (start + "," + len)
                                                       + " while this start,len="
                                                       + (this.start + "," + this.len));
        }
        return new ByteFragment(buf, this.start + start, len);
    }
}
