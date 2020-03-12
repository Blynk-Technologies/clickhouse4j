package cc.blynk.clickhouse.http;

import cc.blynk.clickhouse.ClickHouseExternalData;
import cc.blynk.clickhouse.except.ClickHouseException;
import cc.blynk.clickhouse.util.guava.StreamUtils;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

public interface HttpConnector {

    InputStream post(String sql, URI uri) throws ClickHouseException;

    void post(String sql, InputStream from, URI uri) throws ClickHouseException;

    InputStream post(List<ClickHouseExternalData> externalData, URI uri) throws ClickHouseException;

    void post(String sql, List<byte[]> data, URI uri) throws ClickHouseException;

    void close();

    static byte[] getSqlBytes(String sql) {
        if (!sql.endsWith("\n")) {
            sql += "\n";
        }
        return sql.getBytes(UTF_8);
    }

    static byte[] buildMultipartData(List<ClickHouseExternalData> externalData, String boundaryString)
            throws IOException {
        try (ByteArrayOutputStream requestBodyStream = new ByteArrayOutputStream();
             BufferedWriter httpRequestBodyWriter = new BufferedWriter(new OutputStreamWriter(requestBodyStream))) {
            for (ClickHouseExternalData data : externalData) {
                httpRequestBodyWriter.write("--" + boundaryString + "\r\n");
                httpRequestBodyWriter.write("Content-Disposition: form-data;"
                        + " name=\"" + data.getName() + "\";"
                        + " filename=\"" + data.getName() + "\"" + "\r\n");
                httpRequestBodyWriter.write("Content-Type: application/octet-stream" + "\r\n");
                httpRequestBodyWriter.write("Content-Transfer-Encoding: binary" + "\r\n" + "\r\n");
                httpRequestBodyWriter.flush();

                StreamUtils.copy(data.getContent(), requestBodyStream);

                requestBodyStream.flush();
            }

            httpRequestBodyWriter.write("\r\n" + "--" + boundaryString + "--" + "\r\n");
            httpRequestBodyWriter.flush();

            return requestBodyStream.toByteArray();
        }
    }

}
