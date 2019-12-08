package cc.blynk.clickhouse.http;

import cc.blynk.clickhouse.except.ClickHouseException;
import cc.blynk.clickhouse.except.ClickHouseExceptionSpecifier;
import cc.blynk.clickhouse.settings.ClickHouseProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HttpsURLConnection;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;

final class DefaultHttpConnector extends BaseHttpConnector {

    private static final Logger log = LoggerFactory.getLogger(DefaultHttpConnector.class);

    private HttpURLConnection httpURLConnection;

    DefaultHttpConnector(ClickHouseProperties properties) {
        super(properties);
    }

    @Override
    public HttpURLConnection buildConnection(URI uri) throws ClickHouseException {
        try {
            HttpURLConnection prevConnection = this.httpURLConnection;
            if (prevConnection != null) {
                prevConnection.disconnect();
            }

            URL url = uri.toURL();
            HttpURLConnection newConnection = (HttpURLConnection) url.openConnection();
            newConnection.setInstanceFollowRedirects(true);
            newConnection.setRequestMethod("POST");
            newConnection.setDoInput(true);
            newConnection.setDoOutput(true);

            newConnection.setConnectTimeout(properties.getConnectionTimeout());

            newConnection.addRequestProperty("Authorization", properties.getHttpAuthorization());
            newConnection.addRequestProperty("Content-Type", "text/plain; charset=UTF-8");

            if (newConnection instanceof HttpsURLConnection) {
                configureHttps((HttpsURLConnection) newConnection);
            }

            this.httpURLConnection = newConnection;
            return newConnection;
        } catch (IOException e) {
            log.error("Can't build connection. {}", e.getMessage());
            throw ClickHouseExceptionSpecifier.specify(e, properties.getHost(), properties.getPort());
        }
    }

    @Override
    public void close() {
        if (this.httpURLConnection != null) {
            this.httpURLConnection.disconnect();
        }
    }
}
