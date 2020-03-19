package cc.blynk.clickhouse.http;

import cc.blynk.clickhouse.except.ClickHouseExceptionSpecifier;
import cc.blynk.clickhouse.settings.ClickHouseProperties;
import io.netty.channel.epoll.Epoll;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.OpenSsl;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;

public final class AsyncConnectorFactory extends HttpConnectorFactory {

    @Override
    public HttpConnector create(ClickHouseProperties properties) {
        return new AsyncHttpConnector(properties, initClientConfig(properties));
    }

    private static DefaultAsyncHttpClientConfig initClientConfig(ClickHouseProperties properties) {
        DefaultAsyncHttpClientConfig.Builder clientConfigBuilder = new DefaultAsyncHttpClientConfig.Builder()
                .setUserAgent(null)
                .setKeepAlive(true)
                .setConnectTimeout(properties.getConnectionTimeout())
                .setMaxConnections(properties.getMaxConnections())
                .setUseNativeTransport(Epoll.isAvailable())
                .setUseOpenSsl(OpenSsl.isAvailable());
        if (properties.getSsl()) {
            try {
                JdkSslContext sslContext = new JdkSslContext(getSSLContext(properties), true, ClientAuth.REQUIRE);
                clientConfigBuilder.setSslContext(sslContext);
            } catch (Exception e) {
                 throw new RuntimeException(
                         ClickHouseExceptionSpecifier.specify(e, properties.getHost(), properties.getPort())
                 );
            }
        }
        return clientConfigBuilder.build();
    }

}
