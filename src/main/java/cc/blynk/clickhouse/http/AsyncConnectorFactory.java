package cc.blynk.clickhouse.http;

import cc.blynk.clickhouse.except.ClickHouseExceptionSpecifier;
import cc.blynk.clickhouse.settings.ClickHouseProperties;
import io.netty.channel.epoll.Epoll;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.OpenSsl;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;

final class AsyncConnectorFactory extends HttpConnectorFactory {

    private final DefaultAsyncHttpClientConfig asyncHttpClientConfig;

    AsyncConnectorFactory(DefaultAsyncHttpClientConfig asyncHttpClientConfig, ClickHouseProperties properties) {
        super(properties);
        this.asyncHttpClientConfig = asyncHttpClientConfig;
    }

    AsyncConnectorFactory(ClickHouseProperties properties) {
        this(initClientConfig(properties), properties);
    }

    @Override
    public HttpConnector create() {
        return new AsyncHttpConnector(properties, asyncHttpClientConfig);
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
