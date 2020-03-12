package cc.blynk.clickhouse.http;

import cc.blynk.clickhouse.except.ClickHouseExceptionSpecifier;
import cc.blynk.clickhouse.settings.ClickHouseProperties;
import io.netty.channel.epoll.Epoll;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.JdkSslContext;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;

public class AsyncConnectorFactory extends HttpConnectorFactory {

    private final DefaultAsyncHttpClientConfig asyncHttpClientConfig;

    public AsyncConnectorFactory(DefaultAsyncHttpClientConfig asyncHttpClientConfig, ClickHouseProperties properties) {
        super(properties);
        this.asyncHttpClientConfig = asyncHttpClientConfig;
    }

    public AsyncConnectorFactory(int maxConnections, ClickHouseProperties properties) {
        this(initClientConfig(maxConnections, properties), properties);
    }

    @Override
    public HttpConnector create() {
        return new AsyncHttpConnector(properties, asyncHttpClientConfig);
    }

    private static DefaultAsyncHttpClientConfig initClientConfig(int maxConnections, ClickHouseProperties properties) {
        DefaultAsyncHttpClientConfig.Builder clientConfigBuilder = new DefaultAsyncHttpClientConfig.Builder()
                .setUserAgent(null)
                .setKeepAlive(true)
                .setConnectTimeout(properties.getConnectionTimeout())
                .setMaxConnections(maxConnections)
                .setUseNativeTransport(Epoll.isAvailable());
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
