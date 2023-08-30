package com.linkedin.venice.client.store;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.client.store.transport.HttpTransportClient;
import com.linkedin.venice.client.store.transport.HttpsTransportClient;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.security.SSLFactory;
import java.util.function.Function;
import javax.net.ssl.SSLContext;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class ClientFactoryTest {
  @DataProvider(name = "protocol")
  public static Object[][] protocol() {
    return new Object[][] { { "http" }, { "https" } };
  }

  @Test(dataProvider = "protocol")
  public void testHttpConfig(String protocol) throws Exception {
    ClientConfig config = ClientConfig.defaultGenericClientConfig("store");
    config.setMaxConnectionsPerRoute(7);
    config.setMaxConnectionsTotal(8);
    config.setVeniceURL(protocol + "://localhost:8080");
    config.setSslFactory(mock(SSLFactory.class));

    TransportClient transport = ClientFactory.getTransportClient(config);
    HttpTransportClient httpTransport = (HttpTransportClient) transport;
    if (protocol.equals("https")) {
      Assert.assertTrue(httpTransport instanceof HttpsTransportClient);
    }
    Assert.assertEquals(httpTransport.getMaxConnectionsPerRoute(), 7);
    Assert.assertEquals(httpTransport.getMaxConnectionsTotal(), 8);

    transport.close();
  }

  @DataProvider(name = "httpClient5Http2Enabled")
  public static Object[][] httpClient5Http2Enabled() {
    return new Object[][] { { true }, { false } };
  }

  @Test(dataProvider = "httpClient5Http2Enabled")
  public void testRequireHTTP2(boolean httpClient5Http2Enabled) throws Exception {
    ClientConfig config = ClientConfig.defaultGenericClientConfig("store");
    config.setMaxConnectionsPerRoute(7);
    config.setMaxConnectionsTotal(8);
    config.setHttpClient5Http2Enabled(httpClient5Http2Enabled);
    config.setVeniceURL("https://localhost:8080");
    SSLFactory sslFactory = mock(SSLFactory.class);
    SSLContext sslContext = mock(SSLContext.class);
    when(sslFactory.getSSLContext()).thenReturn(sslContext);
    config.setSslFactory(sslFactory);

    TransportClient transport = ClientFactory.getTransportClient(config);
    HttpsTransportClient httpTransport = (HttpsTransportClient) transport;
    Assert.assertEquals(httpTransport.isRequireHTTP2(), httpClient5Http2Enabled);

    transport.close();
  }

  @Test
  public void testMocking() {
    String storeName = "store";
    ClientConfig storeClientConfig =
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL("http://localhost:1234");
    TransportClient transportClient = mock(TransportClient.class);

    Function<ClientConfig, TransportClient> clientConfigTransportClientFunction = (clientConfig) -> {
      if (clientConfig.getStoreName().equals(storeName)) {
        return transportClient;
      } else {
        // Create TransportClient the regular way
        return null;
      }
    };

    // Setting a TransportClient provider is not supported when not in unit test mode
    Assert.assertThrows(
        VeniceUnsupportedOperationException.class,
        () -> ClientFactory.setTransportClientProvider(clientConfigTransportClientFunction));

    ClientFactory.setUnitTestMode();

    ClientFactory.setTransportClientProvider(clientConfigTransportClientFunction);

    TransportClient actualTransportClient = ClientFactory.getTransportClient(storeClientConfig);
    Assert.assertSame(actualTransportClient, transportClient);
    Assert.assertFalse(actualTransportClient instanceof HttpTransportClient);

    ClientConfig storeClientConfig2 =
        ClientConfig.defaultGenericClientConfig("storeName2").setVeniceURL("http://localhost:1234");
    TransportClient actualTransportClient2 = ClientFactory.getTransportClient(storeClientConfig2);
    Assert.assertTrue(actualTransportClient2 instanceof HttpTransportClient);

    ClientFactory.resetUnitTestMode();

    TransportClient actualTransportClient3 = ClientFactory.getTransportClient(storeClientConfig);
    Assert.assertNotSame(actualTransportClient3, transportClient);

    Assert.assertTrue(actualTransportClient3 instanceof HttpTransportClient);
  }
}
