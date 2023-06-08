package com.linkedin.venice.client.store;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.client.store.transport.HttpTransportClient;
import com.linkedin.venice.client.store.transport.HttpsTransportClient;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.security.SSLFactory;
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
}
