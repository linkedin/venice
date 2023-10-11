package com.linkedin.venice.router;

import static com.linkedin.venice.HttpConstants.HTTP_GET;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.router.httpclient.HttpClient5StorageNodeClient;
import com.linkedin.venice.router.httpclient.PortableHttpResponse;
import com.linkedin.venice.router.httpclient.StorageNodeClientType;
import com.linkedin.venice.router.httpclient.VeniceMetaDataRequest;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.Utils;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.Test;


public class TestReadForHttpClient5 extends TestRead {
  private final Logger LOGGER = LogManager.getLogger(this.getClass());

  @Override
  protected StorageNodeClientType getStorageNodeClientType() {
    return StorageNodeClientType.HTTP_CLIENT_5_CLIENT;
  }

  @Override
  protected boolean isRouterHttp2ClientEnabled() {
    return true;
  }

  @Override
  protected boolean isTestEnabled() {
    boolean testEnabled = Utils.getJavaMajorVersion() >= 11;
    if (!testEnabled) {
      LOGGER.info(
          "All the tests are disabled since StorageNodeClientType: {} with HTTP/2 enabled requires JDK11 or above",
          StorageNodeClientType.HTTP_CLIENT_5_CLIENT);
    }
    return testEnabled;
  }

  @Test
  public void testHttpClient5WithoutRequestTimeout() throws Exception {
    if (!isTestEnabled()) {
      return;
    }
    VeniceClusterWrapper veniceCluster = getVeniceCluster();
    VeniceServerWrapper serverWrapper = veniceCluster.getVeniceServers().get(0);
    Instance serverInstance = Instance.fromHostAndPort(serverWrapper.getHost(), serverWrapper.getPort());
    Optional<SSLFactory> sslFactory = Optional.of(SslUtils.getVeniceLocalSslFactory());
    // Form a heartbeat request
    VeniceMetaDataRequest request = new VeniceMetaDataRequest(
        serverInstance,
        QueryAction.HEALTH.toString().toLowerCase(),
        HTTP_GET,
        sslFactory.isPresent());
    // Don't setup the request timeout

    VeniceRouterConfig config = mock(VeniceRouterConfig.class);
    doReturn(1).when(config).getHttpClient5PoolSize();
    doReturn(2).when(config).getHttpClient5TotalIOThreadCount();
    doReturn(1).when(config).getHttpClient5PoolSize();
    doReturn(true).when(config).isHttpClient5SkipCipherCheck();
    doReturn(1000).when(config).getSocketTimeout();
    try (HttpClient5StorageNodeClient client = new HttpClient5StorageNodeClient(sslFactory, config)) {
      CompletableFuture<PortableHttpResponse> responseFuture = new CompletableFuture<>();
      client.sendRequest(request, responseFuture);
      responseFuture.get(3, TimeUnit.SECONDS);
    }
  }
}
