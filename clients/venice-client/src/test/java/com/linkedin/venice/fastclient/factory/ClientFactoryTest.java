package com.linkedin.venice.fastclient.factory;

import static com.linkedin.venice.client.stats.BasicClientStats.CLIENT_METRIC_ENTITIES;
import static com.linkedin.venice.stats.ClientType.FAST_CLIENT;
import static com.linkedin.venice.stats.VeniceMetricsRepository.getVeniceMetricsRepository;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.fastclient.ClientConfig;
import com.linkedin.venice.fastclient.meta.InstanceHealthMonitor;
import com.linkedin.venice.fastclient.meta.StoreMetadata;
import com.linkedin.venice.meta.RetryManager;
import java.io.IOException;
import org.mockito.MockedConstruction;
import org.testng.annotations.Test;


public class ClientFactoryTest {
  private static final String STORE_NAME = "test_store";

  @Test
  public void testStartFailureClosesTheClient() throws IOException {
    StoreMetadata storeMetadata = mock(StoreMetadata.class);
    doReturn(STORE_NAME).when(storeMetadata).getStoreName();
    doReturn(mock(InstanceHealthMonitor.class, RETURNS_DEEP_STUBS)).when(storeMetadata).getInstanceHealthMonitor();
    doThrow(new VeniceClientException("Mock start failure")).when(storeMetadata).start();
    ClientConfig clientConfig = new ClientConfig.ClientConfigBuilder<>().setStoreName(STORE_NAME)
        .setR2Client(mock(Client.class))
        .setD2Client(mock(D2Client.class))
        .setClusterDiscoveryD2Service("test_server_discovery")
        .setMetricsRepository(getVeniceMetricsRepository(FAST_CLIENT, CLIENT_METRIC_ENTITIES, true))
        .setLongTailRetryEnabledForSingleGet(true)
        .setLongTailRetryThresholdForSingleGetInMicroSeconds(1000)
        .build();

    try (MockedConstruction<RetryManager> retryManagers = mockConstruction(RetryManager.class)) {
      assertThrows(
          VeniceClientException.class,
          () -> ClientFactory.getAndStartGenericStoreClient(storeMetadata, clientConfig));
      // The caller never gets the client, so the factory closes it: its retry managers and its metadata.
      assertEquals(retryManagers.constructed().size(), 2);
      retryManagers.constructed().forEach(retryManager -> verify(retryManager).close());
    }
    verify(storeMetadata).close();
  }
}
