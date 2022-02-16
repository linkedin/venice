package com.linkedin.venice.fastclient;

import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class ClientConfigTest {

  @Test (expectedExceptions = VeniceClientException.class, expectedExceptionsMessageRegExp = ".*Speculative query feature can't be enabled together with.*")
  public void testLongTailRetryWithSpeculativeQuery() {
    new ClientConfig.ClientConfigBuilder<>()
        .setSpeculativeQueryEnabled(true)
        .setR2Client(mock(Client.class))
        .setStoreName("test_store")
        .setGenericThinClient(mock(AvroGenericStoreClient.class))
        .setLongTailRetryEnabledForSingleGet(true)
        .setLongTailRetryThresholdForSingletGetInMicroSeconds(1000)
        .build();

  }

  @Test
  public void testLongTailRetryWithDualRead() {
    new ClientConfig.ClientConfigBuilder<>()
        .setSpeculativeQueryEnabled(false)
        .setR2Client(mock(Client.class))
        .setStoreName("test_store")
        .setDualReadEnabled(true)
        .setGenericThinClient(mock(AvroGenericStoreClient.class))
        .setLongTailRetryEnabledForSingleGet(true)
        .setLongTailRetryThresholdForSingletGetInMicroSeconds(1000)
        .build();

  }
}
