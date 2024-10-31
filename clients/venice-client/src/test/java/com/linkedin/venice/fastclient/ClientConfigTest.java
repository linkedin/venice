package com.linkedin.venice.fastclient;

import static org.mockito.Mockito.mock;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import org.testng.annotations.Test;


/**
 * TODO:
 *  Add tests for options like setMaxAllowedKeyCntInBatchGetReq
 */

public class ClientConfigTest {
  private ClientConfig.ClientConfigBuilder getClientConfigWithMinimumRequiredInputs() {
    return new ClientConfig.ClientConfigBuilder<>().setStoreName("test_store")
        .setR2Client(mock(Client.class))
        .setD2Client(mock(D2Client.class))
        .setClusterDiscoveryD2Service("test_server_discovery");
  }

  @Test(expectedExceptions = VeniceClientException.class, expectedExceptionsMessageRegExp = "storeName param shouldn't be empty")
  public void testClientWithNoStoreName() {
    new ClientConfig.ClientConfigBuilder<>().build();
  }

  @Test(expectedExceptions = VeniceClientException.class, expectedExceptionsMessageRegExp = "storeName param shouldn't be empty")
  public void testClientWithEmptyStoreName() {
    new ClientConfig.ClientConfigBuilder<>().setStoreName("").build();
  }

  @Test(expectedExceptions = VeniceClientException.class, expectedExceptionsMessageRegExp = "r2Client param shouldn't be null")
  public void testClientWithoutR2Client() {
    new ClientConfig.ClientConfigBuilder<>().setStoreName("test_store").build();
  }

  @Test
  public void testClientWithAllRequiredInputs() {
    ClientConfig.ClientConfigBuilder clientConfigBuilder = getClientConfigWithMinimumRequiredInputs();
    clientConfigBuilder.build();
  }

  @Test(expectedExceptions = VeniceClientException.class, expectedExceptionsMessageRegExp = "Either param: specificThinClient or param: genericThinClient.*")
  public void testClientWithDualReadAndNoThinClients() {
    ClientConfig.ClientConfigBuilder clientConfigBuilder = getClientConfigWithMinimumRequiredInputs();
    clientConfigBuilder.setDualReadEnabled(true);
    clientConfigBuilder.build();
  }

  @Test(expectedExceptions = VeniceClientException.class, expectedExceptionsMessageRegExp = "Both param: specificThinClient and param: genericThinClient should not be specified.*")
  public void testClientWithOutDualReadButWithThinClients() {
    ClientConfig.ClientConfigBuilder clientConfigBuilder = getClientConfigWithMinimumRequiredInputs();
    clientConfigBuilder.setGenericThinClient(mock(AvroGenericStoreClient.class));
    clientConfigBuilder.build();
  }

  @Test(expectedExceptions = VeniceClientException.class, expectedExceptionsMessageRegExp = "longTailRetryThresholdForSingleGetInMicroSeconds must be positive.*")
  public void testClientWithInvalidLongTailRetryThresholdForSingleGet() {
    ClientConfig.ClientConfigBuilder clientConfigBuilder = getClientConfigWithMinimumRequiredInputs();
    clientConfigBuilder.setLongTailRetryEnabledForSingleGet(true);
    clientConfigBuilder.setLongTailRetryThresholdForSingleGetInMicroSeconds(0);
    clientConfigBuilder.build();
  }

  @Test(expectedExceptions = VeniceClientException.class, expectedExceptionsMessageRegExp = "longTailRetryThresholdForBatchGetInMicroSeconds must be positive.*")
  public void testClientWithInvalidLongTailRetryThresholdForBatchGet() {
    ClientConfig.ClientConfigBuilder clientConfigBuilder = getClientConfigWithMinimumRequiredInputs();
    clientConfigBuilder.setLongTailRetryEnabledForBatchGet(true);
    clientConfigBuilder.setLongTailRetryThresholdForBatchGetInMicroSeconds(0);
    clientConfigBuilder.build();
  }

  @Test(expectedExceptions = VeniceClientException.class, expectedExceptionsMessageRegExp = "Speculative query feature can't be enabled together with long-tail retry for single-get")
  public void testLongTailRetryWithSpeculativeQuery() {
    ClientConfig.ClientConfigBuilder clientConfigBuilder = getClientConfigWithMinimumRequiredInputs();
    clientConfigBuilder.setSpeculativeQueryEnabled(true);
    clientConfigBuilder.setLongTailRetryEnabledForSingleGet(true);
    clientConfigBuilder.build();
  }

  @Test
  public void testLongTailRetryWithDualRead() {
    ClientConfig.ClientConfigBuilder clientConfigBuilder = getClientConfigWithMinimumRequiredInputs();
    clientConfigBuilder.setSpeculativeQueryEnabled(false)
        .setDualReadEnabled(true)
        .setGenericThinClient(mock(AvroGenericStoreClient.class))
        .setLongTailRetryEnabledForSingleGet(true)
        .setLongTailRetryThresholdForSingleGetInMicroSeconds(1000)
        .build();
  }
}
