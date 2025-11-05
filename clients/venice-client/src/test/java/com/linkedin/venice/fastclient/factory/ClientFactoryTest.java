package com.linkedin.venice.fastclient.factory;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.alpini.base.concurrency.TimeoutProcessor;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.fastclient.ClientConfig;
import com.linkedin.venice.fastclient.DelegatingAvroStoreClient;
import com.linkedin.venice.fastclient.RetriableAvroGenericStoreClient;
import com.linkedin.venice.fastclient.RetriableAvroSpecificStoreClient;
import com.linkedin.venice.fastclient.meta.InstanceHealthMonitor;
import com.linkedin.venice.fastclient.meta.StoreMetadata;
import com.linkedin.venice.read.protocol.response.streaming.StreamingFooterRecordV1;
import io.tehuti.metrics.MetricsRepository;
import java.lang.reflect.Field;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ClientFactoryTest {
  private static final String STORE_NAME = "test_store";

  private StoreMetadata mockStoreMetadata;
  private InstanceHealthMonitor mockHealthMonitor;
  private TimeoutProcessor mockTimeoutProcessor;
  private ClientConfig.ClientConfigBuilder baseConfigBuilder;

  @BeforeMethod
  public void setUp() {
    // Mock dependencies
    mockStoreMetadata = mock(StoreMetadata.class);
    mockHealthMonitor = mock(InstanceHealthMonitor.class);
    mockTimeoutProcessor = mock(TimeoutProcessor.class);

    when(mockStoreMetadata.getInstanceHealthMonitor()).thenReturn(mockHealthMonitor);
    when(mockHealthMonitor.getTimeoutProcessor()).thenReturn(mockTimeoutProcessor);

    // Base config builder with minimum required fields
    baseConfigBuilder = new ClientConfig.ClientConfigBuilder<>().setStoreName(STORE_NAME)
        .setR2Client(mock(Client.class))
        .setD2Client(mock(D2Client.class))
        .setClusterDiscoveryD2Service("test_service")
        .setMetricsRepository(new MetricsRepository());
  }

  @Test
  public void testRetryClientAlwaysInGenericChain() throws Exception {
    ClientConfig clientConfig = baseConfigBuilder.build();

    AvroGenericStoreClient<String, Object> client =
        ClientFactory.getAndStartGenericStoreClient(mockStoreMetadata, clientConfig);

    assertNotNull(client);

    // Verify RetriableAvroGenericStoreClient is always in the delegation chain
    assertTrue(
        containsRetryClientInChain(client),
        "RetriableAvroGenericStoreClient should always be present in the client delegation chain");
  }

  @Test
  public void testRetryClientAlwaysInSpecificChain() throws Exception {
    ClientConfig clientConfig = baseConfigBuilder.setSpecificValueClass(StreamingFooterRecordV1.class).build();

    AvroSpecificStoreClient<String, StreamingFooterRecordV1> client =
        ClientFactory.getAndStartSpecificStoreClient(mockStoreMetadata, clientConfig);

    assertNotNull(client);

    // Verify RetriableAvroSpecificStoreClient is always in the delegation chain
    assertTrue(
        containsRetryClientInChainSpecific(client),
        "RetriableAvroSpecificStoreClient should always be present in the client delegation chain");
  }

  private boolean containsRetryClientInChain(AvroGenericStoreClient<?, ?> client) throws Exception {
    return findClientInDelegationChain(client, RetriableAvroGenericStoreClient.class) != null;
  }

  private boolean containsRetryClientInChainSpecific(AvroSpecificStoreClient<?, ?> client) throws Exception {
    return findClientInDelegationChain(client, RetriableAvroSpecificStoreClient.class) != null;
  }

  private Object findClientInDelegationChain(Object client, Class<?> targetClass) throws Exception {
    if (client == null) {
      return null;
    }

    if (targetClass.isInstance(client)) {
      return client;
    }

    if (client instanceof DelegatingAvroStoreClient) {
      try {
        Field delegateField = DelegatingAvroStoreClient.class.getDeclaredField("delegate");
        delegateField.setAccessible(true);
        Object delegate = delegateField.get(client);
        return findClientInDelegationChain(delegate, targetClass);
      } catch (NoSuchFieldException | IllegalAccessException e) {
        // If we can't access the delegate field, stop traversing
        return null;
      }
    }
    return null;
  }

}
