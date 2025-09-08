package com.linkedin.venice.system.store;

import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_STORE_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class MetaStoreReaderTest {
  private static final String CLUSTER_DISCOVERY_D2_SERVICE_NAME =
      ClientConfig.DEFAULT_CLUSTER_DISCOVERY_D2_SERVICE_NAME + "_test";

  private D2Client d2ClientMock;
  private AvroSpecificStoreClient<StoreMetaKey, StoreMetaValue> storeClientMock;
  private final static String storeName = "venice-test-meta-store";

  @BeforeMethod
  public void setUp() {
    d2ClientMock = mock(D2Client.class);
    storeClientMock = mock(AvroSpecificStoreClient.class);
  }

  @Test
  public void testGetHeartbeatFromMetaStore() throws ExecutionException, InterruptedException, TimeoutException {
    StoreMetaKey storeMetaKey =
        MetaStoreDataType.HEARTBEAT.getStoreMetaKey(Collections.singletonMap(KEY_STRING_STORE_NAME, storeName));
    StoreMetaValue storeMetaValue = new StoreMetaValue();
    storeMetaValue.timestamp = 123L;

    MetaStoreReader storeReaderSpy = spy(new MetaStoreReader(d2ClientMock, CLUSTER_DISCOVERY_D2_SERVICE_NAME));
    CompletableFuture<StoreMetaValue> completableFutureMock = mock(CompletableFuture.class);

    doReturn(storeClientMock).when(storeReaderSpy).getVeniceClient(any());
    when(storeClientMock.get(storeMetaKey)).thenReturn(completableFutureMock);
    when(completableFutureMock.get(anyLong(), any())).thenReturn(storeMetaValue);

    Assert.assertEquals(storeReaderSpy.getHeartbeat(storeName), 123L);
    verify(completableFutureMock).get(anyLong(), any());
    verify(storeClientMock).get(storeMetaKey);
  }

  @Test
  public void testClientConfig() {
    MetaStoreReader storeReaderSpy = spy(new MetaStoreReader(d2ClientMock, CLUSTER_DISCOVERY_D2_SERVICE_NAME));
    ClientConfig clientConfig = storeReaderSpy.getClientConfig(storeName);
    Assert.assertFalse(clientConfig.isStatTrackingEnabled());
  }
}
