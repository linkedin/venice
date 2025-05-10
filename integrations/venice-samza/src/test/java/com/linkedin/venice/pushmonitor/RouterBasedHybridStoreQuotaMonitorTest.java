package com.linkedin.venice.pushmonitor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.routerapi.HybridStoreQuotaStatusResponse;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RouterBasedHybridStoreQuotaMonitorTest {
  private static final String STORE_NAME = "fake_Store";
  private static final String TOPIC_NAME = "fake_Store_v1";

  @Test
  public void testTransportClientReinit()
      throws IOException, ExecutionException, InterruptedException, TimeoutException {
    TransportClient mockTransportclient = Mockito.mock(TransportClient.class);
    TransportClientResponse mockResponse = Mockito.mock(TransportClientResponse.class);
    ObjectMapper mockMapper = Mockito.mock(ObjectMapper.class);
    HybridStoreQuotaStatusResponse mockQuotaStatusResponse = Mockito.mock(HybridStoreQuotaStatusResponse.class);
    Mockito.when(mockResponse.getBody()).thenReturn(STORE_NAME.getBytes());
    Mockito.when(mockTransportclient.get(Mockito.anyString()))
        .thenReturn(CompletableFuture.completedFuture(mockResponse));
    Mockito
        .when(mockMapper.readValue(Mockito.eq(STORE_NAME.getBytes()), Mockito.eq(HybridStoreQuotaStatusResponse.class)))
        .thenReturn(mockQuotaStatusResponse);
    Mockito.when(mockQuotaStatusResponse.isError()).thenReturn(true);
    Mockito.when(mockQuotaStatusResponse.getErrorType()).thenReturn(ErrorType.STORE_NOT_FOUND);

    final boolean[] isReinitCalled = { false };
    RouterBasedHybridStoreQuotaMonitor.TransportClientReinitProvider transportClientReinitProvider = () -> {
      isReinitCalled[0] = true;
      return mockTransportclient;
    };
    RouterBasedHybridStoreQuotaMonitor routerBasedHybridStoreQuotaMonitor = new RouterBasedHybridStoreQuotaMonitor(
        mockTransportclient,
        STORE_NAME,
        Version.PushType.STREAM,
        TOPIC_NAME,
        transportClientReinitProvider);

    routerBasedHybridStoreQuotaMonitor.getHybridQuotaMonitorTask().setMapper(mockMapper);
    routerBasedHybridStoreQuotaMonitor.getHybridQuotaMonitorTask().checkStatus();

    Assert.assertTrue(isReinitCalled[0]);
  }

  @Test
  public void testStatusChange() throws IOException, ExecutionException, InterruptedException, TimeoutException {
    TransportClient mockTransportclient = Mockito.mock(TransportClient.class);
    TransportClientResponse mockResponse = Mockito.mock(TransportClientResponse.class);
    ObjectMapper mockMapper = Mockito.mock(ObjectMapper.class);
    HybridStoreQuotaStatusResponse mockQuotaStatusResponse = Mockito.mock(HybridStoreQuotaStatusResponse.class);
    Mockito.when(mockResponse.getBody()).thenReturn(STORE_NAME.getBytes());
    Mockito.when(mockTransportclient.get(Mockito.anyString()))
        .thenReturn(CompletableFuture.completedFuture(mockResponse));
    Mockito
        .when(mockMapper.readValue(Mockito.eq(STORE_NAME.getBytes()), Mockito.eq(HybridStoreQuotaStatusResponse.class)))
        .thenReturn(mockQuotaStatusResponse);
    Mockito.when(mockQuotaStatusResponse.isError()).thenReturn(false);
    Mockito.when(mockQuotaStatusResponse.getQuotaStatus()).thenReturn(HybridStoreQuotaStatus.QUOTA_VIOLATED);

    final boolean[] isReinitCalled = { false };
    RouterBasedHybridStoreQuotaMonitor.TransportClientReinitProvider transportClientReinitProvider = () -> {
      isReinitCalled[0] = true;
      return mockTransportclient;
    };
    RouterBasedHybridStoreQuotaMonitor routerBasedHybridStoreQuotaMonitor = new RouterBasedHybridStoreQuotaMonitor(
        mockTransportclient,
        STORE_NAME,
        Version.PushType.STREAM,
        TOPIC_NAME,
        transportClientReinitProvider);

    routerBasedHybridStoreQuotaMonitor.getHybridQuotaMonitorTask().setMapper(mockMapper);
    routerBasedHybridStoreQuotaMonitor.getHybridQuotaMonitorTask().checkStatus();

    Assert.assertFalse(isReinitCalled[0]);
    Assert.assertEquals(routerBasedHybridStoreQuotaMonitor.getCurrentStatus(), HybridStoreQuotaStatus.QUOTA_VIOLATED);
  }
}
