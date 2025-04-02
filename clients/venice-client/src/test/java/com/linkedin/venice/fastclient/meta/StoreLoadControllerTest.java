package com.linkedin.venice.fastclient.meta;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.client.exceptions.VeniceClientRateExceededException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.fastclient.ClientConfig;
import org.mockito.Mockito;
import org.testng.annotations.Test;


public class StoreLoadControllerTest {
  @Test
  public void testQuotaRejectedRequest() throws InterruptedException {
    ClientConfig clientConfig = Mockito.mock(ClientConfig.class);
    Mockito.when(clientConfig.getStoreName()).thenReturn("test_store");
    Mockito.when(clientConfig.isStoreLoadControllerEnabled()).thenReturn(true);
    Mockito.when(clientConfig.getStoreLoadControllerWindowSizeInSec()).thenReturn(5);
    Mockito.when(clientConfig.getStoreLoadControllerAcceptMultiplier()).thenReturn(1.5);
    Mockito.when(clientConfig.getStoreLoadControllerMaxRejectionRatio()).thenReturn(0.9);
    Mockito.when(clientConfig.getStoreLoadControllerRejectionRatioUpdateIntervalInSec()).thenReturn(1);
    Mockito.when(clientConfig.getStoreLoadControllerMaxRejectionRatio()).thenReturn(0.9);

    StoreLoadController loadController = new StoreLoadController(clientConfig);

    for (int i = 0; i < 30; i++) {
      loadController.recordResponse(null);
    }
    for (int i = 0; i < 70; i++) {
      loadController.recordResponse(new Exception("Other non-quota exception"));
    }
    assertEquals(loadController.getRejectionRatio(), 0.0d);

    // Use a new sliding window.
    loadController = new StoreLoadController(clientConfig);
    for (int i = 0; i < 10; ++i) {
      loadController.recordResponse(new VeniceException(new VeniceClientRateExceededException("Quota rejected")));
    }
    assertTrue(loadController.getRejectionRatio() > 0);
  }
}
