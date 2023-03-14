package com.linkedin.venice.utils;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.meta.StoreInfo;
import org.testng.annotations.Test;


public class TestUtilsTest {
  @Test
  public void testVerifyDCConfigNativeAndActiveRepl() {
    ControllerClient controllerClient = mock(ControllerClient.class);
    StoreResponse storeResponse = mock(StoreResponse.class);
    StoreInfo storeInfo = mock(StoreInfo.class);
    when(storeInfo.isNativeReplicationEnabled()).thenReturn(true);
    when(storeInfo.isActiveActiveReplicationEnabled()).thenReturn(true);
    when(storeResponse.getStore()).thenReturn(storeInfo);
    when(controllerClient.getStore(anyString())).thenReturn(storeResponse);
    TestUtils.verifyDCConfigNativeAndActiveRepl("blah", true, true, controllerClient);
    verify(controllerClient).getStore(anyString());
    verify(storeInfo).isNativeReplicationEnabled();
    verify(storeInfo).isActiveActiveReplicationEnabled();
  }
}
