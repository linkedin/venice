package com.linkedin.venice.utils;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.meta.StoreInfo;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
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

  @Test
  public void testCombineConfigs() {
    Map<String, String> combinedConfigs = TestUtils.mergeConfigs(
        Arrays.asList(
            getAdditionalConfigsForRegion(0),
            getAdditionalConfigsForRegion(1),
            getAdditionalConfigsForRegion(2)));
    assertNotNull(combinedConfigs);
    assertEquals(combinedConfigs.size(), 2);

    Properties properties = new Properties();
    properties.putAll(combinedConfigs);
    VeniceProperties veniceProperties = new VeniceProperties(properties);

    Map<String, String> regionToBrokerUrlMap = veniceProperties.getMap("pubsub.region.to.broker.url");
    assertEquals(regionToBrokerUrlMap.size(), 3);
    assertEquals(regionToBrokerUrlMap.get("dc0"), "dc0.pubsub.com:9090");
    assertEquals(regionToBrokerUrlMap.get("dc1"), "dc1.pubsub.com:9091");
    assertEquals(regionToBrokerUrlMap.get("dc2"), "dc2.pubsub.com:9092");

    Map<String, String> regionToSomeConfigMap = veniceProperties.getMap("pubsub.region.to.some.config");
    assertEquals(regionToSomeConfigMap.size(), 3);

    assertEquals(regionToSomeConfigMap.get("dc0"), "dc0.some.config.com:8080");
    assertEquals(regionToSomeConfigMap.get("dc1"), "dc1.some.config.com:8081");
    assertEquals(regionToSomeConfigMap.get("dc2"), "dc2.some.config.com:8082");
  }

  private Map<String, String> getAdditionalConfigsForRegion(int regionNumber) {
    Map<String, String> additionalConfig = new HashMap<>(2);
    additionalConfig.put(
        "pubsub.region.to.broker.url",
        "dc" + regionNumber + ":dc" + regionNumber + ".pubsub.com:909" + regionNumber);
    additionalConfig.put(
        "pubsub.region.to.some.config",
        "dc" + regionNumber + ":dc" + regionNumber + ".some.config.com:808" + regionNumber);
    return additionalConfig;
  }
}
