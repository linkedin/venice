package com.linkedin.venice.controllerapi;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NAME;

import com.linkedin.venice.controllerapi.routes.V1Get;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestV1Get {
  @Test
  public void testGetStore() {
    String expectedRaw = "/v1/cluster/:" + CLUSTER + "/store/:" + NAME;
    Assert.assertEquals(V1Get.STORE.getRawPath(), expectedRaw);

    Map<String, String> testParams = new HashMap<>();
    testParams.put(NAME, "test-store");
    testParams.put(CLUSTER, "test-cluster");

    String expectedPath = "/v1/cluster/test-cluster/store/test-store";
    Assert.assertEquals(V1Get.STORE.getPathWithParameters(testParams), expectedPath);
  }

}
