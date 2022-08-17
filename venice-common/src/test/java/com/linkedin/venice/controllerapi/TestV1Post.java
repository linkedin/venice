package com.linkedin.venice.controllerapi;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.KEY_SCHEMA;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.OWNER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.VALUE_SCHEMA;

import com.linkedin.venice.controllerapi.routes.V1Post;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestV1Post {
  @Test
  public void testCreateStore() {
    String expectedRaw = "/v1/cluster/:" + CLUSTER + "/store";
    Assert.assertEquals(V1Post.STORE.getRawPath(), expectedRaw);

    Map<String, String> testParams = new HashMap<>();
    testParams.put(CLUSTER, "test-cluster");

    String expectedPath = "/v1/cluster/test-cluster/store";
    Assert.assertEquals(V1Post.STORE.getPathWithParameters(testParams), expectedPath);

    Assert.assertEquals(V1Post.STORE.getBodyParams(), new String[] { NAME, OWNER, KEY_SCHEMA, VALUE_SCHEMA });
  }
}
