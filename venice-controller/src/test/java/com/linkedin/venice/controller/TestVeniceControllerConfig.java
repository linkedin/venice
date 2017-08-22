package com.linkedin.venice.controller;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.PropertyBuilder;
import java.util.*;

import org.testng.Assert;
import org.testng.annotations.Test;


public class TestVeniceControllerConfig {
  private final static String DELIMITER = ",\\s*";
  private final static String WHITE_LIST = "dc1,dc2";

  @Test
  public void canParseClusterMap() {
    PropertyBuilder builder = new PropertyBuilder();
    builder.put("child.cluster.url.dc1", "http://host:1234, http://host:5678")
        .put("child.cluster.url.dc2", "http://host:1234, http://host:5678");

    Map<String, String> map = VeniceControllerConfig.parseClusterMap(builder.build(), WHITE_LIST);

    Assert.assertEquals(map.size(), 2);
    Assert.assertTrue(map.keySet().contains("dc1"));
    Assert.assertTrue(map.keySet().contains("dc2"));

    String[] uris = map.get("dc1").split(DELIMITER);
    Assert.assertTrue(uris[0].equals("http://host:1234"));
    Assert.assertTrue(uris[1].equals("http://host:5678"));
  }

  @Test
  public void canParseD2ClusterMap() {
    PropertyBuilder builder = new PropertyBuilder();
    builder.put("child.cluster.d2.zkHost.dc1", "zkAddress1")
        .put("child.cluster.d2.zkHost.dc2", "zkAddress2");

    Map<String, String> map = VeniceControllerConfig.parseClusterMap(builder.build(), WHITE_LIST, true);
    Assert.assertEquals(map.get("dc1").split(DELIMITER).length, 1);
    Assert.assertTrue(map.get("dc2").split(DELIMITER)[0].equals("zkAddress2"));
  }

  @Test(expectedExceptions = VeniceException.class)
  public void emptyWhitelist() {
    PropertyBuilder build = new PropertyBuilder()
        .put("child.cluster.url.dc1", "http://host:1234, http://host:5678")
        .put("child.cluster.url.dc2", "http://host:1234, http://host:5678");
    VeniceControllerConfig.parseClusterMap(build.build(), "");
  }

  @Test(expectedExceptions = VeniceException.class)
  public void nullWhitelist() {
    PropertyBuilder build = new PropertyBuilder()
        .put("child.cluster.url.dc1", "http://host:1234, http://host:5678")
        .put("child.cluster.url.dc2", "http://host:1234, http://host:5678");
    VeniceControllerConfig.parseClusterMap(build.build(), "");
  }

  @Test(expectedExceptions = VeniceException.class)
  public void errOnMissingScheme(){
    PropertyBuilder builder = new PropertyBuilder();
    builder.put("child.cluster.url.dc1", "host:1234");
    VeniceControllerConfig.parseClusterMap(builder.build(), WHITE_LIST);
  }

  @Test(expectedExceptions = VeniceException.class)
  public void errOnMissingNodes(){
    PropertyBuilder builder = new PropertyBuilder();
    builder.put("child.cluster.url.dc1", "");
    VeniceControllerConfig.parseClusterMap(builder.build(), WHITE_LIST);
  }
}
