package com.linkedin.venice.controller;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.PropertyBuilder;
import java.util.*;

import org.testng.Assert;
import org.testng.annotations.Test;


public class TestVeniceControllerConfig {

  @Test
  public void canParseClusterMap(){
    PropertyBuilder builder = new PropertyBuilder();
    builder.put("dc1", "http://host:1234, http://host:5678")
           .put("dc2", "http://host:1234, http://host:5678");

    String whitelist = "dc1,dc2";
    Map<String, Set<String>> map = VeniceControllerConfig.parseClusterMap(builder.build(), whitelist);

    Assert.assertEquals(map.size(), 2);
    Assert.assertTrue(map.keySet().contains("dc1"));
    Assert.assertTrue(map.keySet().contains("dc2"));

    Assert.assertEquals(map.get("dc1").size(), 2);
    Assert.assertTrue(map.get("dc1").contains("http://host:1234"));
    Assert.assertTrue(map.get("dc1").contains("http://host:5678"));
  }

  @Test(expectedExceptions = VeniceException.class)
  public void emptyChildControllerList() {
    PropertyBuilder build = new PropertyBuilder();
    String whitelist = "dc1,dc2";
    VeniceControllerConfig.parseClusterMap(build.build(), whitelist);
  }

  @Test(expectedExceptions = VeniceException.class)
  public void emptyWhitelist() {
    PropertyBuilder build = new PropertyBuilder()
        .put("dc1", "http://host:1234, http://host:5678")
        .put("dc2", "http://host:1234, http://host:5678");
    VeniceControllerConfig.parseClusterMap(build.build(), "");
  }

  @Test(expectedExceptions = VeniceException.class)
  public void nullWhitelist() {
    PropertyBuilder build = new PropertyBuilder()
        .put("dc1", "http://host:1234, http://host:5678")
        .put("dc2", "http://host:1234, http://host:5678");
    VeniceControllerConfig.parseClusterMap(build.build(), null);
  }

  @Test(expectedExceptions = VeniceException.class)
  public void errOnMissingClusterName() {
    PropertyBuilder builder = new PropertyBuilder();
    builder.put("", "http://host:1234");
    String whitelist = "dc1,dc2";
    VeniceControllerConfig.parseClusterMap(builder.build(), whitelist);
  }

  @Test(expectedExceptions = VeniceException.class)
  public void errOnMissingScheme(){
    PropertyBuilder builder = new PropertyBuilder();
    builder.put("dc1", "host:1234");

    String whitelist = "dc1";
    VeniceControllerConfig.parseClusterMap(builder.build(), whitelist);
  }

  @Test(expectedExceptions = VeniceException.class)
  public void errOnMissingNodes(){
    PropertyBuilder builder = new PropertyBuilder();
    builder.put("dc1", "");
    String whitelist = "dc1";
    VeniceControllerConfig.parseClusterMap(builder.build(), whitelist);
  }
}
