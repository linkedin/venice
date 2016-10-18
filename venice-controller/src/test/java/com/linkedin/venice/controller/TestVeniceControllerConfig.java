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
    builder.put("ei-ltx1", "http://host:1234, http://host:5678")
           .put("ei-lca1", "http://host:1234, http://host:5678");

    Map<String, Set<String>> map = VeniceControllerConfig.parseClusterMap(builder.build());

    Assert.assertEquals(map.size(), 2);
    Assert.assertTrue(map.keySet().contains("ei-ltx1"));
    Assert.assertTrue(map.keySet().contains("ei-lca1"));

    Assert.assertEquals(map.get("ei-ltx1").size(), 2);
    Assert.assertTrue(map.get("ei-ltx1").contains("http://host:1234"));
    Assert.assertTrue(map.get("ei-ltx1").contains("http://host:5678"));
  }

  @Test(expectedExceptions = VeniceException.class)
  public void emptyChildControllerList() {
    PropertyBuilder build = new PropertyBuilder();

    VeniceControllerConfig.parseClusterMap(build.build());
  }

  @Test(expectedExceptions = VeniceException.class)
  public void errOnMissingClusterName() {
    PropertyBuilder builder = new PropertyBuilder();
    builder.put("", "http://host:1234");

    VeniceControllerConfig.parseClusterMap(builder.build());
  }

  @Test(expectedExceptions = VeniceException.class)
  public void errOnMissingScheme(){
    PropertyBuilder builder = new PropertyBuilder();
    builder.put("ei-ltx1", "host:1234");

    VeniceControllerConfig.parseClusterMap(builder.build());
  }

  @Test(expectedExceptions = VeniceException.class)
  public void errOnMissingNodes(){
    PropertyBuilder builder = new PropertyBuilder();
    builder.put("ei-ltx1", "");

    VeniceControllerConfig.parseClusterMap(builder.build());
  }
}
