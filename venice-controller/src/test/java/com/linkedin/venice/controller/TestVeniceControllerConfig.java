package com.linkedin.venice.controller;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.TestUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestVeniceControllerConfig {

  @Test
  public void canParseSimpleClusterMap(){
    String config = "{\"ei-ltx1\":[\"http://host:1234\"]}";
    Map<String, List<String>> map = VeniceControllerConfig.parseClusterMap(config);
    Assert.assertEquals(map.size(), 1);
    Assert.assertTrue(map.keySet().contains("ei-ltx1"));
  }

  @Test
  public void canParseChildClusterMap()
      throws IOException {
    List<String> nodes = new ArrayList<>();
    for (int i=0; i<30; i++){
      nodes.add(TestUtils.getUniqueString("app")+":"+i);
    }

    List<String> cluster1 = new ArrayList<>();
    for (int i : Arrays.asList(1,2,3)){
      cluster1.add("http://" + nodes.get(i));
    }

    List<String> cluster2 = new ArrayList<>();
    for (int i : Arrays.asList(11,12,13)){
      cluster2.add("http://" + nodes.get(i));
    }

    List<String> cluster3 = new ArrayList<>();
    for (int i : Arrays.asList(21,22,23)){
      cluster3.add("http://" + nodes.get(i));
    }

    Map<String, List<String>> clusters = new HashMap<>();
    clusters.put("ei-ltx1", cluster1);
    clusters.put("ei-lca1", cluster2);
    clusters.put("ei-lva1", cluster3);
    String childConfig = new ObjectMapper().writer().writeValueAsString(clusters);

    Map<String, List<String>> ltxMap = VeniceControllerConfig.parseClusterMap(childConfig);
    Assert.assertEquals(ltxMap.size(), 3, "parsed child cluster map must have 3 entries");
    Assert.assertEquals(ltxMap.get("ei-ltx1"), cluster1);
    Assert.assertEquals(ltxMap.get("ei-lva1"), cluster3);
  }

  @Test(expectedExceptions = VeniceException.class)
  public void errOnMissingScheme(){
    String input = "{\"ei-ltx1\":[\"host:1234\"]}";
    VeniceControllerConfig.parseClusterMap(input);
  }

  @Test(expectedExceptions = VeniceException.class)
  public void errOnMissingNodes(){
    String input = "{\"ei-ltx1\":[]}";
    VeniceControllerConfig.parseClusterMap(input);
  }


}
