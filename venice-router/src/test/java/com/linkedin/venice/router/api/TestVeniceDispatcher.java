package com.linkedin.venice.router.api;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by mwise on 4/25/16.
 */
public class TestVeniceDispatcher {
  @Test
  public void parsesPartitionName(){
    String name = "myCountry_v1_2";
    String number = VeniceDispatcher.numberFromPartitionName(name);

    Assert.assertEquals(number, "2");
  }
}
