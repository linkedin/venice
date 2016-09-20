package com.linkedin.venice.controllerapi;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by mwise on 6/1/16.
 */
public class TestControllerClient {

  @Test
  public static void clientReturnsErrorObjectOnConnectionFailure(){
    VersionResponse r3 = ControllerClient.queryCurrentVersion("http://localhost:17079", "myycluster", "mystore");
    Assert.assertTrue(r3.isError());
  }
}