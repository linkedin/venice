package com.linkedin.venice.meta;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by mwise on 4/22/16.
 */
public class TestInstance {
  @Test
  public void getHostWorksRight() {
    Instance ip4 = new Instance("0", "127.0.0.1", 1234);
    Assert.assertEquals(ip4.getUrl(), "http://127.0.0.1:1234");

    Instance ip6 = new Instance("0", "::1", 4567);
    Assert.assertEquals(ip6.getUrl(), "http://[::1]:4567");
  }

  @Test
  public void parsesNodeId() {
    Instance host = Instance.fromNodeId("localhost_1234");
    Assert.assertEquals(host.getHost(), "localhost");
    Assert.assertEquals(host.getPort(), 1234);
  }

  @Test
  public void testInstanceWithGrpcAddress() {
    Instance nonGrpcInstance = new Instance("localhost_1234", "localhost", 1234);
    Assert.assertEquals(nonGrpcInstance.getGrpcSslPort(), -1);
    Assert.assertEquals(nonGrpcInstance.getGrpcPort(), -1);

    Instance grpcInstance = new Instance("localhost_1234", "localhost", 1234, 1235, 1236);
    Assert.assertEquals(grpcInstance.getGrpcPort(), 1235);
    Assert.assertEquals(grpcInstance.getGrpcSslPort(), 1236);
    Assert.assertEquals(grpcInstance.getGrpcUrl(), "localhost:1235");
    Assert.assertEquals(grpcInstance.getGrpcSslUrl(), "localhost:1236");
  }
}
