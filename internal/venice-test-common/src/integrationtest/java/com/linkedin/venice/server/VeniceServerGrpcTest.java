package com.linkedin.venice.server;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.listener.grpc.VeniceReadServiceClient;
import com.linkedin.venice.listener.grpc.VeniceReadServiceServer;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;


public class VeniceServerGrpcTest {
  private VeniceReadServiceServer server;
  private VeniceReadServiceClient client;

  private void createServer(int port) {
    server = new VeniceReadServiceServer(port);
    server.start();
  }

  private void createClient(String addr) {
    client = new VeniceReadServiceClient(addr);
  }

  @AfterMethod
  public void stopServer() {
    if (server != null) {
      server.stop();
    }

    if (client != null) {
      client.shutdown();
    }
  }

  @Test
  public void testValidToken() {
    createServer(50051);

    createClient("localhost:50051");
    String value = client.get("valid");
    Assert.assertEquals(value, "validvalid");
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*UNAVAILABLE: io exception.*")
  public void testServerNotStarted() {
    createClient("localhost:50051");
    client.get("valid");
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*FAILED_PRECONDITION")
  public void testInvalidToken() {
    createServer(50051);
    createClient("localhost:50051");
    client.get("invalid");
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*UNAVAILABLE: Channel shutdown invoked")
  public void shutDownClientPriorRequest() {
    createServer(50051);
    createClient("localhost:50051");
    client.shutdown();
    client.get("valid");
  }
}
