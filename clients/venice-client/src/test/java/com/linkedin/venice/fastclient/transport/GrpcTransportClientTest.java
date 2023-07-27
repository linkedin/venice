package com.linkedin.venice.fastclient.transport;

import com.linkedin.venice.protocols.VeniceReadServiceGrpc;
import io.grpc.ManagedChannel;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class GrpcTransportClientTest {
  private GrpcTransportClient grpcTransportClient;
  private ManagedChannel mockChannel;
  private VeniceReadServiceGrpc.VeniceReadServiceStub stub;

  @BeforeTest
  public void setUp() throws Exception {

  }

  @Test
  public void test() {
    // placeholder for now, need to figure out robust way to test gRPC Transport Client
  }

}
