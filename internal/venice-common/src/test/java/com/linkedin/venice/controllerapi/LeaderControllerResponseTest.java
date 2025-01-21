package com.linkedin.venice.controllerapi;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import org.testng.annotations.Test;


public class LeaderControllerResponseTest {
  @Test
  public void testLeaderControllerResponse() {
    LeaderControllerResponse response = new LeaderControllerResponse();

    // Testing getters and setters
    String cluster = "test-cluster";
    String url = "http://leader-url";
    String secureUrl = "https://secure-leader-url";
    String grpcUrl = "grpc://leader-grpc-url";
    String secureGrpcUrl = "grpcs://secure-leader-grpc-url";

    response.setCluster(cluster);
    assertEquals(response.getCluster(), cluster, "Cluster name should match the set value");

    response.setUrl(url);
    assertEquals(response.getUrl(), url, "URL should match the set value");

    response.setSecureUrl(secureUrl);
    assertEquals(response.getSecureUrl(), secureUrl, "Secure URL should match the set value");

    response.setGrpcUrl(grpcUrl);
    assertEquals(response.getGrpcUrl(), grpcUrl, "gRPC URL should match the set value");

    response.setSecureGrpcUrl(secureGrpcUrl);
    assertEquals(response.getSecureGrpcUrl(), secureGrpcUrl, "Secure gRPC URL should match the set value");

    // Testing default constructor
    LeaderControllerResponse defaultResponse = new LeaderControllerResponse();
    assertNull(defaultResponse.getCluster(), "Cluster should be null by default");
    assertNull(defaultResponse.getUrl(), "URL should be null by default");
    assertNull(defaultResponse.getSecureUrl(), "Secure URL should be null by default");
    assertNull(defaultResponse.getGrpcUrl(), "gRPC URL should be null by default");
    assertNull(defaultResponse.getSecureGrpcUrl(), "Secure gRPC URL should be null by default");

    // Testing combined constructor
    LeaderControllerResponse combinedResponse = new LeaderControllerResponse();

    combinedResponse.setCluster(cluster);
    combinedResponse.setUrl(url);
    combinedResponse.setSecureUrl(secureUrl);
    combinedResponse.setGrpcUrl(grpcUrl);
    combinedResponse.setSecureGrpcUrl(secureGrpcUrl);

    assertEquals(combinedResponse.getCluster(), cluster, "Cluster should match the set value");
    assertEquals(combinedResponse.getUrl(), url, "URL should match the set value");
    assertEquals(combinedResponse.getSecureUrl(), secureUrl, "Secure URL should match the set value");
    assertEquals(combinedResponse.getGrpcUrl(), grpcUrl, "gRPC URL should match the set value");
    assertEquals(combinedResponse.getSecureGrpcUrl(), secureGrpcUrl, "Secure gRPC URL should match the set value");
  }
}
