package com.linkedin.venice.controllerapi;

import static com.linkedin.venice.meta.Version.PushType.BATCH;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class RequestTopicForPushRequestTest {
  private RequestTopicForPushRequest request;

  @BeforeMethod
  public void setUp() {
    request = new RequestTopicForPushRequest("clusterA", "storeA", BATCH, "job123");
  }

  @Test
  public void testRequestTopicForPushRequestConstructorArgs() {
    assertEquals(request.getClusterName(), "clusterA");
    assertEquals(request.getStoreName(), "storeA");
    assertEquals(request.getPushType(), BATCH);
    assertEquals(request.getPushJobId(), "job123");

    // Invalid clusterName
    IllegalArgumentException ex1 = Assert.expectThrows(
        IllegalArgumentException.class,
        () -> new RequestTopicForPushRequest("", "storeA", BATCH, "job123"));
    assertEquals(ex1.getMessage(), "clusterName is required");

    // Invalid storeName
    IllegalArgumentException ex2 = Assert.expectThrows(
        IllegalArgumentException.class,
        () -> new RequestTopicForPushRequest("clusterA", "", BATCH, "job123"));
    assertEquals(ex2.getMessage(), "storeName is required");

    // Null pushType
    IllegalArgumentException ex3 = Assert.expectThrows(
        IllegalArgumentException.class,
        () -> new RequestTopicForPushRequest("clusterA", "storeA", null, "job123"));
    assertEquals(ex3.getMessage(), "pushType is required");

    // Invalid pushJobId
    IllegalArgumentException ex4 = Assert.expectThrows(
        IllegalArgumentException.class,
        () -> new RequestTopicForPushRequest("clusterA", "storeA", BATCH, ""));
    assertEquals(ex4.getMessage(), "pushJobId is required");
  }

  @Test
  public void testRequestTopicForPushRequestSettersAndGetters() {
    request.setSendStartOfPush(true);
    request.setSorted(true);
    request.setWriteComputeEnabled(true);
    request.setSourceGridFabric("fabricA");
    request.setRewindTimeInSecondsOverride(3600);
    request.setDeferVersionSwap(true);
    request.setTargetedRegions("regionA,regionB");
    request.setRepushSourceVersion(42);
    request.setPartitioners("partitioner1,partitioner2");
    request.setCompressionDictionary("compressionDict");
    request.setEmergencySourceRegion("regionX");
    request.setSeparateRealTimeTopicEnabled(true);

    X509Certificate x509Certificate = mock(X509Certificate.class);
    request.setCertificateInRequest(x509Certificate);

    assertTrue(request.isSendStartOfPush());
    assertTrue(request.isSorted());
    assertTrue(request.isWriteComputeEnabled());
    assertEquals(request.getSourceGridFabric(), "fabricA");
    assertEquals(request.getRewindTimeInSecondsOverride(), 3600);
    assertTrue(request.isDeferVersionSwap());
    assertEquals(request.getTargetedRegions(), "regionA,regionB");
    assertEquals(request.getRepushSourceVersion(), 42);
    assertEquals(request.getPartitioners(), new HashSet<>(Arrays.asList("partitioner1", "partitioner2")));
    assertEquals(request.getCompressionDictionary(), "compressionDict");
    assertEquals(request.getEmergencySourceRegion(), "regionX");
    assertTrue(request.isSeparateRealTimeTopicEnabled());
    assertEquals(request.getCertificateInRequest(), x509Certificate);
  }

  @Test
  public void testSetPartitionersValidAndEmptyCases() {
    // Valid partitioners
    request.setPartitioners("partitioner1");
    assertEquals(request.getPartitioners(), new HashSet<>(Collections.singletonList("partitioner1")));
    request.setPartitioners("partitioner1,partitioner2");
    assertEquals(request.getPartitioners(), new HashSet<>(Arrays.asList("partitioner1", "partitioner2")));

    // Empty set
    request.setPartitioners(Collections.emptySet());
    assertEquals(request.getPartitioners(), Collections.emptySet());

    // Null and empty string
    request.setPartitioners((String) null);
    assertEquals(request.getPartitioners(), Collections.emptySet());

    request.setPartitioners("");
    assertEquals(request.getPartitioners(), Collections.emptySet());
  }
}
