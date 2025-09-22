package com.linkedin.venice.hooks;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.lazy.Lazy;
import java.util.HashMap;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.Test;


public class GrpcStoreLifecycleHookTest {
  private static final String TEST_STORE_NAME = "testStore";
  private static final String TEST_CLUSTER_NAME = "testCluster";
  private static final String TEST_REGION_NAME = "region1";
  private static final String TEST_REGION_NAME2 = "region2";
  private static final int TEST_VERSION_NUMBER = 1;

  @Test
  public void testMissingConfigurationReturnsProceed() {
    VeniceProperties defaultConfigs = new VeniceProperties(new HashMap<>());
    VeniceProperties emptyStoreConfig = new VeniceProperties(new HashMap<>());
    Lazy<JobStatusQueryResponse> mockJobStatus = Mockito.mock(Lazy.class);

    GrpcStoreLifecycleHook hook = new GrpcStoreLifecycleHook(defaultConfigs);

    // Should return PROCEED immediately when config is missing
    StoreVersionLifecycleEventOutcome result = hook.postStoreVersionSwap(
        TEST_CLUSTER_NAME,
        TEST_STORE_NAME,
        TEST_VERSION_NUMBER,
        TEST_REGION_NAME,
        mockJobStatus,
        emptyStoreConfig);

    assertEquals(result, StoreVersionLifecycleEventOutcome.PROCEED);
  }

  @Test
  public void testPartialConfigurationReturnsProceed() {
    VeniceProperties defaultConfigs = new VeniceProperties(new HashMap<>());

    // Only provide channel config, missing stub and method
    Map<String, String> configMap = new HashMap<>();
    configMap.put("grpc.lifecycle.hooks.configs.channel", "localhost:50051");
    VeniceProperties partialConfig = new VeniceProperties(configMap);

    Lazy<JobStatusQueryResponse> mockJobStatus = Mockito.mock(Lazy.class);

    GrpcStoreLifecycleHook hook = new GrpcStoreLifecycleHook(defaultConfigs);

    StoreVersionLifecycleEventOutcome result = hook.postStoreVersionSwap(
        TEST_CLUSTER_NAME,
        TEST_STORE_NAME,
        TEST_VERSION_NUMBER,
        TEST_REGION_NAME,
        mockJobStatus,
        partialConfig);

    assertEquals(result, StoreVersionLifecycleEventOutcome.PROCEED);
  }

  @Test
  public void testMissingStubConfigurationReturnsProceed() {
    VeniceProperties defaultConfigs = new VeniceProperties(new HashMap<>());

    // Provide channel and method but missing stub
    Map<String, String> configMap = new HashMap<>();
    configMap.put("grpc.lifecycle.hooks.configs.channel", "localhost:50051");
    configMap.put("grpc.lifecycle.hooks.configs.method", "runHook");
    VeniceProperties partialConfig = new VeniceProperties(configMap);

    Lazy<JobStatusQueryResponse> mockJobStatus = Mockito.mock(Lazy.class);

    GrpcStoreLifecycleHook hook = new GrpcStoreLifecycleHook(defaultConfigs);

    StoreVersionLifecycleEventOutcome result = hook.postStoreVersionSwap(
        TEST_CLUSTER_NAME,
        TEST_STORE_NAME,
        TEST_VERSION_NUMBER,
        TEST_REGION_NAME,
        mockJobStatus,
        partialConfig);

    assertEquals(result, StoreVersionLifecycleEventOutcome.PROCEED);
  }

  @Test
  public void testMultipleInstancesWithDifferentConfigs() {
    VeniceProperties defaultConfigs1 = new VeniceProperties(new HashMap<>());
    VeniceProperties defaultConfigs2 = new VeniceProperties(new HashMap<>());

    GrpcStoreLifecycleHook hook1 = new GrpcStoreLifecycleHook(defaultConfigs1);
    GrpcStoreLifecycleHook hook2 = new GrpcStoreLifecycleHook(defaultConfigs2);

    assertNotNull(hook1);
    assertNotNull(hook2);

    // Different instances should work independently for config handling
    VeniceProperties emptyConfig = new VeniceProperties(new HashMap<>());
    Lazy<JobStatusQueryResponse> mockJobStatus = Mockito.mock(Lazy.class);

    StoreVersionLifecycleEventOutcome result1 =
        hook1.postStoreVersionSwap(TEST_CLUSTER_NAME, TEST_STORE_NAME, 1, TEST_REGION_NAME, mockJobStatus, emptyConfig);

    StoreVersionLifecycleEventOutcome result2 = hook2.postStoreVersionSwap(
        TEST_CLUSTER_NAME,
        TEST_STORE_NAME,
        2, // Different version to avoid cache interference
        TEST_REGION_NAME,
        mockJobStatus,
        emptyConfig);

    assertEquals(result1, StoreVersionLifecycleEventOutcome.PROCEED);
    assertEquals(result2, StoreVersionLifecycleEventOutcome.PROCEED);
  }

  @Test
  public void testInvalidStubClassNameReturnsProceed() {
    VeniceProperties defaultConfigs = new VeniceProperties(new HashMap<>());

    // Provide all required configs but with invalid stub class
    Map<String, String> configMap = new HashMap<>();
    configMap.put("grpc.lifecycle.hooks.configs.channel", "localhost:50051");
    configMap.put("grpc.lifecycle.hooks.configs.stub", "com.invalid.NonExistentServiceGrpc");
    configMap.put("grpc.lifecycle.hooks.configs.method", "runHook");
    VeniceProperties invalidConfig = new VeniceProperties(configMap);

    Lazy<JobStatusQueryResponse> mockJobStatus = Mockito.mock(Lazy.class);

    GrpcStoreLifecycleHook hook = new GrpcStoreLifecycleHook(defaultConfigs);

    StoreVersionLifecycleEventOutcome result = hook.postStoreVersionSwap(
        TEST_CLUSTER_NAME,
        TEST_STORE_NAME,
        TEST_VERSION_NUMBER,
        TEST_REGION_NAME,
        mockJobStatus,
        invalidConfig);

    // Should return PROCEED as the error handling defaults to PROCEED
    assertEquals(result, StoreVersionLifecycleEventOutcome.PROCEED);
  }

  @Test
  public void testDifferentStoreNames() {
    VeniceProperties defaultConfigs = new VeniceProperties(new HashMap<>());
    VeniceProperties emptyConfig = new VeniceProperties(new HashMap<>());
    Lazy<JobStatusQueryResponse> mockJobStatus = Mockito.mock(Lazy.class);

    GrpcStoreLifecycleHook hook = new GrpcStoreLifecycleHook(defaultConfigs);

    // Test with different store names - should all return PROCEED for empty config
    StoreVersionLifecycleEventOutcome result1 =
        hook.postStoreVersionSwap(TEST_CLUSTER_NAME, "store1", 1, TEST_REGION_NAME, mockJobStatus, emptyConfig);
    StoreVersionLifecycleEventOutcome result2 =
        hook.postStoreVersionSwap(TEST_CLUSTER_NAME, "store2", 1, TEST_REGION_NAME, mockJobStatus, emptyConfig);
    StoreVersionLifecycleEventOutcome result3 =
        hook.postStoreVersionSwap(TEST_CLUSTER_NAME, "store3", 1, TEST_REGION_NAME, mockJobStatus, emptyConfig);

    assertEquals(result1, StoreVersionLifecycleEventOutcome.PROCEED);
    assertEquals(result2, StoreVersionLifecycleEventOutcome.PROCEED);
    assertEquals(result3, StoreVersionLifecycleEventOutcome.PROCEED);
  }

  @Test
  public void testDifferentRegionNames() {
    VeniceProperties defaultConfigs = new VeniceProperties(new HashMap<>());
    VeniceProperties emptyConfig = new VeniceProperties(new HashMap<>());
    Lazy<JobStatusQueryResponse> mockJobStatus = Mockito.mock(Lazy.class);

    GrpcStoreLifecycleHook hook = new GrpcStoreLifecycleHook(defaultConfigs);

    // Test with different region names - should all return PROCEED for empty config
    StoreVersionLifecycleEventOutcome result1 =
        hook.postStoreVersionSwap(TEST_CLUSTER_NAME, TEST_STORE_NAME, 1, TEST_REGION_NAME, mockJobStatus, emptyConfig);
    StoreVersionLifecycleEventOutcome result2 =
        hook.postStoreVersionSwap(TEST_CLUSTER_NAME, TEST_STORE_NAME, 1, TEST_REGION_NAME2, mockJobStatus, emptyConfig);

    assertEquals(result1, StoreVersionLifecycleEventOutcome.PROCEED);
    assertEquals(result2, StoreVersionLifecycleEventOutcome.PROCEED);
  }
}
