package com.linkedin.venice.hooks;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.protocols.hooks.StoreVersionLifecycleEventOutcomeProto;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.lazy.Lazy;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mockito;
import org.testng.annotations.Test;


public class GrpcStoreLifecycleHooksTest {
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

    GrpcStoreLifecycleHooks hook = new GrpcStoreLifecycleHooks(defaultConfigs);

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
  public void testValidChannelConfigurationInitiatesAsyncCall() {
    VeniceProperties defaultConfigs = new VeniceProperties(new HashMap<>());

    Map<String, String> configMap = new HashMap<>();
    configMap.put("grpc.lifecycle.hooks.configs.channel", "localhost:50051");
    VeniceProperties validConfig = new VeniceProperties(configMap);

    Lazy<JobStatusQueryResponse> mockJobStatus = Mockito.mock(Lazy.class);

    GrpcStoreLifecycleHooks hook = new GrpcStoreLifecycleHooks(defaultConfigs);

    StoreVersionLifecycleEventOutcome result = hook.postStoreVersionSwap(
        TEST_CLUSTER_NAME,
        TEST_STORE_NAME,
        TEST_VERSION_NUMBER,
        TEST_REGION_NAME,
        mockJobStatus,
        validConfig);

    // Should return WAIT because the async gRPC call is initiated successfully
    assertEquals(result, StoreVersionLifecycleEventOutcome.WAIT);
  }

  @Test
  public void testChannelConfigWithExtraPropertiesInitiatesAsyncCall() {
    VeniceProperties defaultConfigs = new VeniceProperties(new HashMap<>());

    // Provide channel config with extra properties (should be ignored)
    Map<String, String> configMap = new HashMap<>();
    configMap.put("grpc.lifecycle.hooks.configs.channel", "localhost:50051");
    configMap.put("grpc.lifecycle.hooks.configs.extra.property", "ignored");
    VeniceProperties configWithExtras = new VeniceProperties(configMap);

    Lazy<JobStatusQueryResponse> mockJobStatus = Mockito.mock(Lazy.class);

    GrpcStoreLifecycleHooks hook = new GrpcStoreLifecycleHooks(defaultConfigs);

    StoreVersionLifecycleEventOutcome result = hook.postStoreVersionSwap(
        TEST_CLUSTER_NAME,
        TEST_STORE_NAME,
        TEST_VERSION_NUMBER,
        TEST_REGION_NAME,
        mockJobStatus,
        configWithExtras);

    // Should return WAIT because the async gRPC call is initiated successfully
    assertEquals(result, StoreVersionLifecycleEventOutcome.WAIT);
  }

  @Test
  public void testMultipleInstancesWithDifferentConfigs() {
    VeniceProperties defaultConfigs1 = new VeniceProperties(new HashMap<>());
    VeniceProperties defaultConfigs2 = new VeniceProperties(new HashMap<>());

    GrpcStoreLifecycleHooks hook1 = new GrpcStoreLifecycleHooks(defaultConfigs1);
    GrpcStoreLifecycleHooks hook2 = new GrpcStoreLifecycleHooks(defaultConfigs2);

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

    // Both should return PROCEED because no gRPC config is provided
    assertEquals(result1, StoreVersionLifecycleEventOutcome.PROCEED);
    assertEquals(result2, StoreVersionLifecycleEventOutcome.PROCEED);
  }

  @Test
  public void testInvalidChannelTargetInitiatesAsyncCall() {
    VeniceProperties defaultConfigs = new VeniceProperties(new HashMap<>());

    // Provide invalid channel target
    Map<String, String> configMap = new HashMap<>();
    configMap.put("grpc.lifecycle.hooks.configs.channel", "invalid-channel-target:99999");
    VeniceProperties invalidConfig = new VeniceProperties(configMap);

    Lazy<JobStatusQueryResponse> mockJobStatus = Mockito.mock(Lazy.class);

    GrpcStoreLifecycleHooks hook = new GrpcStoreLifecycleHooks(defaultConfigs);

    StoreVersionLifecycleEventOutcome result = hook.postStoreVersionSwap(
        TEST_CLUSTER_NAME,
        TEST_STORE_NAME,
        TEST_VERSION_NUMBER,
        TEST_REGION_NAME,
        mockJobStatus,
        invalidConfig);

    // Should return WAIT because the async gRPC call is initiated (even though it will eventually fail)
    assertEquals(result, StoreVersionLifecycleEventOutcome.WAIT);
  }

  @Test
  public void testDifferentStoreNames() {
    VeniceProperties defaultConfigs = new VeniceProperties(new HashMap<>());
    VeniceProperties emptyConfig = new VeniceProperties(new HashMap<>());
    Lazy<JobStatusQueryResponse> mockJobStatus = Mockito.mock(Lazy.class);

    GrpcStoreLifecycleHooks hook = new GrpcStoreLifecycleHooks(defaultConfigs);

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

    GrpcStoreLifecycleHooks hook = new GrpcStoreLifecycleHooks(defaultConfigs);

    // Test with different region names - should all return PROCEED for empty config
    StoreVersionLifecycleEventOutcome result1 =
        hook.postStoreVersionSwap(TEST_CLUSTER_NAME, TEST_STORE_NAME, 1, TEST_REGION_NAME, mockJobStatus, emptyConfig);
    StoreVersionLifecycleEventOutcome result2 =
        hook.postStoreVersionSwap(TEST_CLUSTER_NAME, TEST_STORE_NAME, 1, TEST_REGION_NAME2, mockJobStatus, emptyConfig);

    assertEquals(result1, StoreVersionLifecycleEventOutcome.PROCEED);
    assertEquals(result2, StoreVersionLifecycleEventOutcome.PROCEED);
  }

  @Test
  public void testMapProtoToJavaEnum() throws Exception {
    GrpcStoreLifecycleHooks hook = new GrpcStoreLifecycleHooks(new VeniceProperties(new HashMap<>()));

    // Test all enum mappings
    assertEquals(
        hook.mapProtoToJavaEnum(StoreVersionLifecycleEventOutcomeProto.PROCEED),
        StoreVersionLifecycleEventOutcome.PROCEED);

    assertEquals(
        hook.mapProtoToJavaEnum(StoreVersionLifecycleEventOutcomeProto.ABORT),
        StoreVersionLifecycleEventOutcome.ABORT);

    assertEquals(
        hook.mapProtoToJavaEnum(StoreVersionLifecycleEventOutcomeProto.WAIT),
        StoreVersionLifecycleEventOutcome.WAIT);

    assertEquals(
        hook.mapProtoToJavaEnum(StoreVersionLifecycleEventOutcomeProto.ROLLBACK),
        StoreVersionLifecycleEventOutcome.ROLLBACK);
  }

  @Test
  public void testAsyncCallCompletionWithSuccessfulResult() throws Exception {
    GrpcStoreLifecycleHooks hook = new GrpcStoreLifecycleHooks(new VeniceProperties(new HashMap<>()));
    Lazy<JobStatusQueryResponse> mockJobStatus = Mockito.mock(Lazy.class);

    Map<String, String> configMap = new HashMap<>();
    configMap.put("grpc.lifecycle.hooks.configs.channel", "localhost:50051");
    VeniceProperties validConfig = new VeniceProperties(configMap);

    // First call initiates the async call
    StoreVersionLifecycleEventOutcome result1 = hook.postStoreVersionSwap(
        TEST_CLUSTER_NAME,
        TEST_STORE_NAME,
        TEST_VERSION_NUMBER,
        TEST_REGION_NAME,
        mockJobStatus,
        validConfig);
    assertEquals(result1, StoreVersionLifecycleEventOutcome.WAIT);

    // Simulate the async call completing with a result
    String callKey = generateCallKey(TEST_CLUSTER_NAME, TEST_STORE_NAME, TEST_VERSION_NUMBER, TEST_REGION_NAME);
    CompletableFuture<StoreVersionLifecycleEventOutcome> future = hook.getPendingCalls().get(callKey);
    ;
    assertNotNull(future);
    future.complete(StoreVersionLifecycleEventOutcome.ABORT); // Simulate completion with ABORT

    // Second call should return the completed result and clean up
    StoreVersionLifecycleEventOutcome result2 = hook.postStoreVersionSwap(
        TEST_CLUSTER_NAME,
        TEST_STORE_NAME,
        TEST_VERSION_NUMBER,
        TEST_REGION_NAME,
        mockJobStatus,
        validConfig);
    assertEquals(result2, StoreVersionLifecycleEventOutcome.ABORT);

    // Third call should initiate a new async call since the previous one was cleaned up
    StoreVersionLifecycleEventOutcome result3 = hook.postStoreVersionSwap(
        TEST_CLUSTER_NAME,
        TEST_STORE_NAME,
        TEST_VERSION_NUMBER,
        TEST_REGION_NAME,
        mockJobStatus,
        validConfig);
    assertEquals(result3, StoreVersionLifecycleEventOutcome.WAIT);
  }

  @Test
  public void testAsyncCallCompletionWithException() throws Exception {
    GrpcStoreLifecycleHooks hook = new GrpcStoreLifecycleHooks(new VeniceProperties(new HashMap<>()));
    Lazy<JobStatusQueryResponse> mockJobStatus = Mockito.mock(Lazy.class);

    Map<String, String> configMap = new HashMap<>();
    configMap.put("grpc.lifecycle.hooks.configs.channel", "localhost:50051");
    VeniceProperties validConfig = new VeniceProperties(configMap);

    // First call initiates the async call
    StoreVersionLifecycleEventOutcome result1 = hook.postStoreVersionSwap(
        TEST_CLUSTER_NAME,
        TEST_STORE_NAME,
        TEST_VERSION_NUMBER,
        TEST_REGION_NAME,
        mockJobStatus,
        validConfig);
    assertEquals(result1, StoreVersionLifecycleEventOutcome.WAIT);

    // Simulate the async call completing with an exception
    String callKey = generateCallKey(TEST_CLUSTER_NAME, TEST_STORE_NAME, TEST_VERSION_NUMBER, TEST_REGION_NAME);
    CompletableFuture<StoreVersionLifecycleEventOutcome> future = hook.getPendingCalls().get(callKey);
    assertNotNull(future);
    future.completeExceptionally(new RuntimeException("Test exception"));

    // Second call should return PROCEED (default for errors) and clean up
    StoreVersionLifecycleEventOutcome result2 = hook.postStoreVersionSwap(
        TEST_CLUSTER_NAME,
        TEST_STORE_NAME,
        TEST_VERSION_NUMBER,
        TEST_REGION_NAME,
        mockJobStatus,
        validConfig);
    assertEquals(result2, StoreVersionLifecycleEventOutcome.PROCEED);

    // Third call should initiate a new async call since the previous one was cleaned up
    StoreVersionLifecycleEventOutcome result3 = hook.postStoreVersionSwap(
        TEST_CLUSTER_NAME,
        TEST_STORE_NAME,
        TEST_VERSION_NUMBER,
        TEST_REGION_NAME,
        mockJobStatus,
        validConfig);
    assertEquals(result3, StoreVersionLifecycleEventOutcome.WAIT);
  }

  @Test
  public void testAsyncCallInProgress() throws Exception {
    GrpcStoreLifecycleHooks hook = new GrpcStoreLifecycleHooks(new VeniceProperties(new HashMap<>()));
    Lazy<JobStatusQueryResponse> mockJobStatus = Mockito.mock(Lazy.class);

    Map<String, String> configMap = new HashMap<>();
    configMap.put("grpc.lifecycle.hooks.configs.channel", "localhost:50051");
    VeniceProperties validConfig = new VeniceProperties(configMap);

    // First call initiates the async call
    StoreVersionLifecycleEventOutcome result1 = hook.postStoreVersionSwap(
        TEST_CLUSTER_NAME,
        TEST_STORE_NAME,
        TEST_VERSION_NUMBER,
        TEST_REGION_NAME,
        mockJobStatus,
        validConfig);
    assertEquals(result1, StoreVersionLifecycleEventOutcome.WAIT);

    // Second call should return WAIT since the call is still in progress
    StoreVersionLifecycleEventOutcome result2 = hook.postStoreVersionSwap(
        TEST_CLUSTER_NAME,
        TEST_STORE_NAME,
        TEST_VERSION_NUMBER,
        TEST_REGION_NAME,
        mockJobStatus,
        validConfig);
    assertEquals(result2, StoreVersionLifecycleEventOutcome.WAIT);

    // Third call should also return WAIT
    StoreVersionLifecycleEventOutcome result3 = hook.postStoreVersionSwap(
        TEST_CLUSTER_NAME,
        TEST_STORE_NAME,
        TEST_VERSION_NUMBER,
        TEST_REGION_NAME,
        mockJobStatus,
        validConfig);
    assertEquals(result3, StoreVersionLifecycleEventOutcome.WAIT);
  }

  @Test
  public void testValidateHookParamsWithNullParams() {
    VeniceProperties defaultConfigs = new VeniceProperties(new HashMap<>());
    GrpcStoreLifecycleHooks hook = new GrpcStoreLifecycleHooks(defaultConfigs);

    StoreLifecycleEventOutcome result = hook.validateHookParams(TEST_CLUSTER_NAME, TEST_STORE_NAME, null);
    assertEquals(result, StoreLifecycleEventOutcome.ABORT);
  }

  @Test
  public void testValidateHookParamsWithEmptyParams() {
    VeniceProperties defaultConfigs = new VeniceProperties(new HashMap<>());
    GrpcStoreLifecycleHooks hook = new GrpcStoreLifecycleHooks(defaultConfigs);

    Map<String, String> emptyParams = new HashMap<>();
    StoreLifecycleEventOutcome result = hook.validateHookParams(TEST_CLUSTER_NAME, TEST_STORE_NAME, emptyParams);
    assertEquals(result, StoreLifecycleEventOutcome.ABORT);
  }

  @Test
  public void testValidateHookParamsWithMissingChannelConfig() {
    VeniceProperties defaultConfigs = new VeniceProperties(new HashMap<>());
    GrpcStoreLifecycleHooks hook = new GrpcStoreLifecycleHooks(defaultConfigs);

    Map<String, String> paramsWithoutChannel = new HashMap<>();
    paramsWithoutChannel.put("grpc.lifecycle.hooks.configs.other", "value");
    StoreLifecycleEventOutcome result =
        hook.validateHookParams(TEST_CLUSTER_NAME, TEST_STORE_NAME, paramsWithoutChannel);
    assertEquals(result, StoreLifecycleEventOutcome.ABORT);
  }

  @Test
  public void testValidateHookParamsWithValidConfig() {
    VeniceProperties defaultConfigs = new VeniceProperties(new HashMap<>());
    GrpcStoreLifecycleHooks hook = new GrpcStoreLifecycleHooks(defaultConfigs);

    Map<String, String> validParams = new HashMap<>();
    validParams.put("grpc.lifecycle.hooks.configs.channel", "localhost:50051");
    StoreLifecycleEventOutcome result = hook.validateHookParams(TEST_CLUSTER_NAME, TEST_STORE_NAME, validParams);
    assertEquals(result, StoreLifecycleEventOutcome.PROCEED);
  }

  private String generateCallKey(String clusterName, String storeName, int versionNumber, String regionName) {
    return String.format("%s#%s#%d#%s", clusterName, storeName, versionNumber, regionName);
  }
}
