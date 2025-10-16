package com.linkedin.venice.hooks;

import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.protocols.hooks.GrpcStoreLifecycleHookServiceGrpc;
import com.linkedin.venice.protocols.hooks.GrpcStoreLifecycleHooksRequest;
import com.linkedin.venice.protocols.hooks.GrpcStoreLifecycleHooksResponse;
import com.linkedin.venice.protocols.hooks.StoreVersionLifecycleEventOutcomeProto;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.lazy.Lazy;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A hook implementation that calls a gRPC service for store lifecycle events.
 * Uses static caching to maintain state across multiple instances created through reflection.
 */
public class GrpcStoreLifecycleHooks extends StoreLifecycleHooks implements Closeable {
  private static final Logger LOGGER = LogManager.getLogger(GrpcStoreLifecycleHooks.class);
  private static final String GRPC_LIFECYCLE_HOOK_CONFIGS_PREFIX = "grpc.lifecycle.hooks.configs.";
  private static final String GRPC_LIFECYCLE_HOOK_CONFIGS_CHANNEL = "channel";

  // Instance-level caches - package-private for testing
  private final VeniceConcurrentHashMap<String, ManagedChannel> channelCache = new VeniceConcurrentHashMap<>();
  private final VeniceConcurrentHashMap<String, GrpcStoreLifecycleHookServiceGrpc.GrpcStoreLifecycleHookServiceStub> stubCache =
      new VeniceConcurrentHashMap<>();
  private final VeniceConcurrentHashMap<String, CompletableFuture<StoreVersionLifecycleEventOutcome>> pendingCalls =
      new VeniceConcurrentHashMap<>();

  public GrpcStoreLifecycleHooks(VeniceProperties defaultConfigs) {
    super(defaultConfigs);
    LOGGER.info("Created new GrpcStoreLifecycleHook instance created");
  }

  @Override
  public StoreLifecycleEventOutcome validateHookParams(
      String clusterName,
      String storeName,
      Map<String, String> hookParams) {
    if (hookParams == null) {
      LOGGER.error("Hook params cannot be null for store {}", storeName);
      return StoreLifecycleEventOutcome.ABORT;
    }

    // Parse all gRPC-related configs with prefix
    Map<String, String> grpcConfigs = parseParamsWithPrefix(GRPC_LIFECYCLE_HOOK_CONFIGS_PREFIX, hookParams);

    if (grpcConfigs.isEmpty() || !grpcConfigs.containsKey(GRPC_LIFECYCLE_HOOK_CONFIGS_CHANNEL)) {
      LOGGER.error(
          "Missing required gRPC config for store {}, need channel config with prefix {}",
          storeName,
          GRPC_LIFECYCLE_HOOK_CONFIGS_PREFIX);
      return StoreLifecycleEventOutcome.ABORT;
    }
    LOGGER.info("Successfully validated gRPC hook params for store {}", storeName);
    return StoreLifecycleEventOutcome.PROCEED;
  }

  @Override
  public StoreVersionLifecycleEventOutcome postStoreVersionSwap(
      String clusterName,
      String storeName,
      int versionNumber,
      String regionName,
      Lazy<JobStatusQueryResponse> jobStatus,
      VeniceProperties storeHooksConfigs) {

    // Create a unique key for this async call
    String callKey = generateAsyncCallKey(clusterName, storeName, versionNumber, regionName);

    // Check if we have a pending or completed call for this request
    CompletableFuture<StoreVersionLifecycleEventOutcome> existingCall = pendingCalls.get(callKey);
    if (existingCall != null) {
      if (existingCall.isDone()) {
        // Call is complete, return the result and clean up
        try {
          StoreVersionLifecycleEventOutcome result = existingCall.get();
          pendingCalls.remove(callKey);
          LOGGER.info(
              "Async gRPC call completed for store {}, region {}, version {} with outcome: {}",
              storeName,
              regionName,
              versionNumber,
              result);
          return result;
        } catch (Exception e) {
          LOGGER.error("Error getting result from completed async call: {}", e.getMessage(), e);
          pendingCalls.remove(callKey);
          return StoreVersionLifecycleEventOutcome.PROCEED;
        }
      } else {
        // Call is still in progress, return WAIT
        LOGGER.debug(
            "Async gRPC call still in progress for store {}, region {}, version {}",
            storeName,
            regionName,
            versionNumber);
        return StoreVersionLifecycleEventOutcome.WAIT;
      }
    }

    // This is a new call, start the async gRPC request
    return initiateAsyncGrpcCall(callKey, clusterName, storeName, versionNumber, regionName, storeHooksConfigs);
  }

  /**
   * Initiates a new async gRPC call and returns WAIT immediately.
   */
  private StoreVersionLifecycleEventOutcome initiateAsyncGrpcCall(
      String callKey,
      String clusterName,
      String storeName,
      int versionNumber,
      String regionName,
      VeniceProperties storeHooksConfigs) {

    // Parse all gRPC-related configs with prefix
    Map<String, String> grpcConfigs =
        parseParamsWithPrefix(GRPC_LIFECYCLE_HOOK_CONFIGS_PREFIX, storeHooksConfigs.getAsMap());

    if (grpcConfigs.isEmpty()) {
      LOGGER.warn(
          "No gRPC config found for store {}, region {}, version {}. Proceeding with default outcome.",
          storeName,
          regionName,
          versionNumber);
      return StoreVersionLifecycleEventOutcome.PROCEED;
    }

    String channelTarget = grpcConfigs.get(GRPC_LIFECYCLE_HOOK_CONFIGS_CHANNEL);

    if (channelTarget == null) {
      LOGGER.error(
          "Missing required gRPC config for store {}, region {}, version {}. Need channel config.",
          storeName,
          regionName,
          versionNumber);
      return StoreVersionLifecycleEventOutcome.PROCEED;
    }

    try {
      // Get or create the async stub for this service
      GrpcStoreLifecycleHookServiceGrpc.GrpcStoreLifecycleHookServiceStub stub = getOrCreateAsyncStub(channelTarget);

      // Create the request
      GrpcStoreLifecycleHooksRequest request = GrpcStoreLifecycleHooksRequest.newBuilder()
          .setStoreName(storeName)
          .setRegion(regionName)
          .setVersion(versionNumber)
          .build();

      // Create a CompletableFuture to track the async call
      CompletableFuture<StoreVersionLifecycleEventOutcome> future = new CompletableFuture<>();

      // Create StreamObserver to handle the response
      StreamObserver<GrpcStoreLifecycleHooksResponse> responseObserver =
          new StreamObserver<GrpcStoreLifecycleHooksResponse>() {
            @Override
            public void onNext(GrpcStoreLifecycleHooksResponse response) {
              StoreVersionLifecycleEventOutcome outcome = mapProtoToJavaEnum(response.getOutcome());
              future.complete(outcome);
            }

            @Override
            public void onError(Throwable throwable) {
              LOGGER.error(
                  "gRPC call failed for store {}, region {}, version {}: {}",
                  storeName,
                  regionName,
                  versionNumber,
                  throwable.getMessage(),
                  throwable);
              future.complete(StoreVersionLifecycleEventOutcome.PROCEED);
            }

            @Override
            public void onCompleted() {
              // Response already handled in onNext
            }
          };

      // Store the future in our pending calls cache
      pendingCalls.put(callKey, future);

      // Make the async call using the generated stub method
      stub.postVersionSwap(request, responseObserver);

      LOGGER.info(
          "Initiated async gRPC call for store {}, region {}, version {}. Returning WAIT.",
          storeName,
          regionName,
          versionNumber);

      // Return WAIT to indicate the call is in progress
      return StoreVersionLifecycleEventOutcome.WAIT;

    } catch (Exception e) {
      LOGGER.error(
          "Error initiating async gRPC call for store {}, region {}, version {}: {}",
          storeName,
          regionName,
          versionNumber,
          e.getMessage(),
          e);
      return StoreVersionLifecycleEventOutcome.PROCEED;
    }
  }

  /**
   * Generates a unique key for tracking async calls.
   */
  private String generateAsyncCallKey(String clusterName, String storeName, int versionNumber, String regionName) {
    return String.format("%s#%s#%d#%s", clusterName, storeName, versionNumber, regionName);
  }

  /**
   * Maps proto enum to Java enum.
   */
  StoreVersionLifecycleEventOutcome mapProtoToJavaEnum(StoreVersionLifecycleEventOutcomeProto outcomeProto) {
    switch (outcomeProto) {
      case PROCEED:
        return StoreVersionLifecycleEventOutcome.PROCEED;
      case ABORT:
        return StoreVersionLifecycleEventOutcome.ABORT;
      case WAIT:
        return StoreVersionLifecycleEventOutcome.WAIT;
      case ROLLBACK:
        return StoreVersionLifecycleEventOutcome.ROLLBACK;
      default:
        LOGGER.warn("Received UNSPECIFIED outcome from gRPC service, defaulting to PROCEED");
        return StoreVersionLifecycleEventOutcome.PROCEED;
    }
  }

  /**
   * Gets or creates an async stub for the given channel target.
   * Uses the cached stub if available, otherwise creates a new one.
   * Note: No synchronization needed since DeferredVersionSwapService is single-threaded.
   */
  private GrpcStoreLifecycleHookServiceGrpc.GrpcStoreLifecycleHookServiceStub getOrCreateAsyncStub(
      String channelTarget) {
    // Try to get from cache first
    GrpcStoreLifecycleHookServiceGrpc.GrpcStoreLifecycleHookServiceStub stub = stubCache.get(channelTarget);
    if (stub != null && isChannelHealthy((ManagedChannel) stub.getChannel())) {
      return stub;
    }

    // Get or create the channel
    ManagedChannel channel = channelCache.computeIfAbsent(
        channelTarget,
        target -> ManagedChannelBuilder.forTarget(target).usePlaintext().keepAliveTime(2, TimeUnit.HOURS).build());

    // Connection could've been closed by server, verify if the channel is still healthy
    if (!isChannelHealthy(channel)) {
      channelCache.remove(channelTarget, channel);
      if (channel != null) {
        channel.shutdown();
      }
      ManagedChannel newChannel =
          ManagedChannelBuilder.forTarget(channelTarget).usePlaintext().keepAliveTime(2, TimeUnit.HOURS).build();
      channelCache.put(channelTarget, newChannel);
    }

    // Create new async stub using the generated service class
    stub = GrpcStoreLifecycleHookServiceGrpc.newStub(channel);

    // Store in cache
    stubCache.put(channelTarget, stub);
    return stub;
  }

  private Map<String, String> parseParamsWithPrefix(String prefix, Map<String, String> params) {
    if (params == null || params.isEmpty()) {
      return Collections.emptyMap();
    }

    Map<String, String> parsedParams = new HashMap<>();
    for (Map.Entry<String, String> entry: params.entrySet()) {
      if (entry.getKey().startsWith(prefix)) {
        String newKey = entry.getKey().substring(prefix.length());
        parsedParams.put(newKey, entry.getValue());
      }
    }
    return parsedParams;
  }

  private boolean isChannelHealthy(ManagedChannel channel) {
    try {
      return !channel.isShutdown() && !channel.isTerminated();
    } catch (Exception e) {
      LOGGER.warn("Error checking channel {} health", channel, e);
      return false;
    }
  }

  @Override
  public void close() throws IOException {
    for (Map.Entry<String, ManagedChannel> entry: channelCache.entrySet()) {
      entry.getValue().shutdown();
    }
  }

  // For testing
  VeniceConcurrentHashMap<String, CompletableFuture<StoreVersionLifecycleEventOutcome>> getPendingCalls() {
    return pendingCalls;
  }
}
