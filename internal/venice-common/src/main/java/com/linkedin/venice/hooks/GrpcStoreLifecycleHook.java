package com.linkedin.venice.hooks;

import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.protocols.hooks.GrpcStoreLifecycleHooksRequest;
import com.linkedin.venice.protocols.hooks.GrpcStoreLifecycleHooksResponse;
import com.linkedin.venice.protocols.hooks.StoreVersionLifecycleEventOutcomeProto;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.lazy.Lazy;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.AbstractAsyncStub;
import io.grpc.stub.StreamObserver;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
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
public class GrpcStoreLifecycleHook extends StoreLifecycleHooks implements Closeable {
  private static final Logger LOGGER = LogManager.getLogger(GrpcStoreLifecycleHook.class);
  private static final String GRPC_LIFECYCLE_HOOK_CONFIGS_PREFIX = "grpc.lifecycle.hooks.configs.";
  private static final String GRPC_LIFECYCLE_HOOK_CONFIGS_CHANNEL = "channel";
  private static final String GRPC_LIFECYCLE_HOOK_CONFIGS_STUB = "stub";
  private static final String GRPC_LIFECYCLE_HOOK_CONFIGS_METHOD = "method";

  // Static caches shared across all instances
  private static final VeniceConcurrentHashMap<String, ManagedChannel> channelCache = new VeniceConcurrentHashMap<>();
  private static final VeniceConcurrentHashMap<String, AbstractAsyncStub<?>> stubCache =
      new VeniceConcurrentHashMap<>();
  private static final VeniceConcurrentHashMap<String, Class<?>> stubClassCache = new VeniceConcurrentHashMap<>();
  private static final VeniceConcurrentHashMap<String, CompletableFuture<StoreVersionLifecycleEventOutcome>> pendingCalls =
      new VeniceConcurrentHashMap<>();

  private static volatile boolean shutdownHookRegistered = false;

  // Static initializer to register shutdown hook
  static {
    synchronized (GrpcStoreLifecycleHook.class) {
      if (!shutdownHookRegistered) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
          LOGGER.info("JVM shutdown hook triggered - cleaning up GrpcStoreLifecycleHook static resources");
          cleanupStaticResources();
        }));
        shutdownHookRegistered = true;
        LOGGER.info("Registered JVM shutdown hook for GrpcStoreLifecycleHook");
      }
    }
  }

  /**
   * Performs final cleanup of all static resources when JVM is shutting down
   */
  private static void cleanupStaticResources() {
    // Shutdown all channels
    for (ManagedChannel channel: channelCache.values()) {
      try {
        // On JVM shutdown, don't wait too long
        channel.shutdown().awaitTermination(1, TimeUnit.SECONDS);
        if (!channel.isShutdown()) {
          channel.shutdownNow();
        }
      } catch (Exception e) {
        LOGGER.warn("Error shutting down gRPC channel during JVM exit: {}", e.getMessage());
      }
    }

    // Clear all caches
    channelCache.clear();
    stubCache.clear();
    stubClassCache.clear();
    pendingCalls.clear();
  }

  public GrpcStoreLifecycleHook(VeniceProperties defaultConfigs) {
    super(defaultConfigs);
    LOGGER.debug("New GrpcStoreLifecycleHook instance created");
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
    String stubClassName = grpcConfigs.get(GRPC_LIFECYCLE_HOOK_CONFIGS_STUB);
    String method = grpcConfigs.get(GRPC_LIFECYCLE_HOOK_CONFIGS_METHOD);

    if (channelTarget == null || stubClassName == null || method == null) {
      LOGGER.error(
          "Missing required gRPC config for store {}, region {}, version {}. Need channel, stub, and method configs.",
          storeName,
          regionName,
          versionNumber);
      return StoreVersionLifecycleEventOutcome.PROCEED;
    }

    try {
      // Get or create the async stub for this service
      AbstractAsyncStub<?> stub = getOrCreateAsyncStub(channelTarget, stubClassName);

      // Create the request
      GrpcStoreLifecycleHooksRequest request = GrpcStoreLifecycleHooksRequest.newBuilder()
          .setStoreName(storeName)
          .setRegion(regionName)
          .setVersion(String.valueOf(versionNumber))
          .build();

      // Create a CompletableFuture to track the async call
      CompletableFuture<StoreVersionLifecycleEventOutcome> future = new CompletableFuture<>();

      // Find the async RPC method using reflection
      Method rpcMethod = stub.getClass().getMethod(method, GrpcStoreLifecycleHooksRequest.class, StreamObserver.class);

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

      // Make the async call using reflection
      rpcMethod.invoke(stub, request, responseObserver);

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
  private StoreVersionLifecycleEventOutcome mapProtoToJavaEnum(StoreVersionLifecycleEventOutcomeProto outcomeProto) {
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
   * Gets or creates an async stub for the given channel target and stub class name.
   * Uses the cached stub if available, otherwise creates a new one.
   */
  private AbstractAsyncStub<?> getOrCreateAsyncStub(String channelTarget, String stubClassName) throws Exception {
    // Use a combination of channel target and stub class name as a cache key
    String cacheKey = channelTarget + "#" + stubClassName;

    // Try to get from cache first
    AbstractAsyncStub<?> stub = stubCache.get(cacheKey);
    if (stub != null) {
      return stub;
    }

    // Create the stub if it doesn't exist in cache
    synchronized (stubCache) {
      // Double-check within synchronized block
      stub = stubCache.get(cacheKey);
      if (stub != null) {
        return stub;
      }

      // Get or create the channel
      ManagedChannel channel = channelCache.computeIfAbsent(
          channelTarget,
          target -> ManagedChannelBuilder.forTarget(target)
              .usePlaintext() // For simplicity; use .useTransportSecurity() for production
              .build());

      // Get or load the stub class
      Class<?> stubClass = stubClassCache.computeIfAbsent(stubClassName, className -> {
        try {
          return Class.forName(className);
        } catch (ClassNotFoundException e) {
          LOGGER.error("Failed to load stub class: {}", className, e);
          throw new RuntimeException("Failed to load stub class: " + className, e);
        }
      });

      // Create new async stub using reflection
      Method newStubMethod = stubClass.getMethod("newStub", io.grpc.Channel.class);
      stub = (AbstractAsyncStub<?>) newStubMethod.invoke(null, channel);

      // Store in cache
      stubCache.put(cacheKey, stub);
      return stub;
    }
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

  /**
   * Closes this instance. Since we're using static caches, no actual resources need to be cleaned up
   * when an individual instance is closed. Resources will be cleaned up on JVM shutdown.
   */
  @Override
  public void close() throws IOException {
    LOGGER.debug("GrpcStoreLifecycleHook instance closed.");
  }
}
