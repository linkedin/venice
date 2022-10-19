package com.linkedin.venice.fastclient.utils;

import com.google.common.collect.Sets;
import com.linkedin.common.callback.Callback;
import com.linkedin.common.util.None;
import com.linkedin.data.ByteString;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.RestException;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.rest.RestResponseBuilder;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.fastclient.ClientConfig;
import com.linkedin.venice.fastclient.factory.ClientFactory;
import com.linkedin.venice.fastclient.meta.AbstractStoreMetadata;
import com.linkedin.venice.fastclient.meta.ClientRoutingStrategy;
import com.linkedin.venice.read.protocol.request.router.MultiGetRouterRequestKeyV1;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.TestUtils;
import io.tehuti.metrics.MetricsRepository;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.io.ByteBufferOptimizedBinaryDecoder;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.util.Utf8;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.internal.collections.Ints;


/**
 * Time is expressed as timeticks conventionally between 0 and 99. This is meant to simulate timed ordering of
 * events while allowing control over the ordering itself. Each timetick is executed approx 1 ms apart
 * The simulator guarantees that every event with a lower timetick will be executed BEFORE one with higher timetick.
 * If events have same timetick they can be executed in any order. Events can be executed in different threads
 * Initially the simulator is designed to simulate interactions with the R2 client . Later on more events can
 * be supported.
 * ExpectedRequestEvent  simulates timed expectation that some request to a remote host was sent. The timetick
 * supplied by the event means that as soon as the expected request is matched , time will be set to the given timetick
 * SendResponseEvent simulates responses from that host at the given time.
 */
public class TestClientSimulator implements Client {
  private static final Logger LOGGER = LogManager.getLogger(TestClientSimulator.class);

  // List of events to simulate by time tick
  TreeMap<Integer, List<Event>> timeToEvents = new TreeMap<>();

  ConcurrentHashMap<Integer, RequestInfo> requestIdToRequestInfos = new ConcurrentHashMap<>();

  ConcurrentHashMap<String, Deque<ExpectedRequestEvent>> routeToExpectedRequestEvents = new ConcurrentHashMap<>();

  ClientConfig clientConfig;

  private static Schema KEY_VALUE_SCHEMA = new Schema.Parser().parse("\"string\"");

  public final RecordSerializer<String> keySerializer;
  public final RecordDeserializer<Utf8> keyDeserializer;
  public final RecordSerializer<MultiGetResponseRecordV1> multiGetResponseSerializer;
  public final RecordDeserializer<MultiGetRouterRequestKeyV1> multiGetRequestDeserializer;
  private boolean speculativeQueryEnabled = false;
  private Map<String, String> keyValues = new HashMap<>();
  private Map<String, Integer> keysToPartitions = new HashMap<>();
  private Map<String, Set<Integer>> routeToPartitions = new HashMap<>();
  private Map<Integer, List<String>> partitionToReplicas = new HashMap<>();
  private boolean longTailRetryEnabledForBatchGet = false;
  private int longTailRetryThresholdForBatchGetInMicroseconds = 0;

  public TestClientSimulator() {
    this.keySerializer = FastSerializerDeserializerFactory.getAvroGenericSerializer(KEY_VALUE_SCHEMA);
    this.keyDeserializer =
        FastSerializerDeserializerFactory.getAvroGenericDeserializer(KEY_VALUE_SCHEMA, KEY_VALUE_SCHEMA);
    this.multiGetRequestDeserializer = FastSerializerDeserializerFactory
        .getFastAvroSpecificDeserializer(MultiGetRouterRequestKeyV1.SCHEMA$, MultiGetRouterRequestKeyV1.class);
    this.multiGetResponseSerializer =
        FastSerializerDeserializerFactory.getFastAvroGenericSerializer(MultiGetResponseRecordV1.SCHEMA$);
  }

  /**
   * Generate string key values based of range of integers
   * @param start starting of range inclusive
   * @param end end of range exclusive
   * @return keys and values with the pattern k_i and v_i
   */
  public TestClientSimulator generateKeyValues(int start, int end) {
    for (int i = start; i < end; i++) {
      keyValues.put("k_" + i, "v_" + i);
    }
    return this;
  }

  /**
   * Partition Given keys into equal partitions
   * @param numPartitions number of partitions
   * @return partitioned Keys with partition numbers from 0 to numPartitions - 1
   */
  public TestClientSimulator partitionKeys(int numPartitions) {
    int i = 0;
    for (String key: keyValues.keySet()) {
      keysToPartitions.put(key, i++ % numPartitions);
    }
    return this;
  }

  public TestClientSimulator expectRequestWithKeysForPartitionOnRoute(
      int timeTick,
      int requestId,
      String route,
      int... partitions) {
    RequestInfo requestInfo = new RequestInfo();
    requestInfo.requestId = requestId;
    requestInfo.route = route;

    Set<Integer> partitionSet = Sets.newHashSet(Ints.asList(partitions));
    requestInfo.keyValues = keysToPartitions.entrySet()
        .stream()
        .filter(e -> partitionSet.contains(e.getValue()))
        .collect(Collectors.toMap(e -> e.getKey(), e -> keyValues.get(e.getKey())));

    requestIdToRequestInfos.put(requestInfo.requestId, requestInfo);
    ExpectedRequestEvent expectedRequestEvent = new ExpectedRequestEvent(requestInfo, timeTick);
    timeToEvents.computeIfAbsent(timeTick, t -> new ArrayList<>()).add(expectedRequestEvent);
    routeToExpectedRequestEvents.computeIfAbsent(requestInfo.route, r -> new LinkedList<>())
        .addLast(expectedRequestEvent);

    return this;
  }

  public TestClientSimulator respondToRequestWithKeyValues(int timeTick, int requestId) {
    if (!requestIdToRequestInfos.containsKey(requestId)) {
      throw new IllegalStateException("Must have a corresponding request");
    }
    timeToEvents.putIfAbsent(timeTick, new ArrayList<>());
    timeToEvents.get(timeTick).add(new SendResponseEvent(requestIdToRequestInfos.get(requestId)));
    return this;
  }

  public TestClientSimulator respondToRequestWithError(int timeTick, int requestId, int errorCode) {
    if (!requestIdToRequestInfos.containsKey(requestId)) {
      throw new IllegalStateException("Must have a corresponding request");
    }
    timeToEvents.putIfAbsent(timeTick, new ArrayList<>());
    timeToEvents.get(timeTick).add(new SendResponseEvent(requestIdToRequestInfos.get(requestId), errorCode));
    return this;
  }

  @Override
  public Future<RestResponse> restRequest(RestRequest request) {
    throw new IllegalStateException("Unexpected rest request");
  }

  @Override
  public Future<RestResponse> restRequest(RestRequest request, RequestContext requestContext) {
    throw new IllegalStateException("Unexpected rest request");
  }

  @Override
  public void restRequest(RestRequest request, Callback<RestResponse> callback) {
    // Receive and deserialize the request to verify
    URI uri = request.getURI();
    if (uri.getHost() != null) {
      String route = uri.getScheme() + "://" + uri.getHost();
      LOGGER.info("Received rest request on route {} ", route);
      Deque<ExpectedRequestEvent> requestInfos = routeToExpectedRequestEvents.get(route);

      Assert.assertFalse(requestInfos.isEmpty());
      ExpectedRequestEvent expectedRequestEvent = requestInfos.removeFirst();
      Assert.assertNotNull(expectedRequestEvent);

      Set<String> expectedKeys = new HashSet<>(expectedRequestEvent.info.keyValues.keySet());
      expectedRequestEvent.info.orderedKeys = new ArrayList<>();
      LOGGER.info(
          "t:{} Received rest request . Expecting {} keys on route {} routeId {}",
          currentTimeTick.get(),
          expectedKeys.size(),
          route,
          expectedRequestEvent.info.requestId);

      ByteString entity = request.getEntity();
      Iterable<MultiGetRouterRequestKeyV1> multiGetRouterRequestKeyV1s =
          multiGetRequestDeserializer.deserializeObjects(new ByteBufferOptimizedBinaryDecoder(entity.copyBytes()));
      for (MultiGetRouterRequestKeyV1 keyRecord: multiGetRouterRequestKeyV1s) {
        Utf8 key = keyDeserializer.deserialize(keyRecord.keyBytes);
        LOGGER.info("t:{} Received key {} on route {} ", currentTimeTick.get(), key, route);
        Assert.assertTrue(expectedKeys.contains(key.toString()), "Unexpected key received " + key);
        expectedKeys.remove(key.toString());
        expectedRequestEvent.info.orderedKeys.add(key.toString());
      }
      Assert.assertTrue(expectedKeys.isEmpty());
      expectedRequestEvent.info.callback = callback;
      expectedRequestEvent.future.complete(currentTimeTick.get());
    }
  }

  @Override
  public void restRequest(RestRequest request, RequestContext requestContext, Callback<RestResponse> callback) {
    throw new IllegalStateException("Unexpected rest request");
  }

  @Override
  public void shutdown(Callback<None> callback) {

  }

  public TestClientSimulator setKeysToPartitions(Map<String, Integer> keysToPartitions) {
    this.keysToPartitions = keysToPartitions;
    return this;
  }

  public TestClientSimulator assignRouteToPartitions(String route, int... partitions) {
    Set<Integer> partitionSet = this.routeToPartitions.computeIfAbsent(route, r -> new HashSet<>());
    for (int partition: partitions) {
      partitionSet.add(partition);
    }
    return this;
  }

  public TestClientSimulator setRouteToPartitions(Map<String, Set<Integer>> routeToPartitions) {
    this.routeToPartitions = routeToPartitions;
    return this;
  }

  public Map<String, String> getKeyValues() {
    return keyValues;
  }

  public TestClientSimulator expectReplicaRequestForPartitionAndRespondWithReplicas(
      int partitionId,
      List<String> replicas) {
    partitionToReplicas.put(partitionId, replicas);
    return this;
  }

  abstract class Event {
    int timeTick;
    CompletableFuture<Integer> future = new CompletableFuture<>();

    abstract CompletableFuture<Integer> execute();
  }

  class RequestInfo {
    int requestId;
    String route;
    Map<String, String> keyValues;
    Callback<RestResponse> callback;
    List<String> orderedKeys;

    @Override
    public String toString() {
      return "RequestInfo{" + "requestId=" + requestId + ", route='" + route + '\'' + ", keyValues=" + keyValues
          + ", callback=" + callback + '}';
    }
  }

  class ExpectedRequestEvent extends Event {
    RequestInfo info;

    public ExpectedRequestEvent(RequestInfo info, int timeTick) {
      this.info = info;
      this.timeTick = timeTick;
    }

    public CompletableFuture<Integer> execute() {
      LOGGER.info(" t:{} Waiting for request {} ", timeTick, info);
      return future.whenComplete((v, e) -> {
        // If we get here our expectations are matched. Move the clock to the expected timeTick
        LOGGER.info("t:{} Request {} matched ", timeTick, info);
        currentTimeTick.set(timeTick);
      });
    }

    @Override
    public String toString() {
      return "ExpectedRequestEvent {" + "route=" + info.route + ", numberOfKeys =" + info.keyValues.size() + "}";
    }
  }

  class SendResponseEvent extends Event {
    RequestInfo info;
    boolean isError;
    int errorCode;

    public SendResponseEvent(RequestInfo requestInfo) {
      this.info = requestInfo;
    }

    public SendResponseEvent(RequestInfo requestInfo, int errorCode) {
      this.info = requestInfo;
      this.isError = true;
      this.errorCode = errorCode;
    }

    public CompletableFuture<Integer> execute() {
      if (isError) {
        LOGGER.info(
            "t:{} Sending error response via route {} for request {} ",
            currentTimeTick.get(),
            info.route,
            info.requestId);
        info.callback.onError(RestException.forError(errorCode, "Something is rotten"));
      } else {
        LOGGER.info(
            "t:{} Sending {} keys via route {} for request {} ",
            currentTimeTick.get(),
            info.keyValues.size(),
            info.route,
            info.requestId);
        List<MultiGetResponseRecordV1> multiGetResponse = new ArrayList<>();
        for (int i = 0; i < info.orderedKeys.size(); i++) {
          MultiGetResponseRecordV1 rec = new MultiGetResponseRecordV1();
          rec.value = ByteBuffer.wrap(keySerializer.serialize(info.keyValues.get(info.orderedKeys.get(i))));
          rec.keyIndex = i;
          rec.schemaId = 1;
          multiGetResponse.add(rec);
        }
        RestResponseBuilder restResponseBuilder = new RestResponseBuilder();
        restResponseBuilder.setEntity(multiGetResponseSerializer.serializeObjects(multiGetResponse))
            .setStatus(200)
            .build();
        info.callback.onSuccess(restResponseBuilder.build());
      }
      future.complete(timeTick);
      return future;
    }

    @Override
    public String toString() {
      return "SendResponseEvent {" + "route=" + info.route + ", numberOfKeys =" + info.keyValues.size() + "}";
    }
  }

  AtomicInteger currentTimeTick = new AtomicInteger();
  int timeIntervalBetweenEventsInMs = 1;
  ScheduledExecutorService executor;
  CompletableFuture<Integer> simulatorComplete = new CompletableFuture<>();

  public void simulate() {
    LOGGER.info(
        "Starting simulation with {} keys and {} partitions",
        keyValues.size(),
        new HashSet<>(keysToPartitions.values()).size());
    LOGGER.info("Simulating timeline -->  ");
    for (int time: timeToEvents.navigableKeySet()) {
      LOGGER.info("  time: {} , events: -- >", time);
      for (Event event: timeToEvents.get(time)) {
        LOGGER.info("    event: --> {} ", event);
      }
    }
    executor = Executors.newScheduledThreadPool(4);
    executor.schedule(() -> executeTimedEvents(0), timeIntervalBetweenEventsInMs, TimeUnit.MILLISECONDS);
    getSimulatorComplete().whenComplete((v, t) -> {
      try {
        TestUtils.shutdownExecutor(executor);
      } catch (InterruptedException e) {
        Assert.fail("Executor shutdown interrupted", e);
      }
    });
  }

  public synchronized void executeTimedEvents(int time) {
    currentTimeTick.set(time);
    LOGGER.info("t:{} Executing {} timed events ", currentTimeTick.get(), timeToEvents.get(currentTimeTick.get()));
    if (timeToEvents.containsKey(currentTimeTick.get())) {
      List<Event> events = timeToEvents.get(currentTimeTick.get());
      int index = 0;
      CompletableFuture[] futures = new CompletableFuture[events.size()];
      for (Event event: events) {
        futures[index++] = event.future;
      }
      CompletableFuture.allOf(futures).whenComplete(this::scheduleNextTimeTick);
      for (Event event: events) {
        event.execute();
      }
    } else {
      scheduleNextTimeTick(null, null);
    }
  }

  private void scheduleNextTimeTick(Void i, Throwable e) {
    if (e != null) {
      LOGGER.error("Exception while executing timed event {} , {} ", currentTimeTick.get(), e);
      simulatorComplete.completeExceptionally(e);
    }
    Integer nextTimeTick = timeToEvents.higherKey(currentTimeTick.get());
    LOGGER.info("t:{} Scheduling next timer for {} ", currentTimeTick.get(), nextTimeTick);
    if (nextTimeTick != null) {
      int delay = (nextTimeTick - currentTimeTick.get()) * timeIntervalBetweenEventsInMs;
      currentTimeTick.set(nextTimeTick);
      executor.schedule(() -> executeTimedEvents(nextTimeTick), delay, TimeUnit.MILLISECONDS);
    } else { // this is the last timetick
      LOGGER.info("Completing simulation at timeTick {} ", currentTimeTick.get());
      simulatorComplete.complete(currentTimeTick.get());
    }
  }

  public CompletableFuture<Integer> getSimulatorComplete() {
    return simulatorComplete;
  }

  public TestClientSimulator setSpeculativeQueryEnabled(boolean speculativeQueryEnabled) {
    this.speculativeQueryEnabled = speculativeQueryEnabled;
    return this;
  }

  public TestClientSimulator setLongTailRetryEnabledForBatchGet(boolean longTailRetryEnabledForBatchGet) {
    this.longTailRetryEnabledForBatchGet = longTailRetryEnabledForBatchGet;
    return this;
  }

  public TestClientSimulator setLongTailRetryThresholdForBatchGetInMicroseconds(
      int longTailRetryThresholdForBatchGetInMicroseconds) {
    this.longTailRetryThresholdForBatchGetInMicroseconds = longTailRetryThresholdForBatchGetInMicroseconds;
    return this;
  }

  public AvroGenericStoreClient<String, Utf8> getFastClient() {
    // Test generic store client
    ClientConfig.ClientConfigBuilder clientConfigBuilder =
        new ClientConfig.ClientConfigBuilder<Object, Object, SpecificRecord>();
    clientConfigBuilder.setStoreName("unittest");
    clientConfigBuilder.setR2Client(this);
    clientConfigBuilder.setMetricsRepository(new MetricsRepository());
    clientConfigBuilder.setSpeculativeQueryEnabled(speculativeQueryEnabled);
    if (longTailRetryEnabledForBatchGet) {
      clientConfigBuilder.setLongTailRetryEnabledForBatchGet(longTailRetryEnabledForBatchGet);
      clientConfigBuilder
          .setLongTailRetryThresholdForBatchGetInMicroSeconds(longTailRetryThresholdForBatchGetInMicroseconds);
    }
    clientConfigBuilder.setClientRoutingStrategy(new ClientRoutingStrategy() {
      @Override
      public List<String> getReplicas(long requestId, List<String> replicas, int requiredReplicaCount) {
        List<String> retReplicas = new ArrayList<>();
        for (int i = 0; i < requiredReplicaCount && i < replicas.size(); i++) {
          retReplicas.add(replicas.get(i));
        }
        return retReplicas;
      }
    });

    clientConfigBuilder.setDualReadEnabled(false);
    clientConfig = clientConfigBuilder.build();

    AbstractStoreMetadata metadata = new AbstractStoreMetadata(clientConfig) {
      @Override
      public int getCurrentStoreVersion() {
        return 1;
      }

      @Override
      public int getPartitionId(int version, ByteBuffer key) {
        byte[] keyBytes = key.array();
        Utf8 desKey = keyDeserializer.deserialize(keyBytes);
        if (keysToPartitions.containsKey(desKey.toString())) {
          return keysToPartitions.get(desKey.toString());
        } else {
          throw new IllegalStateException("Unexpected key received. partition map not initialized correctly");
        }
      }

      @Override
      public List<String> getReplicas(int version, int partitionId) {
        if (partitionToReplicas.containsKey(partitionId)) {
          return partitionToReplicas.get(partitionId);
        } else {
          return routeToPartitions.keySet()
              .stream()
              .filter(r -> routeToPartitions.get(r).contains(partitionId))
              .collect(Collectors.toList());
        }
      }

      @Override
      public VeniceCompressor getCompressor(CompressionStrategy compressionStrategy, int version) {
        return new CompressorFactory().getCompressor(compressionStrategy);
      }

      @Override
      public void start() {

      }

      @Override
      public Schema getKeySchema() {
        return KEY_VALUE_SCHEMA;
      }

      @Override
      public Schema getValueSchema(int id) {
        return KEY_VALUE_SCHEMA;
      }

      @Override
      public int getValueSchemaId(Schema schema) {
        return 0;
      }

      @Override
      public Schema getLatestValueSchema() {
        return KEY_VALUE_SCHEMA;
      }

      @Override
      public Integer getLatestValueSchemaId() {
        return 0;
      }
    };

    return ClientFactory.getAndStartGenericStoreClient(metadata, clientConfig);
  }

}
