package com.linkedin.venice.fastclient.utils;

import static org.mockito.Mockito.mock;

import com.google.common.collect.Sets;
import com.linkedin.common.callback.Callback;
import com.linkedin.common.util.None;
import com.linkedin.d2.balancer.D2Client;
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
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.fastclient.ClientConfig;
import com.linkedin.venice.fastclient.factory.ClientFactory;
import com.linkedin.venice.fastclient.meta.AbstractClientRoutingStrategy;
import com.linkedin.venice.fastclient.meta.AbstractStoreMetadata;
import com.linkedin.venice.read.protocol.request.router.MultiGetRouterRequestKeyV1;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
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
 * This class simulates fastclient: Idea is to implement some deterministic way to simulate the different concurrency
 * situations (eg: In case of speculative query, what happens when the primary request succeeds first, what happens when
 * the secondary request succeeds first, to be able to simulate these 2 scenarios in a deterministic way). This class
 * uses timeticks to mimic time and order events and simulate different flows that can happen during runtime. Idea is
 * to have a comprehensive set of such flows/events, but it needs to be evolved over time based any new concerns or
 * edge cases found in deployment.
 *
 * Time is expressed as timeticks conventionally between 0 and 99. This is meant to simulate timed ordering of
 * events while allowing control over the ordering itself. Each timetick is executed approx 1 ms apart
 * The simulator guarantees that every event with a lower timetick will be executed BEFORE one with higher timetick.
 * If events have same timetick they can be executed in any order. Events can be executed in different threads.
 * Initially the simulator is designed to simulate interactions with the R2 client. Later on more events can
 * be supported.
 */
public class TestClientSimulator implements Client {
  private static final Logger LOGGER = LogManager.getLogger(TestClientSimulator.class);
  public static final String UNIT_TEST_STORE_NAME = "unittest";

  // List of events to simulate by time tick
  TreeMap<Integer, List<Event>> timeToEvents = new TreeMap<>();

  ConcurrentHashMap<Integer, RequestInfo> requestIdToRequestInfos = new ConcurrentHashMap<>();

  ConcurrentHashMap<String, Deque<ExpectedRequestEvent>> routeToExpectedRequestEvents = new ConcurrentHashMap<>();

  public ClientConfig getClientConfig() {
    return clientConfig;
  }

  ClientConfig clientConfig;

  private static Schema KEY_VALUE_SCHEMA = new Schema.Parser().parse("\"string\"");

  public final RecordSerializer<String> keySerializer;
  public final RecordDeserializer<Utf8> keyDeserializer;
  public final RecordSerializer<MultiGetResponseRecordV1> multiGetResponseSerializer;
  public final RecordDeserializer<MultiGetRouterRequestKeyV1> multiGetRequestDeserializer;
  private final D2Client mockD2Client = mock(D2Client.class);
  private final String dummyD2Discovery = "testD2Endpoint";
  private boolean speculativeQueryEnabled = false;
  private Map<String, String> keyValues = new HashMap<>(); // all keys in the simulation
  private Map<String, String> requestedKeyValues = new HashMap<>(); // subset/all of keyValues are a part of requests
  private Map<String, Integer> keysToPartitions = new HashMap<>();
  private Map<String, Set<Integer>> routeToPartitions = new HashMap<>();
  private Map<Integer, List<String>> partitionToReplicas = new HashMap<>();
  private boolean longTailRetryEnabledForSingleGet = false;
  private int longTailRetryThresholdForSingleGetInMicroseconds = 0;
  private boolean longTailRetryEnabledForBatchGet = false;
  private int longTailRetryThresholdForBatchGetInMicroSeconds = 0;

  private int expectedValueSchemaId = 1;

  private static class UnitTestRoutingStrategy extends AbstractClientRoutingStrategy {
    @Override
    public List<String> getReplicas(long requestId, List<String> replicas, int requiredReplicaCount) {
      List<String> retReplicas = new ArrayList<>();
      for (int i = 0; i < requiredReplicaCount && i < replicas.size(); i++) {
        retReplicas.add(replicas.get(i));
      }
      return retReplicas;
    }
  }

  public TestClientSimulator() {
    // get()
    this.keySerializer = FastSerializerDeserializerFactory.getFastAvroGenericSerializer(KEY_VALUE_SCHEMA);
    this.keyDeserializer =
        FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(KEY_VALUE_SCHEMA, KEY_VALUE_SCHEMA);
    // multiGet()
    this.multiGetResponseSerializer =
        FastSerializerDeserializerFactory.getFastAvroGenericSerializer(MultiGetResponseRecordV1.SCHEMA$);
    this.multiGetRequestDeserializer = FastSerializerDeserializerFactory
        .getFastAvroSpecificDeserializer(MultiGetRouterRequestKeyV1.SCHEMA$, MultiGetRouterRequestKeyV1.class);
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
   * Partition given keys into equal partitions
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
    requestInfo.timeTick = timeTick;

    Set<Integer> partitionSet = Sets.newHashSet(Ints.asList(partitions));
    requestInfo.keyValues = keysToPartitions.entrySet()
        .stream()
        .filter(e -> partitionSet.contains(e.getValue()))
        .collect(Collectors.toMap(e -> e.getKey(), e -> keyValues.get(e.getKey())));

    requestedKeyValues.putAll(requestInfo.keyValues);

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

    RequestInfo requestInfo = requestIdToRequestInfos.get(requestId);
    if (requestInfo.timeTick > timeTick) {
      throw new IllegalStateException("Request should happen before response");
    }

    timeToEvents.putIfAbsent(timeTick, new ArrayList<>());
    timeToEvents.get(timeTick).add(new SendResponseEvent(requestInfo));
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

  public TestClientSimulator setExpectedValueSchemaId(int id) {
    expectedValueSchemaId = id;
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

  /**
   * Mock of actual restRequest() that will be called as a result of get() or post()
   * calls, which is called from venice's get() or multiGet() to validate whether the
   * keys/route coming in from the client get() is what is expected based on input to
   * {@link #expectRequestWithKeysForPartitionOnRoute}.
   *
   * <br><br>
   * This function receives the get/post request, deserialize it, verify it against
   * data in {@link #routeToExpectedRequestEvents}
   */
  @Override
  public void restRequest(RestRequest request, Callback<RestResponse> callback) {
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
        Assert.assertTrue(
            expectedKeys.contains(key.toString()),
            "Unexpected key received: " + key + " Expected keys: " + expectedKeys);
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

  /*public TestClientSimulator setKeysToPartitions(Map<String, Integer> keysToPartitions) {
    this.keysToPartitions = keysToPartitions;
    return this;
  }*/

  public TestClientSimulator assignRouteToPartitions(String route, int... partitions) {
    Set<Integer> partitionSet = this.routeToPartitions.computeIfAbsent(route, r -> new HashSet<>());
    for (int partition: partitions) {
      partitionSet.add(partition);
    }
    return this;
  }

  /*public TestClientSimulator setRouteToPartitions(Map<String, Set<Integer>> routeToPartitions) {
    this.routeToPartitions = routeToPartitions;
    return this;
  }*/

  public Map<String, String> getKeyValues() {
    return keyValues;
  }

  public Map<String, String> getRequestedKeyValues() {
    // only return all the keys that have been sent to request events
    return requestedKeyValues;
  }

  /** Mock the return replica list in the method {@link AbstractStoreMetadata#getReplicas}
   * created inside {@link TestClientSimulator#getFastClient} */
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

  static class RequestInfo {
    int requestId;
    int timeTick;
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

  /**
   * Simulates timed expectation that some request to a remote host was sent. The timetick
   * supplied by the event means that as soon as the expected request is matched, time will be set to the given timetick.
   */
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

  /**
   * Simulates responses from that host at the given time.
   */
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
          rec.schemaId = expectedValueSchemaId;
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
  CompletableFuture<Integer> simulatorCompleteFuture = new CompletableFuture<>();

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
    getSimulatorCompleteFuture().whenComplete((v, t) -> {
      try {
        TestUtils.shutdownExecutor(executor);
      } catch (InterruptedException e) {
        Assert.fail("Executor shutdown interrupted", e);
      }
    });
  }

  public synchronized void executeTimedEvents(int time) {
    currentTimeTick.set(time);
    if (time == 0) {
      LOGGER.info("t:0 Starting the Execution");
    } else {
      LOGGER.info("t:{} Executing {} timed events ", time, timeToEvents.get(time));
    }
    if (timeToEvents.containsKey(time)) {
      List<Event> events = timeToEvents.get(time);
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
      simulatorCompleteFuture.completeExceptionally(e);
    }
    Integer nextTimeTick = timeToEvents.higherKey(currentTimeTick.get());
    LOGGER.info("t:{} Scheduling next timer for {} ", currentTimeTick.get(), nextTimeTick);
    if (nextTimeTick != null) {
      int delay = (nextTimeTick - currentTimeTick.get()) * timeIntervalBetweenEventsInMs;
      currentTimeTick.set(nextTimeTick);
      executor.schedule(() -> executeTimedEvents(nextTimeTick), delay, TimeUnit.MILLISECONDS);
    } else { // this is the last timetick
      LOGGER.info("Completing simulation at timeTick {} ", currentTimeTick.get());
      simulatorCompleteFuture.complete(currentTimeTick.get());
    }
  }

  public CompletableFuture<Integer> getSimulatorCompleteFuture() {
    return simulatorCompleteFuture;
  }

  // TODO need to add tests for this
  public TestClientSimulator setSpeculativeQueryEnabled(boolean speculativeQueryEnabled) {
    this.speculativeQueryEnabled = speculativeQueryEnabled;
    return this;
  }

  public TestClientSimulator setLongTailRetryEnabledForSingleGet(boolean longTailRetryEnabledForSingleGet) {
    this.longTailRetryEnabledForSingleGet = longTailRetryEnabledForSingleGet;
    return this;
  }

  public TestClientSimulator setLongTailRetryThresholdForSingleGetInMicroseconds(
      int longTailRetryThresholdForSingleGetInMicroseconds) {
    this.longTailRetryThresholdForSingleGetInMicroseconds = longTailRetryThresholdForSingleGetInMicroseconds;
    return this;
  }

  public TestClientSimulator setLongTailRetryEnabledForBatchGet(boolean longTailRetryEnabledForBatchGet) {
    this.longTailRetryEnabledForBatchGet = longTailRetryEnabledForBatchGet;
    return this;
  }

  public TestClientSimulator setLongTailRetryThresholdForBatchGetInMicroSeconds(
      int longTailRetryThresholdForBatchGetInMicroSeconds) {
    this.longTailRetryThresholdForBatchGetInMicroSeconds = longTailRetryThresholdForBatchGetInMicroSeconds;
    return this;
  }

  public AvroGenericStoreClient<String, Utf8> getFastClient() {
    // Test generic store client
    ClientConfig.ClientConfigBuilder clientConfigBuilder =
        new ClientConfig.ClientConfigBuilder<Object, Object, SpecificRecord>();
    clientConfigBuilder.setStoreName(UNIT_TEST_STORE_NAME);
    clientConfigBuilder.setR2Client(this);
    clientConfigBuilder.setMetricsRepository(new MetricsRepository());
    clientConfigBuilder.setSpeculativeQueryEnabled(speculativeQueryEnabled);
    if (longTailRetryEnabledForBatchGet) {
      clientConfigBuilder.setLongTailRetryEnabledForBatchGet(true);
      clientConfigBuilder
          .setLongTailRetryThresholdForBatchGetInMicroSeconds(longTailRetryThresholdForBatchGetInMicroSeconds);
    }
    if (longTailRetryEnabledForSingleGet) {
      clientConfigBuilder.setLongTailRetryEnabledForSingleGet(true);
      clientConfigBuilder
          .setLongTailRetryThresholdForSingleGetInMicroSeconds(longTailRetryThresholdForSingleGetInMicroseconds);
    }

    // TODO: need to add tests for simulating dual read
    clientConfigBuilder.setDualReadEnabled(false);
    clientConfigBuilder.setD2Client(mockD2Client);
    clientConfigBuilder.setClusterDiscoveryD2Service(dummyD2Discovery);
    clientConfig = clientConfigBuilder.build();

    AbstractStoreMetadata metadata = new AbstractStoreMetadata(clientConfig) {
      @Override
      public String getClusterName() {
        return "test-cluster";
      }

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
        if (id == expectedValueSchemaId) {
          return KEY_VALUE_SCHEMA;
        } else {
          throw new VeniceException(
              "Unexpected get schema call with id: " + id + ", expecting value schema id: " + expectedValueSchemaId);
        }
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
        return expectedValueSchemaId;
      }

      @Override
      public Schema getUpdateSchema(int valueSchemaId) {
        return null;
      }

      @Override
      public DerivedSchemaEntry getLatestUpdateSchema() {
        return null;
      }

      @Override
      public int getBatchGetLimit() {
        return 1000;
      }
    };

    metadata.setRoutingStrategy(new UnitTestRoutingStrategy());

    return ClientFactory.getAndStartGenericStoreClient(metadata, clientConfig);
  }

}
