package com.linkedin.venice.listener.grpc;

import static com.linkedin.venice.utils.TestUtils.DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.kafka.consumer.PartitionConsumptionState;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.protocols.IngestionMonitorRequest;
import com.linkedin.venice.protocols.IngestionMonitorResponse;
import com.linkedin.venice.protocols.VeniceIngestionMonitorServiceGrpc;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class VeniceIngestionMonitorServiceImplTest {
  private static final String VERSION_TOPIC = "testStore_v1";
  private static final int PARTITION = 0;
  private static final PubSubTopicRepository TOPIC_REPOSITORY = new PubSubTopicRepository();

  private KafkaStoreIngestionService mockIngestionService;
  private StoreIngestionTask mockIngestionTask;
  private PartitionConsumptionState realPcs;
  private Server server;
  private ManagedChannel channel;
  private VeniceIngestionMonitorServiceGrpc.VeniceIngestionMonitorServiceBlockingStub blockingStub;

  @BeforeMethod
  public void setUp() throws Exception {
    mockIngestionService = mock(KafkaStoreIngestionService.class);
    mockIngestionTask = mock(StoreIngestionTask.class);

    // Create a real PCS since its getters are final
    PubSubTopicPartition topicPartition =
        new PubSubTopicPartitionImpl(TOPIC_REPOSITORY.getTopic(VERSION_TOPIC), PARTITION);
    realPcs = new PartitionConsumptionState(
        topicPartition,
        mock(OffsetRecord.class),
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING,
        true,
        false,
        false,
        null);

    when(mockIngestionService.getStoreIngestionTask(VERSION_TOPIC)).thenReturn(mockIngestionTask);
    when(mockIngestionTask.getPartitionConsumptionState(PARTITION)).thenReturn(realPcs);

    VeniceIngestionMonitorServiceImpl serviceImpl = new VeniceIngestionMonitorServiceImpl(mockIngestionService);

    String serverName = InProcessServerBuilder.generateName();
    server = InProcessServerBuilder.forName(serverName).directExecutor().addService(serviceImpl).build().start();

    channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
    blockingStub = VeniceIngestionMonitorServiceGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);
  }

  @AfterMethod
  public void tearDown() throws Exception {
    if (channel != null) {
      channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
    if (server != null) {
      server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testMonitorIngestionReturnsMetrics() {
    IngestionMonitorRequest request = IngestionMonitorRequest.newBuilder()
        .setVersionTopic(VERSION_TOPIC)
        .setPartition(PARTITION)
        .setIntervalMs(1000)
        .build();

    Iterator<IngestionMonitorResponse> responses = blockingStub.monitorIngestion(request);
    assertTrue(responses.hasNext(), "Should receive at least one response");

    IngestionMonitorResponse response = responses.next();
    assertNotNull(response);
    assertTrue(response.getTimestampMs() > 0);
    // PCS starts in STANDBY state
    assertTrue(
        response.getLeaderFollowerState().equals("STANDBY") || response.getLeaderFollowerState().equals("LEADER"));
    assertTrue(response.getIsHybrid());
  }

  @Test(expectedExceptions = StatusRuntimeException.class)
  public void testMonitorIngestionWithEmptyVersionTopic() {
    IngestionMonitorRequest request =
        IngestionMonitorRequest.newBuilder().setVersionTopic("").setPartition(PARTITION).setIntervalMs(1000).build();

    Iterator<IngestionMonitorResponse> responses = blockingStub.monitorIngestion(request);
    responses.next(); // Should trigger the error
  }

  @Test(expectedExceptions = StatusRuntimeException.class)
  public void testMonitorIngestionWithNonExistentTopic() {
    when(mockIngestionService.getStoreIngestionTask("nonexistent_v1")).thenReturn(null);

    IngestionMonitorRequest request = IngestionMonitorRequest.newBuilder()
        .setVersionTopic("nonexistent_v1")
        .setPartition(PARTITION)
        .setIntervalMs(1000)
        .build();

    Iterator<IngestionMonitorResponse> responses = blockingStub.monitorIngestion(request);
    responses.next();
  }

  @Test(expectedExceptions = StatusRuntimeException.class)
  public void testMonitorIngestionWithNonExistentPartition() {
    when(mockIngestionTask.getPartitionConsumptionState(99)).thenReturn(null);

    IngestionMonitorRequest request = IngestionMonitorRequest.newBuilder()
        .setVersionTopic(VERSION_TOPIC)
        .setPartition(99)
        .setIntervalMs(1000)
        .build();

    Iterator<IngestionMonitorResponse> responses = blockingStub.monitorIngestion(request);
    responses.next();
  }

  @Test(expectedExceptions = StatusRuntimeException.class)
  public void testDuplicateSessionReturnsAlreadyExists() {
    IngestionMonitorRequest request = IngestionMonitorRequest.newBuilder()
        .setVersionTopic(VERSION_TOPIC)
        .setPartition(PARTITION)
        .setIntervalMs(1000)
        .build();

    // Start first session
    Iterator<IngestionMonitorResponse> firstSession = blockingStub.monitorIngestion(request);
    assertTrue(firstSession.hasNext());
    firstSession.next(); // consume one response to ensure session is active

    // Attempt second session for the same partition - should fail with ALREADY_EXISTS
    try {
      Iterator<IngestionMonitorResponse> secondSession = blockingStub.monitorIngestion(request);
      secondSession.next();
    } catch (StatusRuntimeException e) {
      assertTrue(
          e.getStatus().getCode() == io.grpc.Status.Code.ALREADY_EXISTS,
          "Expected ALREADY_EXISTS but got: " + e.getStatus().getCode());
      throw e;
    }
  }

  @Test
  public void testMinimumIntervalEnforced() {
    IngestionMonitorRequest request = IngestionMonitorRequest.newBuilder()
        .setVersionTopic(VERSION_TOPIC)
        .setPartition(PARTITION)
        .setIntervalMs(100) // below minimum of 1000
        .build();

    Iterator<IngestionMonitorResponse> responses = blockingStub.monitorIngestion(request);
    assertTrue(responses.hasNext());
    // Should still work, just with clamped interval
    assertNotNull(responses.next());
  }

  @Test
  public void testNonServerCallStreamObserverFailsWithInternal() {
    // gRPC normally hands the service a ServerCallStreamObserver; a wrapping interceptor could break that.
    // The service must fail fast with INTERNAL and leave no monitor attached, rather than throw mid-registration.
    VeniceIngestionMonitorServiceImpl service = new VeniceIngestionMonitorServiceImpl(mockIngestionService);
    @SuppressWarnings("unchecked")
    StreamObserver<IngestionMonitorResponse> observer = mock(StreamObserver.class);
    IngestionMonitorRequest request = IngestionMonitorRequest.newBuilder()
        .setVersionTopic(VERSION_TOPIC)
        .setPartition(PARTITION)
        .setIntervalMs(1000)
        .build();

    service.monitorIngestion(request, observer);

    ArgumentCaptor<Throwable> errorCaptor = ArgumentCaptor.forClass(Throwable.class);
    verify(observer).onError(errorCaptor.capture());
    assertEquals(Status.fromThrowable(errorCaptor.getValue()).getCode(), Status.Code.INTERNAL);
    assertNull(realPcs.getIngestionMonitor(), "No monitor should be attached after a rejected cast");
  }

  @Test
  public void testMonitorIngestionRejectedAfterClose() {
    // After close(), new sessions must be rejected with UNAVAILABLE and must not attach a monitor.
    VeniceIngestionMonitorServiceImpl service = new VeniceIngestionMonitorServiceImpl(mockIngestionService);
    service.close();
    @SuppressWarnings("unchecked")
    StreamObserver<IngestionMonitorResponse> observer = mock(StreamObserver.class);
    IngestionMonitorRequest request = IngestionMonitorRequest.newBuilder()
        .setVersionTopic(VERSION_TOPIC)
        .setPartition(PARTITION)
        .setIntervalMs(1000)
        .build();

    service.monitorIngestion(request, observer);

    ArgumentCaptor<Throwable> errorCaptor = ArgumentCaptor.forClass(Throwable.class);
    verify(observer).onError(errorCaptor.capture());
    assertEquals(Status.fromThrowable(errorCaptor.getValue()).getCode(), Status.Code.UNAVAILABLE);
    assertNull(realPcs.getIngestionMonitor(), "No monitor should be attached after close()");
  }
}
