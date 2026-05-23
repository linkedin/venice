package com.linkedin.venice.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.exceptions.InvalidVeniceSchemaException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.status.protocol.BatchJobHeartbeatKey;
import com.linkedin.venice.status.protocol.BatchJobHeartbeatValue;
import com.linkedin.venice.status.protocol.PushJobDetails;
import com.linkedin.venice.status.protocol.PushJobStatusRecordKey;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.mockito.MockedStatic;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class PushJobDetailsManagerTest {
  private static final String CLUSTER_NAME = "test-cluster";
  private static final int VALID_SCHEMA_ID = 7;

  private Admin admin;
  private D2Client d2Client;
  private PubSubTopicRepository pubSubTopicRepository;
  private TopicManager topicManager;
  private VeniceWriterFactory veniceWriterFactory;
  private PushJobDetailsManager manager;

  @BeforeMethod
  public void setUp() {
    admin = mock(Admin.class);
    d2Client = mock(D2Client.class);
    pubSubTopicRepository = new PubSubTopicRepository();
    topicManager = mock(TopicManager.class);
    veniceWriterFactory = mock(VeniceWriterFactory.class);

    when(admin.getPubSubTopicRepository()).thenReturn(pubSubTopicRepository);
    when(admin.getTopicManager()).thenReturn(topicManager);
    when(admin.getVeniceWriterFactory()).thenReturn(veniceWriterFactory);

    manager = new PushJobDetailsManager(admin, d2Client, false, CLUSTER_NAME, true, Optional.empty());
  }

  @Test
  public void testGetPushJobDetailsSerializerReturnsNonNull() {
    assertNotNull(manager.getPushJobDetailsSerializer());
  }

  @Test
  public void testWriteToLocalRTTopicThrowsWhenStoreClusterBlankAndMultiRegion() {
    PushJobDetailsManager m =
        new PushJobDetailsManager(admin, d2Client, false, "", /*multiRegion*/ true, Optional.empty());
    VeniceException ex = expectThrows(
        VeniceException.class,
        () -> m.writeToLocalRTTopic(new PushJobStatusRecordKey(), new PushJobDetails()));
    assertTrue(
        ex.getMessage().contains("Unable to send the push job details"),
        "Unexpected exception message: " + ex.getMessage());
  }

  @Test
  public void testWriteToLocalRTTopicHappyPathReusesWriterAndCachesTopic() {
    PubSubTopic rtTopic = expectedRTTopic();
    when(topicManager.containsTopicAndAllPartitionsAreOnline(rtTopic)).thenReturn(true);
    when(admin.isLeaderControllerFor(CLUSTER_NAME)).thenReturn(true);
    when(admin.getValueSchemaId(eq(CLUSTER_NAME), eq(rtTopic.getStoreName()), any())).thenReturn(VALID_SCHEMA_ID);

    VeniceWriter writer = mock(VeniceWriter.class);
    when(veniceWriterFactory.createVeniceWriter(any())).thenReturn(writer);

    PushJobStatusRecordKey key = new PushJobStatusRecordKey();
    PushJobDetails value = new PushJobDetails();

    manager.writeToLocalRTTopic(key, value);
    manager.writeToLocalRTTopic(key, value);

    // Writer + schema-id resolution + topic lookup all happen once and are cached for the second call.
    verify(veniceWriterFactory, times(1)).createVeniceWriter(any());
    verify(admin, times(1)).getValueSchemaId(eq(CLUSTER_NAME), eq(rtTopic.getStoreName()), any());
    verify(topicManager, times(1)).containsTopicAndAllPartitionsAreOnline(rtTopic);
    verify(writer, times(2)).put(eq(key), eq(value), eq(VALID_SCHEMA_ID), isNull());
  }

  @Test
  public void testWriteToLocalRTTopicThrowsWhenTopicNeverAppears() {
    PubSubTopic rtTopic = expectedRTTopic();
    when(topicManager.containsTopicAndAllPartitionsAreOnline(rtTopic)).thenReturn(false);

    try (MockedStatic<Utils> utilsMock = mockStatic(Utils.class, CALLS_REAL_METHODS)) {
      // Skip the retry backoff so the test stays fast.
      utilsMock.when(() -> Utils.sleep(anyLong())).thenReturn(true);

      VeniceException ex = expectThrows(
          VeniceException.class,
          () -> manager.writeToLocalRTTopic(new PushJobStatusRecordKey(), new PushJobDetails()));
      assertTrue(ex.getMessage().contains("not found"), "Unexpected exception message: " + ex.getMessage());
    }

    // Loop should have made every allowed attempt.
    verify(topicManager, times(VeniceHelixAdmin.INTERNAL_STORE_GET_RRT_TOPIC_ATTEMPTS))
        .containsTopicAndAllPartitionsAreOnline(rtTopic);
    verify(veniceWriterFactory, never()).createVeniceWriter(any());
  }

  @Test
  public void testWriteToLocalRTTopicThrowsInvalidSchemaWhenLeaderHasNoMatch() {
    PubSubTopic rtTopic = expectedRTTopic();
    when(topicManager.containsTopicAndAllPartitionsAreOnline(rtTopic)).thenReturn(true);
    when(admin.isLeaderControllerFor(CLUSTER_NAME)).thenReturn(true);
    when(admin.getValueSchemaId(eq(CLUSTER_NAME), eq(rtTopic.getStoreName()), any()))
        .thenReturn(SchemaData.INVALID_VALUE_SCHEMA_ID);

    assertThrows(
        InvalidVeniceSchemaException.class,
        () -> manager.writeToLocalRTTopic(new PushJobStatusRecordKey(), new PushJobDetails()));
    verify(veniceWriterFactory, never()).createVeniceWriter(any());
  }

  @Test
  public void testWriteToLocalRTTopicUsesRemoteControllerWhenNotLeader() {
    PubSubTopic rtTopic = expectedRTTopic();
    when(topicManager.containsTopicAndAllPartitionsAreOnline(rtTopic)).thenReturn(true);
    when(admin.isLeaderControllerFor(CLUSTER_NAME)).thenReturn(false);
    Instance leader = mock(Instance.class);
    when(leader.getUrl(false)).thenReturn("http://leader");
    when(admin.getLeaderController(CLUSTER_NAME)).thenReturn(leader);

    VeniceWriter writer = mock(VeniceWriter.class);
    when(veniceWriterFactory.createVeniceWriter(any())).thenReturn(writer);

    ControllerClient controllerClient = mock(ControllerClient.class);
    SchemaResponse okResponse = new SchemaResponse();
    okResponse.setId(VALID_SCHEMA_ID);
    when(controllerClient.getValueSchemaID(eq(rtTopic.getStoreName()), any())).thenReturn(okResponse);

    PushJobStatusRecordKey key = new PushJobStatusRecordKey();
    PushJobDetails value = new PushJobDetails();
    try (MockedStatic<ControllerClient> controllerClientMock = mockStatic(ControllerClient.class)) {
      controllerClientMock
          .when(() -> ControllerClient.constructClusterControllerClient(eq(CLUSTER_NAME), eq("http://leader"), any()))
          .thenReturn(controllerClient);

      manager.writeToLocalRTTopic(key, value);
    }

    verify(writer).put(eq(key), eq(value), eq(VALID_SCHEMA_ID), isNull());
    verify(admin, never()).getValueSchemaId(any(), any(), any());
  }

  @Test
  public void testWriteToLocalRTTopicThrowsWhenRemoteControllerErrors() {
    PubSubTopic rtTopic = expectedRTTopic();
    when(topicManager.containsTopicAndAllPartitionsAreOnline(rtTopic)).thenReturn(true);
    when(admin.isLeaderControllerFor(CLUSTER_NAME)).thenReturn(false);
    Instance leader = mock(Instance.class);
    when(leader.getUrl(false)).thenReturn("http://leader");
    when(admin.getLeaderController(CLUSTER_NAME)).thenReturn(leader);

    ControllerClient controllerClient = mock(ControllerClient.class);
    SchemaResponse errorResponse = new SchemaResponse();
    errorResponse.setError("boom");
    when(controllerClient.getValueSchemaID(eq(rtTopic.getStoreName()), any())).thenReturn(errorResponse);

    try (MockedStatic<ControllerClient> controllerClientMock = mockStatic(ControllerClient.class)) {
      controllerClientMock
          .when(() -> ControllerClient.constructClusterControllerClient(eq(CLUSTER_NAME), eq("http://leader"), any()))
          .thenReturn(controllerClient);

      VeniceException ex = expectThrows(
          VeniceException.class,
          () -> manager.writeToLocalRTTopic(new PushJobStatusRecordKey(), new PushJobDetails()));
      assertTrue(ex.getMessage().contains("boom"), "Unexpected exception message: " + ex.getMessage());
    }

    verify(veniceWriterFactory, never()).createVeniceWriter(any());
  }

  @Test
  public void testGetPushJobDetailsThrowsOnNullKey() {
    assertThrows(IllegalArgumentException.class, () -> manager.getPushJobDetails(null));
  }

  @Test
  public void testGetPushJobDetailsReturnsValueFromStoreClient() throws Exception {
    AvroSpecificStoreClient<PushJobStatusRecordKey, PushJobDetails> client = mock(AvroSpecificStoreClient.class);
    PushJobStatusRecordKey key = new PushJobStatusRecordKey();
    PushJobDetails expected = new PushJobDetails();
    when(client.get(key)).thenReturn(CompletableFuture.completedFuture(expected));
    manager.setPushJobDetailsStoreClient(client);

    assertSame(manager.getPushJobDetails(key), expected);
  }

  @Test
  public void testGetPushJobDetailsLazilyInitializesClient() {
    AvroSpecificStoreClient<PushJobStatusRecordKey, PushJobDetails> client = mock(AvroSpecificStoreClient.class);
    PushJobStatusRecordKey key = new PushJobStatusRecordKey();
    PushJobDetails expected = new PushJobDetails();
    when(client.get(key)).thenReturn(CompletableFuture.completedFuture(expected));

    when(admin.discoverCluster(any())).thenReturn(CLUSTER_NAME);
    when(admin.getRouterD2Service(CLUSTER_NAME)).thenReturn("d2://router");

    try (MockedStatic<ClientFactory> clientFactoryMock = mockStatic(ClientFactory.class)) {
      clientFactoryMock.when(() -> ClientFactory.getAndStartSpecificAvroClient(any())).thenReturn(client);

      assertSame(manager.getPushJobDetails(key), expected);
      // Second invocation should reuse the cached client and not invoke the factory again.
      assertSame(manager.getPushJobDetails(key), expected);
      clientFactoryMock.verify(() -> ClientFactory.getAndStartSpecificAvroClient(any()), times(1));
    }
  }

  @Test
  public void testGetPushJobDetailsWrapsClientFailureAsVeniceException() {
    AvroSpecificStoreClient<PushJobStatusRecordKey, PushJobDetails> client = mock(AvroSpecificStoreClient.class);
    PushJobStatusRecordKey key = new PushJobStatusRecordKey();
    CompletableFuture<PushJobDetails> failed = new CompletableFuture<>();
    failed.completeExceptionally(new RuntimeException("downstream failure"));
    when(client.get(key)).thenReturn(failed);
    manager.setPushJobDetailsStoreClient(client);

    VeniceException ex = expectThrows(VeniceException.class, () -> manager.getPushJobDetails(key));
    assertTrue(ex.getCause() instanceof ExecutionException, "Expected ExecutionException cause");
  }

  @Test
  public void testGetBatchJobHeartbeatValueThrowsOnNullKey() {
    assertThrows(IllegalArgumentException.class, () -> manager.getBatchJobHeartbeatValue(null));
  }

  @Test
  public void testGetBatchJobHeartbeatValueLazilyInitializesClient() throws Exception {
    AvroSpecificStoreClient<BatchJobHeartbeatKey, BatchJobHeartbeatValue> client = mock(AvroSpecificStoreClient.class);
    BatchJobHeartbeatKey key = new BatchJobHeartbeatKey();
    BatchJobHeartbeatValue expected = new BatchJobHeartbeatValue();
    when(client.get(key)).thenReturn(CompletableFuture.completedFuture(expected));

    when(admin.discoverCluster(any())).thenReturn(CLUSTER_NAME);
    when(admin.getRouterD2Service(CLUSTER_NAME)).thenReturn("d2://router");

    try (MockedStatic<ClientFactory> clientFactoryMock = mockStatic(ClientFactory.class)) {
      clientFactoryMock.when(() -> ClientFactory.getAndStartSpecificAvroClient(any())).thenReturn(client);

      assertSame(manager.getBatchJobHeartbeatValue(key), expected);

      // Second invocation should reuse the cached client and not invoke the factory again.
      assertSame(manager.getBatchJobHeartbeatValue(key), expected);
      clientFactoryMock.verify(() -> ClientFactory.getAndStartSpecificAvroClient(any()), times(1));
    }
  }

  @Test
  public void testGetBatchJobHeartbeatValueWrapsClientFailureAsVeniceException() {
    AvroSpecificStoreClient<BatchJobHeartbeatKey, BatchJobHeartbeatValue> client = mock(AvroSpecificStoreClient.class);
    BatchJobHeartbeatKey key = new BatchJobHeartbeatKey();
    CompletableFuture<BatchJobHeartbeatValue> failed = new CompletableFuture<>();
    failed.completeExceptionally(new RuntimeException("downstream failure"));
    when(client.get(key)).thenReturn(failed);

    when(admin.discoverCluster(any())).thenReturn(CLUSTER_NAME);
    when(admin.getRouterD2Service(CLUSTER_NAME)).thenReturn("d2://router");

    try (MockedStatic<ClientFactory> clientFactoryMock = mockStatic(ClientFactory.class)) {
      clientFactoryMock.when(() -> ClientFactory.getAndStartSpecificAvroClient(any())).thenReturn(client);

      VeniceException ex = expectThrows(VeniceException.class, () -> manager.getBatchJobHeartbeatValue(key));
      assertTrue(ex.getCause() instanceof ExecutionException, "Expected ExecutionException cause");
    }
  }

  @Test
  public void testCloseClosesWritersAndStoreClients() {
    // Populate the writer map by running one successful write.
    PubSubTopic rtTopic = expectedRTTopic();
    when(topicManager.containsTopicAndAllPartitionsAreOnline(rtTopic)).thenReturn(true);
    when(admin.isLeaderControllerFor(CLUSTER_NAME)).thenReturn(true);
    when(admin.getValueSchemaId(eq(CLUSTER_NAME), eq(rtTopic.getStoreName()), any())).thenReturn(VALID_SCHEMA_ID);
    VeniceWriter writer = mock(VeniceWriter.class);
    when(veniceWriterFactory.createVeniceWriter(any())).thenReturn(writer);
    manager.writeToLocalRTTopic(new PushJobStatusRecordKey(), new PushJobDetails());

    AvroSpecificStoreClient<PushJobStatusRecordKey, PushJobDetails> pushJobDetailsClient =
        mock(AvroSpecificStoreClient.class);
    manager.setPushJobDetailsStoreClient(pushJobDetailsClient);

    AvroSpecificStoreClient<BatchJobHeartbeatKey, BatchJobHeartbeatValue> heartbeatClient =
        mock(AvroSpecificStoreClient.class);
    BatchJobHeartbeatKey heartbeatKey = new BatchJobHeartbeatKey();
    when(heartbeatClient.get(heartbeatKey)).thenReturn(CompletableFuture.completedFuture(new BatchJobHeartbeatValue()));
    when(admin.discoverCluster(any())).thenReturn(CLUSTER_NAME);
    when(admin.getRouterD2Service(CLUSTER_NAME)).thenReturn("d2://router");
    try (MockedStatic<ClientFactory> clientFactoryMock = mockStatic(ClientFactory.class)) {
      clientFactoryMock.when(() -> ClientFactory.getAndStartSpecificAvroClient(any())).thenReturn(heartbeatClient);
      manager.getBatchJobHeartbeatValue(heartbeatKey);
    }

    manager.close();

    verify(writer).close();
    verify(pushJobDetailsClient).close();
    verify(heartbeatClient).close();
  }

  /**
   * Mirrors how {@link PushJobDetailsManager#writeToLocalRTTopic} composes the expected
   * push-job-details RT topic, so tests can stub the topic-manager check against the exact instance.
   */
  private PubSubTopic expectedRTTopic() {
    String storeName = VeniceSystemStoreUtils.getPushJobDetailsStoreName();
    return pubSubTopicRepository.getTopic(Utils.composeRealTimeTopic(storeName));
  }
}
