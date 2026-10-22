package com.linkedin.davinci.helix;

import static com.linkedin.venice.ConfigKeys.CLUSTER_PUSH_STATUS_STORE_DUAL_WRITE_ENABLED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.linkedin.davinci.config.VeniceClusterConfig;
import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.venice.common.PushStatusStoreUtils;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixPartitionStatusAccessor;
import com.linkedin.venice.helix.VeniceOfflinePushMonitorAccessor;
import com.linkedin.venice.helix.ZkClientFactory;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.pushstatus.PushStatusKey;
import com.linkedin.venice.pushstatushelper.PushStatusStoreWriter;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mockito.InOrder;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class HelixParticipationServiceTest {
  private static final Logger LOGGER = LogManager.getLogger(HelixParticipationServiceTest.class);

  @DataProvider
  public Object[][] dualWriteConfigurations() {
    return new Object[][] { { null }, { "false" }, { "true" } };
  }

  @Test(dataProvider = "dualWriteConfigurations")
  public void testPushStatusWriterHonorsDualWriteConfiguration(String enabled) throws Exception {
    try (WriterFixture fixture = new WriterFixture(enabled, 1);
        PushStatusStoreWriter writer =
            fixture.createService().createPushStatusStoreWriter(fixture.valueSchema, fixture.updateSchema)) {
      writer.writeHeartbeat("store", 42);
      PushStatusKey heartbeatKey =
          PushStatusStoreUtils.getHeartbeatKey(Utils.getHelixNodeIdentifier("localhost", 1234));
      verify(fixture.legacyWriter).update(eq(heartbeatKey), any(), eq(fixture.valueSchema.getId()), eq(1), eq(null));
      if (Boolean.parseBoolean(enabled)) {
        Assert.assertTrue(fixture.clusterUpdates.await(10, TimeUnit.SECONDS));
        verify(fixture.clusterWriter).update(
            eq(PushStatusStoreUtils.getClusterPushStatusKey("store", heartbeatKey)),
            any(),
            eq(fixture.valueSchema.getId()),
            eq(1),
            any());
      }
      verifyNoMoreInteractions(fixture.legacyWriter, fixture.clusterWriter);
      String legacyTopic = Utils.composeRealTimeTopic(VeniceSystemStoreUtils.getDaVinciPushStatusStoreName("store"));
      verify(fixture.writerFactory).createVeniceWriter(argThat(o -> o.getTopicName().equals(legacyTopic)));
      if (Boolean.parseBoolean(enabled)) {
        verify(fixture.writerFactory).createVeniceWriter(argThat(o -> o.getTopicName().equals(fixture.clusterTopic)));
      }
      verifyNoMoreInteractions(fixture.writerFactory);
    }
  }

  @Test
  public void testShutdownClosesOwnedDualWriteStatusWriterAfterIngestionStops() throws Exception {
    ZkClient zkClient = mock(ZkClient.class);
    try (WriterFixture fixture = new WriterFixture("true", 2);
        MockedConstruction<ZKHelixManager> managers =
            mockConstruction(ZKHelixManager.class, withSettings().defaultAnswer(RETURNS_DEEP_STUBS));
        MockedConstruction<ZKHelixAdmin> admins = mockConstruction(ZKHelixAdmin.class, (admin, context) -> {
          when(admin.getClusters()).thenReturn(Collections.singletonList("cluster"));
          when(admin.getInstancesInCluster("cluster")).thenReturn(Collections.singletonList("localhost_1234"));
        });
        MockedConstruction<VeniceOfflinePushMonitorAccessor> monitors =
            mockConstruction(VeniceOfflinePushMonitorAccessor.class);
        MockedConstruction<HelixPartitionStatusAccessor> accessors =
            mockConstruction(HelixPartitionStatusAccessor.class);
        MockedStatic<ZkClientFactory> zkClients = mockStatic(ZkClientFactory.class)) {
      zkClients.when(() -> ZkClientFactory.newZkClient("unused")).thenReturn(zkClient);
      CountDownLatch writersClosed = new CountDownLatch(2);
      for (VeniceWriter producer: new VeniceWriter[] { fixture.legacyWriter, fixture.clusterWriter }) {
        doAnswer(invocation -> {
          writersClosed.countDown();
          return null;
        }).when(producer).close();
      }
      HelixParticipationService service = fixture.createService();
      try {
        try (MockedStatic<CompletableFuture> futures = mockStatic(CompletableFuture.class)) {
          // Run startup on this thread so constructor mocks also cover the asynchronous Helix connection.
          futures.when(() -> CompletableFuture.runAsync(any(Runnable.class))).thenAnswer(invocation -> {
            ((Runnable) invocation.getArgument(0)).run();
            return new CompletableFuture<>();
          });
          service.start();
        }
        Assert.assertTrue(service.isRunning());
        PushStatusStoreWriter writer = service.getStatusStoreWriter();
        writer.writeHeartbeat("store", 42);
        doAnswer(invocation -> {
          writer.writeHeartbeat("store", 43);
          Assert.assertTrue(fixture.clusterUpdates.await(10, TimeUnit.SECONDS));
          Assert.assertEquals(writersClosed.getCount(), 2L);
          return null;
        }).when(fixture.ingestionService).stopInner();

        // Match VeniceServer's reverse service shutdown order.
        fixture.ingestionService.stopInner();
        service.stop();

        Assert.assertTrue(writersClosed.await(10, TimeUnit.SECONDS));
        Assert.assertFalse(service.isRunning());
        Assert.assertThrows(IllegalStateException.class, () -> writer.writeHeartbeat("store", 44));
        for (VeniceWriter producer: new VeniceWriter[] { fixture.legacyWriter, fixture.clusterWriter }) {
          InOrder shutdownOrder = inOrder(fixture.ingestionService, producer);
          shutdownOrder.verify(fixture.ingestionService).stopInner();
          shutdownOrder.verify(producer).close();
        }
        verify(zkClient).close();
      } finally {
        service.stop();
      }
    }
  }

  private static class WriterFixture implements AutoCloseable {
    final VeniceConfigLoader configLoader = mock(VeniceConfigLoader.class);
    final KafkaStoreIngestionService ingestionService = mock(KafkaStoreIngestionService.class);
    final ReadOnlySchemaRepository schemaRepository = mock(ReadOnlySchemaRepository.class);
    final MetricsRepository metricsRepository = new MetricsRepository();
    final VeniceWriterFactory writerFactory = mock(VeniceWriterFactory.class);
    final VeniceWriter legacyWriter = mock(VeniceWriter.class);
    final VeniceWriter clusterWriter = mock(VeniceWriter.class);
    final String clusterTopic =
        Utils.composeRealTimeTopic(VeniceSystemStoreUtils.getDaVinciPushStatusStoreNameForCluster("cluster"));
    final SchemaEntry valueSchema = new SchemaEntry(
        AvroProtocolDefinition.PUSH_STATUS_SYSTEM_SCHEMA_STORE.getCurrentProtocolVersion(),
        AvroProtocolDefinition.PUSH_STATUS_SYSTEM_SCHEMA_STORE.getCurrentProtocolVersionSchema());
    final DerivedSchemaEntry updateSchema = new DerivedSchemaEntry(
        valueSchema.getId(),
        1,
        WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchema.getSchema()));
    final CountDownLatch clusterUpdates;

    WriterFixture(String enabled, int expectedUpdates) {
      Properties properties = new Properties();
      if (enabled != null) {
        properties.setProperty(CLUSTER_PUSH_STATUS_STORE_DUAL_WRITE_ENABLED, enabled);
      }
      VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
      VeniceClusterConfig clusterConfig = mock(VeniceClusterConfig.class);
      when(configLoader.getCombinedProperties()).thenReturn(new VeniceProperties(properties));
      when(configLoader.getVeniceServerConfig()).thenReturn(serverConfig);
      when(configLoader.getVeniceClusterConfig()).thenReturn(clusterConfig);
      when(serverConfig.getMaxLeaderFollowerStateTransitionThreadNumber()).thenReturn(1);
      when(serverConfig.getLeaderFollowerThreadPoolStrategy())
          .thenReturn(LeaderFollowerPartitionStateModelFactory.LeaderFollowerThreadPoolStrategy.SINGLE_POOL_STRATEGY);
      when(clusterConfig.getRefreshAttemptsForZkReconnect()).thenReturn(1);
      when(schemaRepository.getSupersetOrLatestValueSchema(any())).thenReturn(valueSchema);
      when(schemaRepository.getLatestDerivedSchema(any(), eq(valueSchema.getId()))).thenReturn(updateSchema);
      when(ingestionService.getVeniceWriterFactory()).thenReturn(writerFactory);
      when(writerFactory.createVeniceWriter(any(VeniceWriterOptions.class))).thenAnswer(invocation -> {
        VeniceWriterOptions options = invocation.getArgument(0);
        boolean cluster = options.getTopicName().equals(clusterTopic);
        Assert.assertEquals(
            options.getPartitionCount().intValue(),
            cluster
                ? VeniceSystemStoreUtils.CLUSTER_PUSH_STATUS_STORE_PARTITION_COUNT
                : VeniceSystemStoreUtils.DEFAULT_USER_SYSTEM_STORE_PARTITION_COUNT);
        return cluster ? clusterWriter : legacyWriter;
      });
      clusterUpdates = new CountDownLatch(expectedUpdates);
      when(clusterWriter.update(any(), any(), eq(valueSchema.getId()), eq(1), any())).thenAnswer(invocation -> {
        clusterUpdates.countDown();
        return CompletableFuture.completedFuture(null);
      });
    }

    HelixParticipationService createService() {
      return new HelixParticipationService(
          ingestionService,
          mock(StorageService.class),
          mock(StorageMetadataService.class),
          configLoader,
          mock(ReadOnlyStoreRepository.class),
          schemaRepository,
          metricsRepository,
          "unused",
          "cluster",
          1234,
          "localhost",
          new CompletableFuture<>(),
          null,
          null);
    }

    @Override
    public void close() {
      metricsRepository.close();
    }
  }

  @Test
  public void testResetAllInstanceCVStates() {
    HelixPartitionStatusAccessor mockAccessor = mock(HelixPartitionStatusAccessor.class);
    StorageService mockStorageService = mock(StorageService.class);

    // Call the reset method
    HelixParticipationService.resetAllInstanceCVStates(mockAccessor, mockStorageService, LOGGER);

    // Verify that the bulk delete API is called instead of per-partition deletion
    verify(mockAccessor).deleteAllCustomizedStates();
  }

  @Test
  public void testResetAllInstanceCVStatesWithException() {
    HelixPartitionStatusAccessor mockAccessor = mock(HelixPartitionStatusAccessor.class);
    StorageService mockStorageService = mock(StorageService.class);

    // Make the delete operation throw an exception
    VeniceException testException = new VeniceException("Test exception during CV cleanup");
    doThrow(testException).when(mockAccessor).deleteAllCustomizedStates();

    HelixParticipationService.resetAllInstanceCVStates(mockAccessor, mockStorageService, LOGGER);

    verify(mockAccessor).deleteAllCustomizedStates();
  }

  @Test
  public void testUnknownHelixInstanceOperation() {
    HelixParticipationService mockHelixParticipationService = mock(HelixParticipationService.class);
    VeniceServerConfig mockServerConfig = mock(VeniceServerConfig.class);

    when(mockServerConfig.isHelixJoinAsUnknownEnabled()).thenReturn(true);

    doCallRealMethod().when(mockHelixParticipationService).buildHelixManagerProperty(mockServerConfig);
    mockHelixParticipationService.buildHelixManagerProperty(mockServerConfig);
  }
}
