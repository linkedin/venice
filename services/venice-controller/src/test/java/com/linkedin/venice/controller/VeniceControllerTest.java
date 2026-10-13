package com.linkedin.venice.controller;

import static com.linkedin.venice.ConfigKeys.CHILD_CLUSTER_ALLOWLIST;
import static com.linkedin.venice.ConfigKeys.CHILD_CLUSTER_URL_PREFIX;
import static com.linkedin.venice.ConfigKeys.CHILD_DATA_CENTER_KAFKA_URL_PREFIX;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_PARENT_MODE;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_PARENT_REGION_STATE;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_SSL_ENABLED;
import static com.linkedin.venice.ConfigKeys.LOCAL_REGION_NAME;
import static com.linkedin.venice.ConfigKeys.NATIVE_REPLICATION_FABRIC_ALLOWLIST;
import static com.linkedin.venice.controller.ParentControllerRegionState.ACTIVE;
import static com.linkedin.venice.controller.ParentControllerRegionState.PASSIVE;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.controller.kafka.TopicCleanupService;
import com.linkedin.venice.controller.server.AdminSparkServer;
import com.linkedin.venice.controller.systemstore.SystemStoreRepairService;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.servicediscovery.ServiceDiscoveryAnnouncer;
import com.linkedin.venice.status.protocol.PushJobDetails;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.function.BooleanSupplier;
import org.mockito.MockedConstruction;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class VeniceControllerTest {
  @DataProvider
  public Object[][] ownerReadinessCases() {
    return new Object[][] { { false, null }, { true, null }, { false, ACTIVE }, { false, PASSIVE } };
  }

  @Test(dataProvider = "ownerReadinessCases")
  public void testOwnerReadinessTracksStartupServicesAndShutdown(
      boolean sslEnabled,
      ParentControllerRegionState parentRegionState) {
    boolean parent = parentRegionState != null;
    boolean expectedReady = !parent || parentRegionState == ACTIVE;
    Properties properties = TestVeniceControllerClusterConfig.getBaseSingleRegionProperties(false);
    properties.putAll(SslUtils.getVeniceLocalSslProperties());
    properties.put(CONTROLLER_SSL_ENABLED, sslEnabled);
    if (parent) {
      String childRegion = properties.getProperty(LOCAL_REGION_NAME);
      properties.put(CONTROLLER_PARENT_MODE, true);
      properties.put(CONTROLLER_PARENT_REGION_STATE, parentRegionState.name());
      properties.put(CHILD_CLUSTER_ALLOWLIST, childRegion);
      properties.put(CHILD_CLUSTER_URL_PREFIX + childRegion, "http://localhost");
      properties.put(NATIVE_REPLICATION_FABRIC_ALLOWLIST, childRegion);
      properties.put(CHILD_DATA_CENTER_KAFKA_URL_PREFIX + "." + childRegion, "localhost:9092");
    }
    Admin admin = parent ? mock(VeniceParentHelixAdmin.class) : mock(VeniceHelixAdmin.class);
    Class<? extends AbstractVeniceService> cleanupServiceType =
        parent ? StoreGraveyardCleanupService.class : StoreBackupVersionCleanupService.class;
    Class<? extends AbstractVeniceService> maintenanceServiceType =
        parent ? UnusedValueSchemaCleanupService.class : DisabledPartitionEnablerService.class;
    ServiceDiscoveryAnnouncer announcer = mock(ServiceDiscoveryAnnouncer.class);
    List<BooleanSupplier> readinessSignals = new ArrayList<>();
    doAnswer(invocation -> {
      readinessSignals.forEach(signal -> assertFalse(signal.getAsBoolean()));
      return null;
    }).when(announcer).register();
    doAnswer(invocation -> {
      readinessSignals.forEach(signal -> assertFalse(signal.getAsBoolean()));
      return null;
    }).when(announcer).unregister();
    MetricsRepository metricsRepository = new MetricsRepository();
    try (
        InternalAvroSpecificSerializer<PushJobDetails> serializer =
            AvroProtocolDefinition.PUSH_JOB_DETAILS.getSerializer();
        MockedConstruction<VeniceControllerService> coreServices =
            mockConstruction(VeniceControllerService.class, (service, context) -> {
              when(service.getVeniceHelixAdmin()).thenReturn(admin);
              when(service.getPushJobDetailsSerializer()).thenReturn(serializer);
              when(service.isRunning()).thenReturn(true);
            });
        MockedConstruction<AdminSparkServer> listeners = mockConstruction(AdminSparkServer.class, (server, context) -> {
          assertSame(context.arguments().get(13), serializer);
          assertEquals(((Optional<?>) context.arguments().get(5)).isPresent(), context.getCount() == 2);
          readinessSignals.add((BooleanSupplier) context.arguments().get(14));
          when(server.isRunning()).thenReturn(true);
        });
        MockedConstruction<TopicCleanupService> topicCleanup = mockConstruction(TopicCleanupService.class);
        MockedConstruction<? extends AbstractVeniceService> cleanup = mockConstruction(cleanupServiceType);
        MockedConstruction<? extends AbstractVeniceService> maintenance = mockConstruction(maintenanceServiceType);
        MockedConstruction<SystemStoreRepairService> repair = mockConstruction(SystemStoreRepairService.class)) {
      VeniceController controller = new VeniceController(
          new VeniceControllerContext.Builder()
              .setPropertiesList(Collections.singletonList(new VeniceProperties(properties)))
              .setMetricsRepository(metricsRepository)
              .setServiceDiscoveryAnnouncers(Collections.singletonList(announcer))
              .build());
      try {
        assertEquals(readinessSignals.size(), sslEnabled ? 2 : 1);
        readinessSignals.forEach(signal -> assertFalse(signal.getAsBoolean()));
        clearInvocations(admin);
        doThrow(new IllegalStateException("startup failure")).doNothing()
            .when(topicCleanup.constructed().get(0))
            .start();
        expectThrows(IllegalStateException.class, controller::start);
        readinessSignals.forEach(signal -> assertFalse(signal.getAsBoolean()));
        controller.start();
        readinessSignals.forEach(signal -> assertEquals(signal.getAsBoolean(), expectedReady));
        List<AbstractVeniceService> requiredServices = new ArrayList<>(listeners.constructed());
        requiredServices.add(coreServices.constructed().get(0));
        for (AbstractVeniceService service: requiredServices) {
          when(service.isRunning()).thenReturn(false);
          readinessSignals.forEach(signal -> assertFalse(signal.getAsBoolean()));
          when(service.isRunning()).thenReturn(true);
          readinessSignals.forEach(signal -> assertEquals(signal.getAsBoolean(), expectedReady));
        }
        verifyNoInteractions(admin);
        VeniceControllerService coreService = coreServices.constructed().get(0);
        doAnswer(invocation -> {
          doReturn(true).when(coreService).isRunning();
          controller.stop();
          return true;
        }).when(coreService).isRunning();
        readinessSignals.forEach(signal -> assertFalse(signal.getAsBoolean()));
      } finally {
        controller.stop();
      }
      readinessSignals.forEach(signal -> assertFalse(signal.getAsBoolean()));
    } finally {
      metricsRepository.close();
    }
  }

  @Test
  public void testReadinessRequiresCompletedStartup() {
    VeniceController.ApiReadiness readiness = new VeniceController.ApiReadiness();

    assertFalse(readiness.isReady());
    readiness.markReady();
    assertTrue(readiness.isReady());
  }

  @Test
  public void testStoppingCannotBeUndoneByLateStartup() {
    VeniceController.ApiReadiness readiness = new VeniceController.ApiReadiness();
    readiness.markStopping();
    readiness.markReady();
    assertFalse(readiness.isReady());

    VeniceController.ApiReadiness started = new VeniceController.ApiReadiness();
    started.markReady();
    assertTrue(started.isReady());
    started.markStopping();
    assertFalse(started.isReady());
    started.markReady();
    assertFalse(started.isReady());
  }
}
