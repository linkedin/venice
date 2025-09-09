package com.linkedin.venice.controller.multitaskscheduler;

import static com.linkedin.venice.controller.multitaskscheduler.MigrationRecord.Step;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.StoreMigrationResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.TrackableControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class StoreMigrationTaskTest {
  @Mock
  private MigrationRecord mockRecord;
  @Mock
  private StoreMigrationManager mockManager;
  @Mock
  private ControllerClient mocksrcControllerClient;
  @Mock
  private ControllerClient mockDestControllerClient;
  @Mock
  private D2ControllerClient mockD2srcControllerClient;
  @Mock
  private D2ControllerClient mockD2DestControllerClient;

  private StoreMigrationTask task;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    Map<String, ControllerClient> srcChildControllerClientMap = new HashMap<>();
    srcChildControllerClientMap.put("fabric1", mockD2srcControllerClient);
    Map<String, ControllerClient> destChildControllerClientMap = new HashMap<>();
    destChildControllerClientMap.put("fabric1", mockD2DestControllerClient);

    task = new StoreMigrationTask(
        mockRecord,
        mockManager,
        mocksrcControllerClient,
        mockDestControllerClient,
        srcChildControllerClientMap,
        destChildControllerClientMap,
        Arrays.asList("fabric1"));
  }

  @Test
  public void testRun_CheckDiskSpace() {
    when(mockRecord.getCurrentStep()).thenReturn(0);
    when(mockRecord.getCurrentStepEnum()).thenReturn(Step.CHECK_DISK_SPACE);

    task.run();

    verify(mockRecord).setCurrentStep(Step.PRE_CHECK_AND_SUBMIT_MIGRATION_REQUEST);
    verify(mockRecord).resetAttempts();
    verify(mockManager).scheduleNextStep(same(task), eq(0));
  }

  @Test
  public void testRun_PreCheckAndSubmitMigrationRequest() {
    String storeName = "testMigrateStore";
    String destCluster = "destCluster";
    when(mockRecord.getStoreName()).thenReturn(storeName);
    when(mockRecord.getDestinationCluster()).thenReturn(destCluster);
    when(mockRecord.getCurrentStep()).thenReturn(1);
    when(mockRecord.getCurrentStepEnum()).thenReturn(Step.PRE_CHECK_AND_SUBMIT_MIGRATION_REQUEST);
    when(mockRecord.getPauseAfter()).thenReturn(Step.NONE);
    when(mockRecord.getCurrentStep()).thenReturn(1);
    when(mockRecord.getCurrentStepEnum()).thenReturn(Step.PRE_CHECK_AND_SUBMIT_MIGRATION_REQUEST);
    when(mockRecord.getPauseAfter()).thenReturn(Step.NONE);

    StoreResponse srcStoreResponse = new StoreResponse();
    StoreInfo srcStoreInfo = createStoreInfoWithVersion(storeName, 1);
    srcStoreResponse.setStore(srcStoreInfo);
    when(mocksrcControllerClient.getStore(storeName)).thenReturn(srcStoreResponse);

    StoreMigrationResponse storeMigrationResponse = new StoreMigrationResponse();
    when(mocksrcControllerClient.migrateStore(storeName, destCluster)).thenReturn(storeMigrationResponse);

    task.run();

    verify(mockRecord).setCurrentStep(Step.VERIFY_MIGRATION_STATUS);
    verify(mockRecord).resetAttempts();
    verify(mockManager).scheduleNextStep(same(task), eq(0));
  }

  @Test
  public void testRun_VerifyMigrationStatus_Timeout() {
    String storeName = "testVerifyMigrationStatusStore";
    when(mockRecord.getCurrentStep()).thenReturn(2);
    when(mockRecord.getCurrentStepEnum()).thenReturn(Step.VERIFY_MIGRATION_STATUS);
    when(mockRecord.getPauseAfter()).thenReturn(Step.NONE);

    when(mockRecord.getStoreMigrationStartTime()).thenReturn(Instant.now().minus(Duration.ofDays(2)));
    when(mockRecord.getStoreName()).thenReturn(storeName);
    when(mockRecord.getAbortOnFailure()).thenReturn(true);

    StoreResponse srcChildStoreResponse = new StoreResponse();
    srcChildStoreResponse.setStore(createStoreInfoWithVersion(storeName, 9));
    when(mockD2srcControllerClient.getStore(storeName)).thenReturn(srcChildStoreResponse);

    StoreResponse destChildStoreResponse = new StoreResponse();
    when(mockD2DestControllerClient.getStore(storeName)).thenReturn(destChildStoreResponse);

    StoreResponse srcStoreResponse = new StoreResponse();
    when(mocksrcControllerClient.getStore(storeName)).thenReturn(srcStoreResponse);

    task.setTimeout(0); // Set timeout to zero to force a timeout during the test
    task.run();

    verify(mockManager).cleanupMigrationRecord(mockRecord.getStoreName());
    verify(mockRecord).setIsAborted(true);
  }

  @Test
  public void testRun_VerifyMigrationStatus_NoTimeout_NotVerified() {
    String storeName = "testVerifyMigrationStatusStore";
    when(mockRecord.getCurrentStep()).thenReturn(2);
    when(mockRecord.getCurrentStepEnum()).thenReturn(Step.VERIFY_MIGRATION_STATUS);
    when(mockRecord.getStoreMigrationStartTime()).thenReturn(Instant.now());
    when(mockRecord.getStoreName()).thenReturn(storeName);
    when(mockRecord.getPauseAfter()).thenReturn(Step.NONE);
    when(mockManager.getDelayInSeconds()).thenReturn(10);

    StoreResponse srcStoreResponse = new StoreResponse();
    srcStoreResponse.setStore(createStoreInfoWithVersion(storeName, 9));
    when(mockD2srcControllerClient.getStore(storeName)).thenReturn(srcStoreResponse);

    StoreResponse destStoreResponse = new StoreResponse();
    when(mockD2DestControllerClient.getStore(storeName)).thenReturn(destStoreResponse);

    task.run();

    verify(mockManager, never()).cleanupMigrationRecord(mockRecord.getStoreName());
    verify(mockRecord, never()).setCurrentStep(Step.UPDATE_CLUSTER_DISCOVERY);
    verify(mockRecord, never()).resetAttempts();
    verify(mockManager).scheduleNextStep(same(task), eq(10));
  }

  @Test
  public void testRun_VerifyMigrationStatus_NoTimeout_Verified() {
    String storeName = "testVerifyMigrationStatusStore";
    when(mockRecord.getCurrentStep()).thenReturn(2);
    when(mockRecord.getCurrentStepEnum()).thenReturn(Step.VERIFY_MIGRATION_STATUS);
    when(mockRecord.getStoreMigrationStartTime()).thenReturn(Instant.now());
    when(mockRecord.getStoreName()).thenReturn(storeName);
    when(mockRecord.getPauseAfter()).thenReturn(Step.NONE);

    StoreResponse srcStoreResponse = new StoreResponse();
    srcStoreResponse.setStore(createStoreInfoWithVersion(storeName, 9));
    when(mockD2srcControllerClient.getStore(storeName)).thenReturn(srcStoreResponse);

    StoreResponse destStoreResponse = new StoreResponse();
    destStoreResponse.setStore(createStoreInfoWithVersion(storeName, 10));
    when(mockD2DestControllerClient.getStore(storeName)).thenReturn(destStoreResponse);

    task.run();

    verify(mockManager, never()).cleanupMigrationRecord(mockRecord.getStoreName());
    verify(mockRecord).setCurrentStep(Step.UPDATE_CLUSTER_DISCOVERY);
    verify(mockRecord).resetAttempts();
    verify(mockManager).scheduleNextStep(same(task), eq(1));
  }

  @Test
  public void testRun_UpdateClusterDiscovery_NotUpdateSucessful() {
    String storeName = "testUpdateClusterStore";
    when(mockRecord.getCurrentStep()).thenReturn(2);
    when(mockRecord.getCurrentStepEnum()).thenReturn(Step.VERIFY_MIGRATION_STATUS);
    when(mockRecord.getStoreMigrationStartTime()).thenReturn(Instant.now());
    when(mockRecord.getStoreName()).thenReturn(storeName);
    when(mockRecord.getPauseAfter()).thenReturn(Step.NONE);

    StoreResponse srcStoreResponse = new StoreResponse();
    srcStoreResponse.setStore(createStoreInfoWithoutOnlineVersion(storeName));
    when(mockD2srcControllerClient.getStore(storeName)).thenReturn(srcStoreResponse);

    StoreResponse destStoreResponse = new StoreResponse();
    destStoreResponse.setStore(createStoreInfoWithoutOnlineVersion(storeName));
    when(mockD2DestControllerClient.getStore(storeName)).thenReturn(destStoreResponse);

    task.run(); // Run the task to set up statusVerified as true and fabricReadyMap

    verify(mockManager, never()).cleanupMigrationRecord(mockRecord.getStoreName());
    verify(mockRecord).setCurrentStep(Step.UPDATE_CLUSTER_DISCOVERY);
    verify(mockRecord).resetAttempts();
    verify(mockManager).scheduleNextStep(same(task), eq(0));

    when(mockRecord.getCurrentStep()).thenReturn(3);
    when(mockRecord.getCurrentStepEnum()).thenReturn(Step.UPDATE_CLUSTER_DISCOVERY);
    when(mockRecord.getDestinationCluster()).thenReturn("destCluster");
    when(mockRecord.getSourceCluster()).thenReturn("sourceCluster");
    when(mockRecord.getStoreName()).thenReturn(storeName);

    D2ServiceDiscoveryResponse d2ServiceDiscoveryResponse = new D2ServiceDiscoveryResponse();
    d2ServiceDiscoveryResponse.setCluster("sourceCluster");
    when(mockD2DestControllerClient.discoverCluster(storeName)).thenReturn(d2ServiceDiscoveryResponse);

    StoreMigrationResponse storeMigrationResponse = new StoreMigrationResponse();
    when(mockD2srcControllerClient.completeMigration(storeName, "destCluster")).thenReturn(storeMigrationResponse);

    task.run();

    verify(mockRecord).incrementAttempts();
  }

  @Test
  public void testRun_UpdateClusterDiscovery_UpdateSucessful() {
    String storeName = "testUpdateClusterStore";
    when(mockRecord.getCurrentStep()).thenReturn(2);
    when(mockRecord.getCurrentStepEnum()).thenReturn(Step.VERIFY_MIGRATION_STATUS);
    when(mockRecord.getStoreMigrationStartTime()).thenReturn(Instant.now());
    when(mockRecord.getStoreName()).thenReturn(storeName);
    when(mockRecord.getPauseAfter()).thenReturn(Step.NONE);

    StoreResponse srcStoreResponse = new StoreResponse();
    srcStoreResponse.setStore(createStoreInfoWithoutOnlineVersion(storeName));
    when(mockD2srcControllerClient.getStore(storeName)).thenReturn(srcStoreResponse);

    StoreResponse destStoreResponse = new StoreResponse();
    destStoreResponse.setStore(createStoreInfoWithoutOnlineVersion(storeName));
    when(mockD2DestControllerClient.getStore(storeName)).thenReturn(destStoreResponse);

    task.run(); // Run the task to set up statusVerified as true and fabricReadyMap

    verify(mockManager, never()).cleanupMigrationRecord(mockRecord.getStoreName());
    verify(mockRecord).setCurrentStep(Step.UPDATE_CLUSTER_DISCOVERY);
    verify(mockRecord).resetAttempts();
    verify(mockManager).scheduleNextStep(same(task), eq(0));

    when(mockRecord.getCurrentStep()).thenReturn(3);
    when(mockRecord.getCurrentStepEnum()).thenReturn(Step.UPDATE_CLUSTER_DISCOVERY);
    when(mockRecord.getDestinationCluster()).thenReturn("destCluster");
    when(mockRecord.getSourceCluster()).thenReturn("sourceCluster");
    when(mockRecord.getStoreName()).thenReturn(storeName);

    D2ServiceDiscoveryResponse destChildControllerd2ServiceDiscoveryResponse = new D2ServiceDiscoveryResponse();
    destChildControllerd2ServiceDiscoveryResponse.setCluster("destCluster");
    when(mockD2DestControllerClient.discoverCluster(storeName))
        .thenReturn(destChildControllerd2ServiceDiscoveryResponse);

    D2ServiceDiscoveryResponse destControllerD2ServiceDiscoveryresponse = new D2ServiceDiscoveryResponse();
    destControllerD2ServiceDiscoveryresponse.setCluster("destCluster");
    when(mockDestControllerClient.discoverCluster(storeName)).thenReturn(destControllerD2ServiceDiscoveryresponse);

    task.run();

    verify(mockRecord).setCurrentStep(Step.VERIFY_READ_REDIRECTION);
    verify(mockRecord, times(2)).resetAttempts();
    verify(mockManager, times(2)).scheduleNextStep(same(task), eq(0));
  }

  @Test
  public void testRun_VerifyReadRedirection() {
    when(mockRecord.getCurrentStep()).thenReturn(4);
    when(mockRecord.getCurrentStepEnum()).thenReturn(Step.VERIFY_READ_REDIRECTION);
    when(mockRecord.getPauseAfter()).thenReturn(Step.NONE);

    task.run();

    verify(mockRecord).setCurrentStep(Step.END_MIGRATION);
    verify(mockRecord).resetAttempts();
    verify(mockManager).scheduleNextStep(same(task), eq(0));
  }

  @Test
  public void testRun_EndMigration() {
    String storeName = "testEndMigrationStore";
    String destCluster = "destCluster";
    when(mockRecord.getCurrentStep()).thenReturn(5);
    when(mockRecord.getCurrentStepEnum()).thenReturn(Step.END_MIGRATION);
    when(mockRecord.getStoreName()).thenReturn(storeName);
    when(mockRecord.getDestinationCluster()).thenReturn(destCluster);
    when(mockRecord.getPauseAfter()).thenReturn(Step.NONE);

    D2ServiceDiscoveryResponse destControllerD2ServiceDiscoveryresponse = new D2ServiceDiscoveryResponse();
    destControllerD2ServiceDiscoveryresponse.setCluster(destCluster);
    when(mockDestControllerClient.discoverCluster(storeName)).thenReturn(destControllerD2ServiceDiscoveryresponse);

    StoreResponse srcStoreResponse = new StoreResponse();
    StoreInfo srcStoreInfo = new StoreInfo();
    srcStoreInfo.setMigrating(true);
    srcStoreResponse.setStore(srcStoreInfo);
    when(mocksrcControllerClient.getStore(storeName)).thenReturn(srcStoreResponse);

    TrackableControllerResponse srcDeleteResponse = new TrackableControllerResponse();
    when(mocksrcControllerClient.deleteStore(storeName, true)).thenReturn(srcDeleteResponse);

    StoreResponse childSrcStoreResponse = new StoreResponse();
    childSrcStoreResponse.setError("Store not found");
    childSrcStoreResponse.setErrorType(ErrorType.STORE_NOT_FOUND);
    when(mockD2srcControllerClient.getStore(storeName)).thenReturn(childSrcStoreResponse);

    ControllerResponse resetMigrationFlagResponse = new ControllerResponse();
    when(mockDestControllerClient.updateStore(eq(storeName), any(UpdateStoreQueryParams.class)))
        .thenReturn(resetMigrationFlagResponse);

    task.run();

    verify(mockRecord).setCurrentStep(Step.MIGRATION_SUCCEED);
    verify(mockManager).cleanupMigrationRecord(mockRecord.getStoreName());
  }

  @Test
  public void testRun_InvalidMigrationRecordStep() {
    when(mockRecord.getCurrentStepEnum()).thenReturn(Step.MIGRATION_SUCCEED); // Invalid step
    when(mockRecord.getAttempts()).thenReturn(0);
    when(mockManager.getMaxRetryAttempts()).thenReturn(3);

    task.run();

    verify(mockManager).cleanupMigrationRecord(mockRecord.getStoreName());
  }

  @Test
  public void testRun_ExceptionHandling_Retry() {
    when(mockRecord.getCurrentStepEnum()).thenThrow(new RuntimeException("Test exception"));
    when(mockRecord.getAttempts()).thenReturn(0);
    when(mockManager.getMaxRetryAttempts()).thenReturn(3);

    task.run();

    verify(mockRecord).incrementAttempts();
    verify(mockManager).scheduleNextStep(any(StoreMigrationTask.class), eq(0));
  }

  @Test
  public void testRun_ExceptionHandling_Abort_BeforeMigration() {
    String storeName = "testExpHandlingAbortBeforeMigrationStore";
    when(mockRecord.getCurrentStepEnum()).thenThrow(new RuntimeException("Test exception"))
        .thenReturn(Step.PRE_CHECK_AND_SUBMIT_MIGRATION_REQUEST);
    when(mockRecord.getAttempts()).thenReturn(3);
    when(mockRecord.getStoreName()).thenReturn(storeName);
    when(mockManager.getMaxRetryAttempts()).thenReturn(3);
    when(mockRecord.getAbortOnFailure()).thenReturn(true);

    task.run();
    verify(mockRecord, never()).setIsAborted(false);
    verify(mockManager).cleanupMigrationRecord(mockRecord.getStoreName());
  }

  @Test
  public void testRun_ExceptionHandling_Abort_EndMigration() {
    String storeName = "testExpHandlingAbortStore";
    when(mockRecord.getCurrentStepEnum()).thenThrow(new RuntimeException("Test exception"))
        .thenReturn(Step.END_MIGRATION);
    when(mockRecord.getAttempts()).thenReturn(3);
    when(mockRecord.getStoreName()).thenReturn(storeName);
    when(mockManager.getMaxRetryAttempts()).thenReturn(3);
    when(mockRecord.getAbortOnFailure()).thenReturn(true);

    VeniceException exp = expectThrows(VeniceException.class, () -> task.run());
    assertEquals(
        exp.getMessage(),
        "Migration for store testExpHandlingAbortStore on step END_MIGRATION. Cannot abort migration on or after end migration.");
  }

  @Test
  public void testRun_ExceptionHandling_Abort_IsMigration() {
    String storeName = "testExpHandlingAbortStore";
    when(mockRecord.getCurrentStepEnum()).thenThrow(new RuntimeException("Test exception"))
        .thenReturn(Step.VERIFY_MIGRATION_STATUS);
    when(mockRecord.getAttempts()).thenReturn(3);
    when(mockRecord.getStoreName()).thenReturn(storeName);
    when(mockManager.getMaxRetryAttempts()).thenReturn(3);
    when(mockRecord.getAbortOnFailure()).thenReturn(true);
    when(mockRecord.getSourceCluster()).thenReturn("srcCluster");

    StoreResponse srcStoreResponse = new StoreResponse();
    StoreInfo srcStoreInfo = createStoreInfoWithVersion(storeName, 1);
    srcStoreResponse.setStore(srcStoreInfo);
    when(mocksrcControllerClient.getStore(storeName)).thenReturn(srcStoreResponse);

    VeniceException exp = expectThrows(VeniceException.class, () -> task.run());
    assertEquals(
        exp.getMessage(),
        "Store testExpHandlingAbortStore is not in migration state on source cluster srcCluster on step VERIFY_MIGRATION_STATUS. Cannot abort migration.");
  }

  @Test
  public void testRun_ExceptionHandling_Abort_NotDiscoverableOnSrc() {
    String storeName = "testExpHandlingAbortStore";
    String srcCluster = "srcCluster";
    when(mockRecord.getCurrentStepEnum()).thenThrow(new RuntimeException("Test exception"))
        .thenReturn(Step.VERIFY_MIGRATION_STATUS);
    when(mockRecord.getAttempts()).thenReturn(3);
    when(mockRecord.getStoreName()).thenReturn(storeName);
    when(mockManager.getMaxRetryAttempts()).thenReturn(3);
    when(mockRecord.getAbortOnFailure()).thenReturn(true);
    when(mockRecord.getSourceCluster()).thenReturn(srcCluster);

    StoreResponse srcStoreResponse = new StoreResponse();
    StoreInfo srcStoreInfo = createStoreInfoWithVersion(storeName, 1);
    srcStoreInfo.setMigrating(true); // Simulate that the store is not discoverable on source
    srcStoreResponse.setStore(srcStoreInfo);
    when(mocksrcControllerClient.getStore(storeName)).thenReturn(srcStoreResponse);

    D2ServiceDiscoveryResponse discoveryResponse = new D2ServiceDiscoveryResponse();
    discoveryResponse.setCluster("NotSrcCluster");
    when(mocksrcControllerClient.discoverCluster(storeName)).thenReturn(discoveryResponse);

    VeniceException exp = expectThrows(VeniceException.class, () -> task.run());
    assertEquals(
        exp.getMessage(),
        "Store " + storeName + " discovered on cluster NotSrcCluster instead of source cluster srcCluster. "
            + "Either store migration has completed, or the internal states are messed up.");
  }

  @Test
  public void testRun_AbortMigration() {
    String storeName = "testAbortStore";
    String srcCluster = "srcCluster";
    String destCluster = "destCluster";
    when(mockRecord.getCurrentStepEnum()).thenThrow(new RuntimeException("Test exception"))
        .thenReturn(Step.VERIFY_MIGRATION_STATUS);
    when(mockRecord.getAttempts()).thenReturn(3);
    when(mockRecord.getStoreName()).thenReturn(storeName);
    when(mockManager.getMaxRetryAttempts()).thenReturn(3);
    when(mockRecord.getAbortOnFailure()).thenReturn(true);
    when(mockRecord.getSourceCluster()).thenReturn(srcCluster);
    when(mockRecord.getDestinationCluster()).thenReturn(destCluster);

    StoreResponse srcStoreResponse = new StoreResponse();
    StoreInfo srcStoreInfo = createStoreInfoWithVersion(storeName, 1);
    srcStoreInfo.setMigrating(true); // Simulate that the store is not discoverable on source
    srcStoreResponse.setStore(srcStoreInfo);
    when(mocksrcControllerClient.getStore(storeName)).thenReturn(srcStoreResponse);

    D2ServiceDiscoveryResponse discoveryResponse = new D2ServiceDiscoveryResponse();
    discoveryResponse.setCluster(srcCluster);
    when(mocksrcControllerClient.discoverCluster(storeName)).thenReturn(discoveryResponse);

    StoreMigrationResponse abortResponse = new StoreMigrationResponse();
    when(mocksrcControllerClient.abortMigration(storeName, destCluster)).thenReturn(abortResponse);

    StoreResponse destStoreResponse = new StoreResponse();
    StoreInfo destStoreInfo = createStoreInfoWithVersion(storeName, 2);
    destStoreInfo.setMigrating(true); // Simulate that the store is not discoverable on source
    destStoreResponse.setStore(srcStoreInfo);
    when(mockDestControllerClient.getStore(storeName)).thenReturn(destStoreResponse);

    TrackableControllerResponse deleteResponse = new TrackableControllerResponse();
    when(mockDestControllerClient.deleteStore(storeName, true)).thenReturn(deleteResponse);

    task.run();

    verify(mockRecord).setIsAborted(true);
    verify(mockManager).cleanupMigrationRecord(mockRecord.getStoreName());
  }

  // For stores with an online version
  private StoreInfo createStoreInfoWithVersion(String storeName, int versionNumber) {
    StoreInfo storeInfo = new StoreInfo();
    Version version = new VersionImpl(storeName, versionNumber, "pushJobId");
    version.setStatus(VersionStatus.ONLINE);
    storeInfo.setVersions(Collections.singletonList(version));
    return storeInfo;
  }

  // For stores without an online version
  private StoreInfo createStoreInfoWithoutOnlineVersion(String storeName) {
    StoreInfo storeInfo = new StoreInfo();
    storeInfo.setVersions(Collections.emptyList());
    return storeInfo;
  }
}
