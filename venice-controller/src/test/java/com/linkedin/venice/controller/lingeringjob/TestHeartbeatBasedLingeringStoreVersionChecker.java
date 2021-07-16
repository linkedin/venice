package com.linkedin.venice.controller.lingeringjob;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.status.protocol.BatchJobHeartbeatKey;
import com.linkedin.venice.status.protocol.BatchJobHeartbeatValue;
import com.linkedin.venice.status.protocol.PushJobDetails;
import com.linkedin.venice.utils.Time;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static com.linkedin.venice.status.BatchJobHeartbeatConfigs.*;
import static org.mockito.Mockito.*;


public class TestHeartbeatBasedLingeringStoreVersionChecker {
  private static final String STORE_NAME = "store_name";
  private static final int VERSION_NUMBER = 1;
  private static final Instant NOW = Instant.now();
  private static final String DEFAULT_PRINCIPAL_NAME = "principal_name";

  private Store store;
  private Version version;
  private Time time;

  @BeforeTest
  public void setup() {
    store = mock(Store.class);
    version = mock(Version.class);
    time = mock(Time.class);
    when(store.getName()).thenReturn(STORE_NAME);
    when(store.getCreatedTime()).thenReturn(NOW.toEpochMilli());
    when(store.getBootstrapToOnlineTimeoutInHours()).thenReturn(24);
    when(version.getNumber()).thenReturn(VERSION_NUMBER);
    when(version.getCreatedTime()).thenReturn(NOW.toEpochMilli());
    when(time.getMilliseconds()).thenReturn(NOW.toEpochMilli());
  }

  @Test
  public void testNoRequesterPrincipalProvided() {
    Duration heartbeatTimeout = Duration.ofMinutes(10);
    Duration initialHeartbeatBufferTime = Duration.ofMinutes(3);
    DefaultLingeringStoreVersionChecker defaultLingeringStoreVersionChecker = mock(DefaultLingeringStoreVersionChecker.class);
    when(defaultLingeringStoreVersionChecker.isStoreVersionLingering(any(), any(), any(), any(), any())). thenReturn(false);

    HeartbeatBasedLingeringStoreVersionChecker checker = new HeartbeatBasedLingeringStoreVersionChecker(
        heartbeatTimeout,
        initialHeartbeatBufferTime,
        defaultLingeringStoreVersionChecker,
        mock(HeartbeatBasedCheckerStats.class)
    );
    Assert.assertFalse(checker.isStoreVersionLingering(store, version, time, mock(Admin.class), Optional.empty()));
    // Expect fallback to use the default checker
    verify(defaultLingeringStoreVersionChecker, times(1)).isStoreVersionLingering(any(), any(), any(), any(), any());
  }

  @Test
  public void testNoWritePermissionToHeartbeatStore() {
    Duration heartbeatTimeout = Duration.ofMinutes(10);
    Duration initialHeartbeatBufferTime = Duration.ofMinutes(3);
    DefaultLingeringStoreVersionChecker defaultLingeringStoreVersionChecker = mock(DefaultLingeringStoreVersionChecker.class);
    when(defaultLingeringStoreVersionChecker.isStoreVersionLingering(any(), any(), any(), any(), any())). thenReturn(false);

    HeartbeatBasedLingeringStoreVersionChecker checker = new HeartbeatBasedLingeringStoreVersionChecker(
        heartbeatTimeout,
        initialHeartbeatBufferTime,
        defaultLingeringStoreVersionChecker,
        mock(HeartbeatBasedCheckerStats.class)
    );
    Admin admin = mock(Admin.class);
    String principal = "requester_principal";
    when(admin.hasWritePermissionToBatchJobHeartbeatStore(principal)).thenReturn(false);
    Assert.assertFalse(checker.isStoreVersionLingering(store, version, time, admin, Optional.of(principal)));
    // Expect fallback to use the default checker
    verify(defaultLingeringStoreVersionChecker, times(1)).isStoreVersionLingering(any(), any(), any(), any(), any());
  }

  @Test
  public void testGetPushJobDetailsReturnsNull() {
    verifyGetPushJobDetailsFailedCase(null);
  }

  @Test
  public void testGetPushJobDetailsReturnsNullConfig() {
    verifyGetPushJobDetailsFailedCase(new PushJobDetails());
  }

  @Test
  public void testGetPushJobDetailsReturnMissingConfig() {
    PushJobDetails pushJobDetails = new PushJobDetails();
    pushJobDetails.pushJobConfigs = Collections.emptyMap();
    verifyGetPushJobDetailsFailedCase(pushJobDetails);
  }

  private void verifyGetPushJobDetailsFailedCase(PushJobDetails expectedReturnValue) {
    Duration heartbeatTimeout = Duration.ofMinutes(10);
    Duration initialHeartbeatBufferTime = Duration.ofMinutes(3);
    DefaultLingeringStoreVersionChecker defaultLingeringStoreVersionChecker = mock(DefaultLingeringStoreVersionChecker.class);
    when(defaultLingeringStoreVersionChecker.isStoreVersionLingering(any(), any(), any(), any(), any())). thenReturn(false);

    HeartbeatBasedLingeringStoreVersionChecker checker = new HeartbeatBasedLingeringStoreVersionChecker(
        heartbeatTimeout,
        initialHeartbeatBufferTime,
        defaultLingeringStoreVersionChecker,
        mock(HeartbeatBasedCheckerStats.class)
    );
    Admin admin = mock(Admin.class);
    when(admin.hasWritePermissionToBatchJobHeartbeatStore(DEFAULT_PRINCIPAL_NAME)).thenReturn(true);
    when(admin.getPushJobDetails(any())).thenReturn(expectedReturnValue);
    Assert.assertFalse(checker.isStoreVersionLingering(store, version, time, admin, Optional.of(DEFAULT_PRINCIPAL_NAME)));
    // Expect fallback to use the default checker
    verify(defaultLingeringStoreVersionChecker, times(1)).isStoreVersionLingering(any(), any(), any(), any(), any());
  }

  @Test
  public void testGetHeartbeatValueFailed() {
    BatchJobHeartbeatKey batchJobHeartbeatKey = new BatchJobHeartbeatKey();
    batchJobHeartbeatKey.storeVersion = VERSION_NUMBER;
    batchJobHeartbeatKey.storeName = STORE_NAME;
    Admin mockAdmin = createCheckBatchJobHasHeartbeatAdmin();
    when(mockAdmin.getBatchJobHeartbeatValue(batchJobHeartbeatKey)).thenThrow(new VeniceException("Simulated exception"));

    verifyCheckBatchJobHasHeartbeat(
        Duration.ofMinutes(10),
        Duration.ofMinutes(0),
        mockAdmin,
        true
    );
  }

  @Test
  public void testGetNullHeartbeatValue() {
    BatchJobHeartbeatKey batchJobHeartbeatKey = new BatchJobHeartbeatKey();
    batchJobHeartbeatKey.storeVersion = VERSION_NUMBER;
    batchJobHeartbeatKey.storeName = STORE_NAME;
    Admin mockAdmin = createCheckBatchJobHasHeartbeatAdmin();
    when(mockAdmin.getBatchJobHeartbeatValue(batchJobHeartbeatKey)).thenReturn(null);

    verifyCheckBatchJobHasHeartbeat(
        Duration.ofMinutes(10),
        Duration.ofMinutes(0),
        mockAdmin,
        false // No/null heartbeat means the job is not alive
    );
  }

  @Test
  public void testGetHeartbeatValueToProveAliveness() {
    BatchJobHeartbeatKey batchJobHeartbeatKey = new BatchJobHeartbeatKey();
    batchJobHeartbeatKey.storeVersion = VERSION_NUMBER;
    batchJobHeartbeatKey.storeName = STORE_NAME;
    BatchJobHeartbeatValue batchJobHeartbeatValue = new BatchJobHeartbeatValue();
    batchJobHeartbeatValue.timestamp = NOW.toEpochMilli() - TimeUnit.MINUTES.toMillis(5); // 5 minutes ago
    Admin mockAdmin = createCheckBatchJobHasHeartbeatAdmin();
    when(mockAdmin.getBatchJobHeartbeatValue(batchJobHeartbeatKey)).thenReturn(batchJobHeartbeatValue);

    verifyCheckBatchJobHasHeartbeat(
        Duration.ofMinutes(10),
        Duration.ofMinutes(0),
        mockAdmin,
        true
    );
  }

  @Test
  public void testGetHeartbeatValueToProveTimeout() {
    BatchJobHeartbeatKey batchJobHeartbeatKey = new BatchJobHeartbeatKey();
    batchJobHeartbeatKey.storeVersion = VERSION_NUMBER;
    batchJobHeartbeatKey.storeName = STORE_NAME;
    BatchJobHeartbeatValue batchJobHeartbeatValue = new BatchJobHeartbeatValue();
    batchJobHeartbeatValue.timestamp = NOW.toEpochMilli() - TimeUnit.MINUTES.toMillis(11); // 11 minutes ago
    Admin mockAdmin = createCheckBatchJobHasHeartbeatAdmin();
    when(mockAdmin.getBatchJobHeartbeatValue(batchJobHeartbeatKey)).thenReturn(batchJobHeartbeatValue);

    verifyCheckBatchJobHasHeartbeat(
        Duration.ofMinutes(10),
        Duration.ofMinutes(0),
        mockAdmin,
        false
    );
  }

  private Admin createCheckBatchJobHasHeartbeatAdmin() {
    Admin admin = mock(Admin.class);
    when(admin.hasWritePermissionToBatchJobHeartbeatStore(DEFAULT_PRINCIPAL_NAME)).thenReturn(true);
    PushJobDetails pushJobDetails = new PushJobDetails();
    pushJobDetails.pushJobConfigs = Collections.singletonMap(HEARTBEAT_ENABLED_CONFIG.getConfigName(), "true");
    when(admin.getPushJobDetails(any())).thenReturn(pushJobDetails);
    return admin;
  }

  private void verifyCheckBatchJobHasHeartbeat(
      Duration heartbeatTimeout,
      Duration initialHeartbeatBufferTime,
      Admin mockControllerAdmin,
      boolean expectedCheckResult
  ) {
    DefaultLingeringStoreVersionChecker defaultLingeringStoreVersionChecker = mock(DefaultLingeringStoreVersionChecker.class);
    when(defaultLingeringStoreVersionChecker.isStoreVersionLingering(any(), any(), any(), any(), any())). thenReturn(false);

    HeartbeatBasedLingeringStoreVersionChecker checker = new HeartbeatBasedLingeringStoreVersionChecker(
        heartbeatTimeout,
        initialHeartbeatBufferTime,
        defaultLingeringStoreVersionChecker,
        mock(HeartbeatBasedCheckerStats.class)
    );
    Assert.assertEquals(
        checker.isStoreVersionLingering(store, version, time, mockControllerAdmin, Optional.of(DEFAULT_PRINCIPAL_NAME)),
        expectedCheckResult
    );
    // Expect no fallback to use the default checker
    verify(defaultLingeringStoreVersionChecker, times(0)).isStoreVersionLingering(any(), any(), any(), any(), any());
  }
}
