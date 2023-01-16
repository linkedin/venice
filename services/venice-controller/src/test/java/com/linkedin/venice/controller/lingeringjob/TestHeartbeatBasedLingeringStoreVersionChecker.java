package com.linkedin.venice.controller.lingeringjob;

import static com.linkedin.venice.status.BatchJobHeartbeatConfigs.HEARTBEAT_ENABLED_CONFIG;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.acl.AclException;
import com.linkedin.venice.authorization.IdentityParser;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.status.protocol.BatchJobHeartbeatKey;
import com.linkedin.venice.status.protocol.BatchJobHeartbeatValue;
import com.linkedin.venice.status.protocol.PushJobDetails;
import com.linkedin.venice.utils.Time;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class TestHeartbeatBasedLingeringStoreVersionChecker {
  private static final String STORE_NAME = "store_name";
  private static final int VERSION_NUMBER = 1;
  private static final Instant NOW = Instant.now();
  private static final X509Certificate DEFAULT_REQUEST_CERT = mock(X509Certificate.class);
  private static final IdentityParser DEFAULT_IDENTITY_PARSER = (certificate) -> "Whatever identity";

  private Store store;
  private Version version;
  private Time time;

  @BeforeTest
  public void setUp() {
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
    DefaultLingeringStoreVersionChecker defaultLingeringStoreVersionChecker =
        mock(DefaultLingeringStoreVersionChecker.class);

    Admin mockAdmin = mock(Admin.class);
    Optional<X509Certificate> emptyRequesterCert = Optional.empty();
    IdentityParser mockIdentityParser = mock(IdentityParser.class);

    when(
        defaultLingeringStoreVersionChecker
            .isStoreVersionLingering(store, version, time, mockAdmin, emptyRequesterCert, mockIdentityParser))
                .thenReturn(false);

    HeartbeatBasedLingeringStoreVersionChecker checker = new HeartbeatBasedLingeringStoreVersionChecker(
        heartbeatTimeout,
        initialHeartbeatBufferTime,
        defaultLingeringStoreVersionChecker,
        mock(HeartbeatBasedCheckerStats.class));
    Assert.assertFalse(
        checker.isStoreVersionLingering(store, version, time, mockAdmin, emptyRequesterCert, mockIdentityParser));
    // Expect fallback to use the default checker
    verify(defaultLingeringStoreVersionChecker, times(1))
        .isStoreVersionLingering(store, version, time, mockAdmin, emptyRequesterCert, mockIdentityParser);
  }

  @Test
  public void testNoWritePermissionToHeartbeatStore() throws Exception {
    Duration heartbeatTimeout = Duration.ofMinutes(10);
    Duration initialHeartbeatBufferTime = Duration.ofMinutes(3);
    DefaultLingeringStoreVersionChecker defaultLingeringStoreVersionChecker =
        mock(DefaultLingeringStoreVersionChecker.class);
    when(defaultLingeringStoreVersionChecker.isStoreVersionLingering(any(), any(), any(), any(), any(), any()))
        .thenReturn(false);

    HeartbeatBasedLingeringStoreVersionChecker checker = new HeartbeatBasedLingeringStoreVersionChecker(
        heartbeatTimeout,
        initialHeartbeatBufferTime,
        defaultLingeringStoreVersionChecker,
        mock(HeartbeatBasedCheckerStats.class));
    Admin admin = mock(Admin.class);
    X509Certificate cert = mock(X509Certificate.class);
    when(
        admin.hasWritePermissionToBatchJobHeartbeatStore(
            cert,
            VeniceSystemStoreType.BATCH_JOB_HEARTBEAT_STORE.getPrefix())).thenReturn(false);
    Assert.assertFalse(
        checker.isStoreVersionLingering(
            store,
            version,
            time,
            admin,
            Optional.of(cert),
            (certificate) -> "whatever identity"));
    // Expect fallback to use the default checker
    verify(defaultLingeringStoreVersionChecker, times(1))
        .isStoreVersionLingering(any(), any(), any(), any(), any(), any());
  }

  @Test
  public void testGetPushJobDetailsReturnsNull() throws AclException {
    verifyGetPushJobDetailsFailedCase(null);
  }

  @Test
  public void testGetPushJobDetailsReturnsNullConfig() throws AclException {
    verifyGetPushJobDetailsFailedCase(new PushJobDetails());
  }

  @Test
  public void testGetPushJobDetailsReturnMissingConfig() throws AclException {
    PushJobDetails pushJobDetails = new PushJobDetails();
    pushJobDetails.pushJobConfigs = Collections.emptyMap();
    verifyGetPushJobDetailsFailedCase(pushJobDetails);
  }

  private void verifyGetPushJobDetailsFailedCase(PushJobDetails expectedReturnValue) throws AclException {
    Duration heartbeatTimeout = Duration.ofMinutes(10);
    Duration initialHeartbeatBufferTime = Duration.ofMinutes(3);
    DefaultLingeringStoreVersionChecker defaultLingeringStoreVersionChecker =
        mock(DefaultLingeringStoreVersionChecker.class);
    when(defaultLingeringStoreVersionChecker.isStoreVersionLingering(any(), any(), any(), any(), any(), any()))
        .thenReturn(false);

    HeartbeatBasedLingeringStoreVersionChecker checker = new HeartbeatBasedLingeringStoreVersionChecker(
        heartbeatTimeout,
        initialHeartbeatBufferTime,
        defaultLingeringStoreVersionChecker,
        mock(HeartbeatBasedCheckerStats.class));
    Admin admin = mock(Admin.class);
    X509Certificate cert = mock(X509Certificate.class);
    when(
        admin.hasWritePermissionToBatchJobHeartbeatStore(
            cert,
            VeniceSystemStoreType.BATCH_JOB_HEARTBEAT_STORE.getPrefix())).thenReturn(true);
    when(admin.getPushJobDetails(any())).thenReturn(expectedReturnValue);
    Assert.assertFalse(
        checker.isStoreVersionLingering(
            store,
            version,
            time,
            admin,
            Optional.of(cert),
            (certificate) -> "whatever identity"));
    // Expect fallback to use the default checker
    verify(defaultLingeringStoreVersionChecker, times(1))
        .isStoreVersionLingering(any(), any(), any(), any(), any(), any());
  }

  @Test
  public void testGetHeartbeatValueFailed() throws AclException {
    BatchJobHeartbeatKey batchJobHeartbeatKey = new BatchJobHeartbeatKey();
    batchJobHeartbeatKey.storeVersion = VERSION_NUMBER;
    batchJobHeartbeatKey.storeName = STORE_NAME;
    Admin mockAdmin = createCheckBatchJobHasHeartbeatAdmin();
    when(mockAdmin.getBatchJobHeartbeatValue(batchJobHeartbeatKey))
        .thenThrow(new VeniceException("Simulated exception"));

    verifyCheckBatchJobHasHeartbeat(Duration.ofMinutes(10), Duration.ofMinutes(0), mockAdmin, false, 0);
  }

  @Test
  public void testGetNullHeartbeatValue() throws AclException {
    BatchJobHeartbeatKey batchJobHeartbeatKey = new BatchJobHeartbeatKey();
    batchJobHeartbeatKey.storeVersion = VERSION_NUMBER;
    batchJobHeartbeatKey.storeName = STORE_NAME;
    Admin mockAdmin = createCheckBatchJobHasHeartbeatAdmin();
    when(mockAdmin.getBatchJobHeartbeatValue(batchJobHeartbeatKey)).thenReturn(null);

    verifyCheckBatchJobHasHeartbeat(
        Duration.ofMinutes(10),
        Duration.ofMinutes(0),
        mockAdmin,
        true, // No/null heartbeat means the job is not alive. Hence the job is lingering
        0);
  }

  @Test
  public void testGetHeartbeatValueToProveAliveness() throws AclException {
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
        false, // Not lingering
        0);
  }

  @Test
  public void testGetHeartbeatValueToProveTimeout() throws AclException {
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
        true, // Lingering
        0);
  }

  private Admin createCheckBatchJobHasHeartbeatAdmin() throws AclException {
    Admin admin = mock(Admin.class);
    when(
        admin.hasWritePermissionToBatchJobHeartbeatStore(
            DEFAULT_REQUEST_CERT,
            VeniceSystemStoreType.BATCH_JOB_HEARTBEAT_STORE.getPrefix())).thenReturn(true);
    PushJobDetails pushJobDetails = new PushJobDetails();
    pushJobDetails.pushJobConfigs = Collections.singletonMap(HEARTBEAT_ENABLED_CONFIG.getConfigName(), "true");
    when(admin.getPushJobDetails(any())).thenReturn(pushJobDetails);
    return admin;
  }

  private void verifyCheckBatchJobHasHeartbeat(
      Duration heartbeatTimeout,
      Duration initialHeartbeatBufferTime,
      Admin mockControllerAdmin,
      boolean expectedCheckResult,
      int expectedInteractionCountWithDefaultChecker) {
    DefaultLingeringStoreVersionChecker defaultLingeringStoreVersionChecker =
        mock(DefaultLingeringStoreVersionChecker.class);
    when(defaultLingeringStoreVersionChecker.isStoreVersionLingering(any(), any(), any(), any(), any(), any()))
        .thenReturn(false);

    HeartbeatBasedLingeringStoreVersionChecker checker = new HeartbeatBasedLingeringStoreVersionChecker(
        heartbeatTimeout,
        initialHeartbeatBufferTime,
        defaultLingeringStoreVersionChecker,
        mock(HeartbeatBasedCheckerStats.class));
    Assert.assertEquals(
        checker.isStoreVersionLingering(
            store,
            version,
            time,
            mockControllerAdmin,
            Optional.of(DEFAULT_REQUEST_CERT),
            DEFAULT_IDENTITY_PARSER),
        expectedCheckResult);
    // Expect no fallback to use the default checker
    verify(defaultLingeringStoreVersionChecker, times(expectedInteractionCountWithDefaultChecker))
        .isStoreVersionLingering(any(), any(), any(), any(), any(), any());
  }
}
