package com.linkedin.venice.controller;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.ACCESS_CONTROLLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.AUTO_SCHEMA_REGISTER_FOR_PUSHJOB_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.BACKUP_STRATEGY;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.BACKUP_VERSION_RETENTION_MS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.BATCH_GET_LIMIT;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLIENT_DECOMPRESSION_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.COMPRESSION_STRATEGY;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.ENABLE_READS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.ENABLE_WRITES;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.HYBRID_STORE_DISK_QUOTA_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.LARGEST_USED_VERSION_NUMBER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.MIGRATION_DUPLICATE_STORE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NATIVE_REPLICATION_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NATIVE_REPLICATION_SOURCE_FABRIC;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NUM_VERSIONS_TO_PRESERVE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.OWNER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PUSH_STREAM_SOURCE_ADDRESS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.READ_COMPUTATION_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.REPLICATION_FACTOR;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.REPLICATION_METADATA_PROTOCOL_VERSION_ID;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.SEPARATE_REAL_TIME_TOPIC_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORAGE_NODE_READ_QUOTA_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORAGE_QUOTA_IN_BYTE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.TARGET_REGION_PROMOTED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.controller.kafka.protocol.admin.UpdateStore;
import com.linkedin.venice.controller.storeconfig.StoreConfigUpdater;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.helix.StoragePersonaRepository;
import com.linkedin.venice.helix.ZkRoutersClusterManager;
import com.linkedin.venice.hooks.StoreLifecycleHooks;
import com.linkedin.venice.meta.BackupStrategy;
import com.linkedin.venice.meta.ExternalStorageReadMode;
import com.linkedin.venice.meta.IngestionPauseMode;
import com.linkedin.venice.meta.LifecycleHooksRecord;
import com.linkedin.venice.meta.LifecycleHooksRecordImpl;
import com.linkedin.venice.meta.StorageMode;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.utils.ConfigCommonUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Safety-net coverage for the upcoming dedup of {@link StoreConfigUpdater#applyOnChild} and
 * {@link StoreConfigUpdater#applyOnParent}. The future refactor will collapse the ~24 trivial
 * scalar fields below into a single descriptor table; these tests lock in the parent admin-message
 * round-trip, the child admin-setter call, and the parent/child symmetry for that exact field set
 * so any wiring drift introduced by the dedup fails loudly.
 *
 * <p>Carve-outs (intentionally NOT covered here, because they are asymmetric or guarded by extra
 * logic — kept on existing dedicated tests): hybrid-config merge, partitioner merge + parent
 * class-load probe, read-quota cluster check, partitionCount preCheck, currentVersion (asymmetric
 * onlyParentRegionFilter arg), storeMigration legacy alias, writeComputationEnabled /
 * chunkingEnabled / rmdChunkingEnabled (parent routes through ParentControllerConfigUpdateUtils),
 * activeActiveReplicationEnabled (amp-factor cross-check), latestSupersetSchemaId (parent
 * value-schema validation), min/maxCompactionLagSeconds (parent throws on max<min),
 * ingestionPauseMode/regions, storeViews / viewName family, storagePersona, the ETL block, and
 * every child-only field that routes through {@code storeMetadataUpdate} (compaction*,
 * max*RecordSizeBytes, unusedSchemaDeletionEnabled, blob*, nearlineProducer*, targetSwap*,
 * isDavinciHeartbeatReported, globalRtDivEnabled, ttlRepushEnabled, enumSchemaEvolutionAllowed,
 * flinkVeniceViewsEnabled, previousCurrentVersion, storageMode, externalStorageReadMode,
 * largestUsedRTVersionNumber).
 *
 * <p>Quirks the descriptor table locks in — naming mismatches a naive dedup would silently get
 * wrong, so they live as explicit columns rather than as derived defaults:
 * <ul>
 *   <li><b>{@code autoSchemaRegisterPushJobEnabled} has three different spellings.</b> The
 *       params setter is {@code UpdateStoreQueryParams.setAutoSchemaPushJobEnabled} (no
 *       "Register"), the admin setter is {@code VeniceHelixAdmin.setAutoSchemaRegisterPushJobEnabled},
 *       and the Avro field is {@code UpdateStore.schemaAutoRegisterFromPushJobEnabled}. A
 *       generated descriptor that assumed name parity would fail to resolve any of these.</li>
 *   <li><b>{@code AUTO_SCHEMA_REGISTER_FOR_PUSHJOB_ENABLED} has a typo'd value.</b> The constant
 *       name reads "register_for_pushjob", but the string value is
 *       {@code "auto_auto_register_for_pushjob_enabled"} — the user-facing key has a doubled
 *       "auto_". The constant name doesn't hint at it; only the literal value does. Renaming the
 *       value would be a breaking API change, so the descriptor pins it as-is.</li>
 *   <li><b>{@code compressionStrategy} and {@code backupStrategy} cross an enum→int boundary at
 *       the parent.</b> The params and child-side admin setters both take the enum, but the
 *       parent stores {@code compressionStrategy.getValue()} (a custom int code) and
 *       {@code backupStrategy.ordinal()} (the JVM-assigned int) into the Avro message. So the
 *       descriptor carries different {@code parentExpected} (the int) and {@code childExpectedArg}
 *       (the enum) values for the same logical field.</li>
 *   <li><b>{@code setLargestUsedVersionNumber} (params) ↔ {@code setStoreLargestUsedVersion}
 *       (admin).</b> The params setter has the "Number" suffix; the admin setter drops it and
 *       gains a "Store" prefix. A descriptor that derived one name from the other by stripping a
 *       common prefix/suffix would miss this.</li>
 * </ul>
 */
public class StoreConfigUpdaterTest extends AbstractTestVeniceParentHelixAdmin {
  private static final String NEW_OWNER = "new-owner-for-trivial-test";
  private static final String NEW_PUSH_SRC = "kafka://broker:9092";
  private static final String NEW_NR_FABRIC = "dc-trivial-test";

  /**
   * Descriptor for one trivial scalar field. All four assertions for that field (parent Avro
   * round-trip, parent updatedConfigsList entry, child admin-setter call, reflective symmetry)
   * are driven from this single record so the future dedup has exactly one source of truth to
   * stay in sync with.
   */
  private static final class TrivialField {
    final String name;
    final String configKey;
    final String avroFieldName;
    final String childSetterName;
    final Class<?> childSetterValueType;
    final Consumer<UpdateStoreQueryParams> setOnParams;
    final Object parentExpected;
    final Object childExpectedArg;

    TrivialField(
        String name,
        String configKey,
        String avroFieldName,
        String childSetterName,
        Class<?> childSetterValueType,
        Consumer<UpdateStoreQueryParams> setOnParams,
        Object parentExpected,
        Object childExpectedArg) {
      this.name = name;
      this.configKey = configKey;
      this.avroFieldName = avroFieldName;
      this.childSetterName = childSetterName;
      this.childSetterValueType = childSetterValueType;
      this.setOnParams = setOnParams;
      this.parentExpected = parentExpected;
      this.childExpectedArg = childExpectedArg;
    }
  }

  private static final List<TrivialField> TRIVIAL_FIELDS = Arrays.asList(
      new TrivialField(
          "owner",
          OWNER,
          "owner",
          "setStoreOwner",
          String.class,
          p -> p.setOwner(NEW_OWNER),
          NEW_OWNER,
          NEW_OWNER),
      new TrivialField(
          "enableReads",
          ENABLE_READS,
          "enableReads",
          "setStoreReadability",
          boolean.class,
          p -> p.setEnableReads(true),
          true,
          true),
      new TrivialField(
          "enableWrites",
          ENABLE_WRITES,
          "enableWrites",
          "setStoreWriteability",
          boolean.class,
          p -> p.setEnableWrites(false),
          false,
          false),
      new TrivialField(
          "storageQuotaInByte",
          STORAGE_QUOTA_IN_BYTE,
          "storageQuotaInByte",
          "setStoreStorageQuota",
          long.class,
          p -> p.setStorageQuotaInByte(12345L),
          12345L,
          12345L),
      new TrivialField(
          // Quirk: params setter is setLargestUsedVersionNumber (with "Number"); admin setter
          // drops the suffix AND gains a "Store" prefix as setStoreLargestUsedVersion. The Avro
          // field also drops "Number" — and is a nullable Integer, not a primitive int, so the
          // parent-expected comparison relies on Number-to-long unboxing in eqUnboxed.
          "largestUsedVersionNumber",
          LARGEST_USED_VERSION_NUMBER,
          "largestUsedVersionNumber",
          "setStoreLargestUsedVersion",
          int.class,
          p -> p.setLargestUsedVersionNumber(7),
          7,
          7),
      new TrivialField(
          "bootstrapToOnlineTimeoutInHours",
          BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS,
          "bootstrapToOnlineTimeoutInHours",
          "setBootstrapToOnlineTimeoutInHours",
          int.class,
          p -> p.setBootstrapToOnlineTimeoutInHours(48),
          48,
          48),
      new TrivialField(
          "accessControlled",
          ACCESS_CONTROLLED,
          "accessControlled",
          "setAccessControl",
          boolean.class,
          p -> p.setAccessControlled(true),
          true,
          true),
      new TrivialField(
          // Quirk: enum→int boundary at the parent. The child admin setter takes the enum
          // directly, but the parent persists CompressionStrategy.getValue() (a custom int code,
          // NOT the JVM ordinal) into the Avro message. Hence parentExpected is the int and
          // childExpectedArg is the enum.
          "compressionStrategy",
          COMPRESSION_STRATEGY,
          "compressionStrategy",
          "setStoreCompressionStrategy",
          CompressionStrategy.class,
          p -> p.setCompressionStrategy(CompressionStrategy.GZIP),
          CompressionStrategy.GZIP.getValue(),
          CompressionStrategy.GZIP),
      new TrivialField(
          "clientDecompressionEnabled",
          CLIENT_DECOMPRESSION_ENABLED,
          "clientDecompressionEnabled",
          "setClientDecompressionEnabled",
          boolean.class,
          p -> p.setClientDecompressionEnabled(false),
          false,
          false),
      new TrivialField(
          "batchGetLimit",
          BATCH_GET_LIMIT,
          "batchGetLimit",
          "setBatchGetLimit",
          int.class,
          p -> p.setBatchGetLimit(150),
          150,
          150),
      new TrivialField(
          "numVersionsToPreserve",
          NUM_VERSIONS_TO_PRESERVE,
          "numVersionsToPreserve",
          "setNumVersionsToPreserve",
          int.class,
          p -> p.setNumVersionsToPreserve(5),
          5,
          5),
      new TrivialField(
          "separateRealTimeTopicEnabled",
          SEPARATE_REAL_TIME_TOPIC_ENABLED,
          "separateRealTimeTopicEnabled",
          "setSeparateRealTimeTopicEnabled",
          boolean.class,
          p -> p.setSeparateRealTimeTopicEnabled(true),
          true,
          true),
      new TrivialField(
          "replicationFactor",
          REPLICATION_FACTOR,
          "replicationFactor",
          "setReplicationFactor",
          int.class,
          p -> p.setReplicationFactor(3),
          3,
          3),
      new TrivialField(
          "migrationDuplicateStore",
          MIGRATION_DUPLICATE_STORE,
          "migrationDuplicateStore",
          "setMigrationDuplicateStore",
          boolean.class,
          p -> p.setMigrationDuplicateStore(true),
          true,
          true),
      new TrivialField(
          "replicationMetadataVersionID",
          REPLICATION_METADATA_PROTOCOL_VERSION_ID,
          "replicationMetadataVersionID",
          "setReplicationMetadataVersionID",
          int.class,
          p -> p.setReplicationMetadataVersionID(2),
          2,
          2),
      new TrivialField(
          // Set false on a default-batch store: never crosses the false→true edge, so the parent
          // superset-schema dry-run is NOT triggered.
          "readComputationEnabled",
          READ_COMPUTATION_ENABLED,
          "readComputationEnabled",
          "setReadComputationEnabled",
          boolean.class,
          p -> p.setReadComputationEnabled(false),
          false,
          false),
      new TrivialField(
          "nativeReplicationEnabled",
          NATIVE_REPLICATION_ENABLED,
          "nativeReplicationEnabled",
          "setNativeReplicationEnabled",
          boolean.class,
          p -> p.setNativeReplicationEnabled(true),
          true,
          true),
      new TrivialField(
          "pushStreamSourceAddress",
          PUSH_STREAM_SOURCE_ADDRESS,
          "pushStreamSourceAddress",
          "setPushStreamSourceAddress",
          String.class,
          p -> p.setPushStreamSourceAddress(NEW_PUSH_SRC),
          NEW_PUSH_SRC,
          NEW_PUSH_SRC),
      new TrivialField(
          // Quirk: same enum→int boundary as compressionStrategy, but here the parent uses
          // BackupStrategy.ordinal() (JVM-assigned, position-sensitive) rather than a getValue()
          // custom code. Reordering the BackupStrategy enum would silently change every existing
          // store's persisted backupStrategy; this row pins that contract.
          "backupStrategy",
          BACKUP_STRATEGY,
          "backupStrategy",
          "setBackupStrategy",
          BackupStrategy.class,
          p -> p.setBackupStrategy(BackupStrategy.KEEP_MIN_VERSIONS),
          BackupStrategy.KEEP_MIN_VERSIONS.ordinal(),
          BackupStrategy.KEEP_MIN_VERSIONS),
      new TrivialField(
          // Quirk: three different spellings for the same logical field — params setter
          // setAutoSchemaPushJobEnabled (no "Register"), admin setter
          // setAutoSchemaRegisterPushJobEnabled (with "Register"), and Avro field
          // schemaAutoRegisterFromPushJobEnabled (reordered, with "From"). Plus the config-key
          // value AUTO_SCHEMA_REGISTER_FOR_PUSHJOB_ENABLED = "auto_auto_register_for_pushjob_enabled"
          // has a doubled "auto_" the constant name doesn't show — renaming the value would be a
          // breaking API change, so the descriptor pins it as-is.
          "autoSchemaRegisterPushJobEnabled",
          AUTO_SCHEMA_REGISTER_FOR_PUSHJOB_ENABLED,
          "schemaAutoRegisterFromPushJobEnabled",
          "setAutoSchemaRegisterPushJobEnabled",
          boolean.class,
          p -> p.setAutoSchemaPushJobEnabled(true),
          true,
          true),
      new TrivialField(
          "hybridStoreDiskQuotaEnabled",
          HYBRID_STORE_DISK_QUOTA_ENABLED,
          "hybridStoreDiskQuotaEnabled",
          "setHybridStoreDiskQuotaEnabled",
          boolean.class,
          p -> p.setHybridStoreDiskQuotaEnabled(true),
          true,
          true),
      new TrivialField(
          "backupVersionRetentionMs",
          BACKUP_VERSION_RETENTION_MS,
          "backupVersionRetentionMs",
          "setBackupVersionRetentionMs",
          long.class,
          p -> p.setBackupVersionRetentionMs(86400000L),
          86400000L,
          86400000L),
      new TrivialField(
          "nativeReplicationSourceFabric",
          NATIVE_REPLICATION_SOURCE_FABRIC,
          "nativeReplicationSourceFabric",
          "setNativeReplicationSourceFabric",
          String.class,
          p -> p.setNativeReplicationSourceFabric(NEW_NR_FABRIC),
          NEW_NR_FABRIC,
          NEW_NR_FABRIC),
      new TrivialField(
          "storageNodeReadQuotaEnabled",
          STORAGE_NODE_READ_QUOTA_ENABLED,
          "storageNodeReadQuotaEnabled",
          "setStorageNodeReadQuotaEnabled",
          boolean.class,
          p -> p.setStorageNodeReadQuotaEnabled(true),
          true,
          true));

  @BeforeMethod
  public void setupTestCase() {
    setupInternalMocks();
    initializeParentAdmin(Optional.empty(), Optional.empty());
  }

  @AfterMethod
  public void cleanupTestCase() {
    super.cleanupTestCase();
  }

  /**
   * Drives every trivial field through {@code parentAdmin.updateStore} in a single call, captures
   * the UPDATE_STORE admin message, and asserts that each field's Avro slot has the supplied value
   * AND that the matching ControllerApiConstants key was added to {@code updatedConfigsList}.
   */
  @Test
  public void testApplyOnParent_TrivialFieldsRoundTrip() {
    String storeName = Utils.getUniqueString("trivial-parent");
    Store store = TestUtils.createTestStore(storeName, "test-owner", System.currentTimeMillis());
    doReturn(store).when(internalAdmin).getStore(clusterName, storeName);
    parentAdmin.initStorageCluster(clusterName);

    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    for (TrivialField f: TRIVIAL_FIELDS) {
      f.setOnParams.accept(params);
    }

    parentAdmin.updateStore(clusterName, storeName, params);

    UpdateStore msg = captureLastUpdateStore();
    Set<String> updatedKeys = msg.updatedConfigsList.stream().map(CharSequence::toString).collect(Collectors.toSet());

    List<String> failures = new ArrayList<>();
    for (TrivialField f: TRIVIAL_FIELDS) {
      Object actual;
      try {
        actual = readAvroField(msg, f.avroFieldName);
      } catch (Exception e) {
        failures.add(f.name + ": cannot read Avro field '" + f.avroFieldName + "': " + e);
        continue;
      }
      if (actual instanceof CharSequence) {
        actual = actual.toString();
      }
      if (!eqUnboxed(actual, f.parentExpected)) {
        failures.add(
            f.name + ": Avro field '" + f.avroFieldName + "' expected <" + f.parentExpected + "> but was <" + actual
                + ">");
      }
      if (!updatedKeys.contains(f.configKey)) {
        failures.add(f.name + ": updatedConfigsList missing config key '" + f.configKey + "'");
      }
    }
    if (!failures.isEmpty()) {
      fail("Parent round-trip failed for " + failures.size() + " field(s):\n  - " + String.join("\n  - ", failures));
    }
  }

  /**
   * Drives every trivial field through {@code StoreConfigUpdater.applyOnChild} and uses Mockito
   * verify to confirm the corresponding {@code VeniceHelixAdmin.setX(...)} method was called with
   * the exact value supplied. Bypasses the public {@code updateStore} wrapper (which acquires a
   * cluster lock that's awkward to mock); the only behavior under test is the dispatcher.
   */
  @Test
  public void testApplyOnChild_TrivialFieldsCallsAdminSetters() {
    String storeName = Utils.getUniqueString("trivial-child");
    VeniceHelixAdmin admin = newChildAdminMock(storeName);

    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    for (TrivialField f: TRIVIAL_FIELDS) {
      f.setOnParams.accept(params);
    }

    StoreConfigUpdater.applyOnChild(admin, clusterName, storeName, params);

    List<String> failures = new ArrayList<>();
    for (TrivialField f: TRIVIAL_FIELDS) {
      try {
        invokeVerify(admin, f, storeName);
      } catch (AssertionError e) {
        failures.add(f.name + " (" + f.childSetterName + "): " + e.getMessage());
      } catch (Exception e) {
        failures.add(f.name + " (" + f.childSetterName + "): " + e);
      }
    }
    if (!failures.isEmpty()) {
      fail(
          "Child setter verification failed for " + failures.size() + " field(s):\n  - "
              + String.join("\n  - ", failures));
    }
  }

  /**
   * Two-part symmetry check on the descriptor table itself:
   * <ol>
   *   <li>For each row, the named Avro field, child admin setter, and {@code ControllerApiConstants}
   *       key all actually exist with the declared shape.</li>
   *   <li>Each row, when set in isolation on a fresh {@code UpdateStoreQueryParams}, causes the
   *       parent admin message's {@code updatedConfigsList} to contain exactly {@code [configKey]}
   *       AND triggers exactly one matching child admin setter call (no spillage onto siblings).</li>
   * </ol>
   * Locks in the parent/child correspondence the future descriptor-table dedup depends on.
   */
  @Test
  public void testSymmetry_DescriptorsResolveAndDriveBothSidesIdentically() {
    // ----- Part 1: reflective shape checks -----
    List<String> shapeFailures = new ArrayList<>();
    for (TrivialField f: TRIVIAL_FIELDS) {
      Field avro;
      try {
        avro = UpdateStore.class.getField(f.avroFieldName);
      } catch (NoSuchFieldException e) {
        shapeFailures.add(f.name + ": UpdateStore has no field '" + f.avroFieldName + "'");
        continue;
      }
      assertNotNull(avro, "UpdateStore." + f.avroFieldName);

      Method setter;
      try {
        setter =
            VeniceHelixAdmin.class.getMethod(f.childSetterName, String.class, String.class, f.childSetterValueType);
      } catch (NoSuchMethodException e) {
        shapeFailures.add(
            f.name + ": VeniceHelixAdmin." + f.childSetterName + "(String, String, "
                + f.childSetterValueType.getSimpleName() + ") not found");
        continue;
      }
      assertNotNull(setter);

      if (f.configKey == null || f.configKey.isEmpty()) {
        shapeFailures.add(f.name + ": configKey is null/empty");
      }
    }
    if (!shapeFailures.isEmpty()) {
      fail("Descriptor shape failures (" + shapeFailures.size() + "):\n  - " + String.join("\n  - ", shapeFailures));
    }

    // ----- Part 2: per-field parent/child isolation drive -----
    List<String> driveFailures = new ArrayList<>();
    for (TrivialField f: TRIVIAL_FIELDS) {
      // Fresh fixture per field; the BeforeMethod fixture is already up but we re-seed the store
      // each time and rely on the captor seeing the latest put.
      String storeName = Utils.getUniqueString("symm-" + f.name);
      Store store = TestUtils.createTestStore(storeName, "test-owner", System.currentTimeMillis());
      doReturn(store).when(internalAdmin).getStore(clusterName, storeName);

      // ---- parent leg ----
      UpdateStoreQueryParams parentParams = new UpdateStoreQueryParams();
      f.setOnParams.accept(parentParams);
      try {
        parentAdmin.initStorageCluster(clusterName);
        parentAdmin.updateStore(clusterName, storeName, parentParams);
      } catch (Throwable t) {
        driveFailures.add(f.name + ": parent updateStore threw: " + t);
        continue;
      }
      UpdateStore msg;
      try {
        msg = captureLastUpdateStore();
      } catch (Throwable t) {
        driveFailures.add(f.name + ": capturing parent admin message failed: " + t);
        continue;
      }
      Set<String> keys = msg.updatedConfigsList.stream().map(CharSequence::toString).collect(Collectors.toSet());
      if (!keys.contains(f.configKey)) {
        driveFailures.add(f.name + ": parent updatedConfigsList missing '" + f.configKey + "' (got " + keys + ")");
      }
      // Sanity: list contains only this key (no spillage onto sibling fields).
      Set<String> extras = new HashSet<>(keys);
      extras.remove(f.configKey);
      if (!extras.isEmpty()) {
        driveFailures.add(f.name + ": parent updatedConfigsList has unexpected extras " + extras);
      }

      // ---- child leg ----
      VeniceHelixAdmin childAdmin = newChildAdminMock(storeName);
      UpdateStoreQueryParams childParams = new UpdateStoreQueryParams();
      f.setOnParams.accept(childParams);
      try {
        StoreConfigUpdater.applyOnChild(childAdmin, clusterName, storeName, childParams);
      } catch (Throwable t) {
        driveFailures.add(f.name + ": child applyOnChild threw: " + t);
        continue;
      }
      try {
        invokeVerify(childAdmin, f, storeName);
      } catch (Throwable t) {
        driveFailures.add(f.name + ": child setter not invoked correctly: " + t.getMessage());
      }
    }
    if (!driveFailures.isEmpty()) {
      fail("Per-field drive failures (" + driveFailures.size() + "):\n  - " + String.join("\n  - ", driveFailures));
    }
  }

  /**
   * Covers the child-only branches that route through {@code admin.storeMetadataUpdate(...)} —
   * the long tail of {@code Optional.ifPresent} calls in {@code applyOnChild} that the
   * trivial-field descriptor table intentionally carves out (compaction*, max*RecordSizeBytes,
   * unusedSchemaDeletion, blob*, nearlineProducer*, targetSwap*, isDavinciHeartbeatReported,
   * globalRtDivEnabled, ttlRepushEnabled, enumSchemaEvolutionAllowed, flinkVeniceViewsEnabled,
   * previousCurrentVersion, storageMode, externalStorageReadMode). Each field is set, then we
   * verify {@code storeMetadataUpdate} was invoked at least as many times as the number of
   * distinct fields we set, which flips every corresponding {@code .ifPresent} branch from
   * absent to present in one pass.
   */
  @Test
  public void testApplyOnChild_ChildOnlyMetadataFields_TriggerStoreMetadataUpdates() {
    String storeName = Utils.getUniqueString("child-meta-sweep");
    VeniceHelixAdmin admin = newChildAdminMock(storeName);

    UpdateStoreQueryParams params = new UpdateStoreQueryParams().setStorageMode(StorageMode.INTERNAL)
        .setExternalStorageReadMode(ExternalStorageReadMode.VENICE_ONLY)
        .setCompactionEnabled(true)
        .setCompactionThresholdMilliseconds(3600_000L)
        .setMinCompactionLagSeconds(1L)
        .setMaxCompactionLagSeconds(2L)
        .setMaxRecordSizeBytes(1024)
        .setMaxNearlineRecordSizeBytes(2048)
        .setUnusedSchemaDeletionEnabled(true)
        .setBlobTransferEnabled(true)
        .setBlobTransferInServerEnabled(ConfigCommonUtils.ActivationState.ENABLED)
        .setBlobDbEnabled(ConfigCommonUtils.ActivationState.ENABLED)
        .setNearlineProducerCompressionEnabled(true)
        .setNearlineProducerCountPerWriter(2)
        .setTargetRegionSwap("dc-1")
        .setTargetRegionSwapWaitTime(120)
        .setIsDavinciHeartbeatReported(true)
        .setGlobalRtDivEnabled(true)
        .setTTLRepushEnabled(true)
        .setEnumSchemaEvolutionAllowed(true)
        .setFlinkVeniceViewsEnabled(true)
        .setPreviousCurrentVersion(1)
        .setTargetRegionPromoted(true);

    StoreConfigUpdater.applyOnChild(admin, clusterName, storeName, params);

    // 23 distinct ifPresent/conditional branches above (added targetRegionPromoted). Allow some
    // headroom because a few of those (e.g., the compaction lag pair) read through the same generic ifPresent.
    verify(admin, atLeast(20)).storeMetadataUpdate(eq(clusterName), eq(storeName), any());
  }

  /**
   * Covers the child-side persona branch: when {@code personaName} is supplied, the resolved
   * {@link StoragePersonaRepository} from the cluster resources must receive an
   * {@code addStoresToPersona(name, [storeName])} call.
   */
  @Test
  public void testApplyOnChild_PersonaName_AddsStoreToPersona() {
    String storeName = Utils.getUniqueString("child-persona");
    VeniceHelixAdmin admin = newChildAdminMock(storeName);
    HelixVeniceClusterResources resources = admin.getHelixVeniceClusterResources(clusterName);
    StoragePersonaRepository personaRepo = mock(StoragePersonaRepository.class);
    doReturn(personaRepo).when(resources).getStoragePersonaRepository();

    UpdateStoreQueryParams params = new UpdateStoreQueryParams().setStoragePersona("p1");

    StoreConfigUpdater.applyOnChild(admin, clusterName, storeName, params);

    verify(personaRepo, times(1)).addStoresToPersona(eq("p1"), eq(Arrays.asList(storeName)));
  }

  /**
   * Covers the child-side lifecycle-hooks branch, which delegates to
   * {@code StoreLifecycleHooksPolicy.validateLifecycleHooks} and then calls
   * {@code admin.setStoreLifecycleHooks}.
   */
  @Test
  public void testApplyOnChild_StoreLifecycleHooks_InvokesAdminSetter() {
    String storeName = Utils.getUniqueString("child-lifecycle-hooks");
    VeniceHelixAdmin admin = newChildAdminMock(storeName);

    LifecycleHooksRecord hook = new LifecycleHooksRecordImpl("com.example.SomeHooks", Collections.emptyMap());
    UpdateStoreQueryParams params =
        new UpdateStoreQueryParams().setStoreLifecycleHooks(Collections.singletonList(hook));

    StoreConfigUpdater.applyOnChild(admin, clusterName, storeName, params);

    verify(admin, times(1)).setStoreLifecycleHooks(eq(clusterName), eq(storeName), any());
  }

  /**
   * Covers the child-side ingestion-pause branch: when {@code ingestionPauseMode} is set with a
   * regions list, the non-parent path computes {@code appliesToThisRegion} and writes through
   * {@code storeMetadataUpdate}. We don't assert the resolved persisted state here because the
   * inner lambda is not invoked on a mocked admin; the assertion is that the dispatch happened.
   */
  @Test
  public void testApplyOnChild_IngestionPauseModeWithRegions_InvokesStoreMetadataUpdate() {
    String storeName = Utils.getUniqueString("child-pause-regions");
    VeniceHelixAdmin admin = newChildAdminMock(storeName);
    doReturn("dc-local").when(admin).getRegionName();

    UpdateStoreQueryParams params = new UpdateStoreQueryParams().setIngestionPauseMode(IngestionPauseMode.ALL_VERSIONS)
        .setIngestionPausedRegions(Arrays.asList("dc-local", "dc-other"));

    StoreConfigUpdater.applyOnChild(admin, clusterName, storeName, params);

    verify(admin, atLeastOnce()).storeMetadataUpdate(eq(clusterName), eq(storeName), any());
  }

  /**
   * Covers the validation throw at the top of the ingestion-pause block: supplying
   * {@code ingestionPausedRegions} without {@code ingestionPauseMode} must surface as a 400.
   */
  @Test
  public void testApplyOnChild_IngestionPausedRegionsWithoutMode_Throws() {
    String storeName = Utils.getUniqueString("child-pause-no-mode");
    VeniceHelixAdmin admin = newChildAdminMock(storeName);

    UpdateStoreQueryParams params = new UpdateStoreQueryParams().setIngestionPausedRegions(Arrays.asList("dc-1"));

    try {
      StoreConfigUpdater.applyOnChild(admin, clusterName, storeName, params);
      fail("Expected VeniceHttpException because ingestionPausedRegions was set without ingestionPauseMode");
    } catch (VeniceHttpException expected) {
      // expected
    }
  }

  /**
   * Covers the hybrid block's outer guard: when any hybrid-related field is supplied,
   * {@link com.linkedin.venice.controller.HybridStoreConfigPolicy#mergeNewSettingsIntoOldHybridStoreConfig}
   * is invoked and the resulting non-empty {@code newHybridStoreConfig} drives a
   * {@code storeMetadataUpdate} dispatch (the inner lambda is the rollout to {@code Store}).
   */
  @Test
  public void testApplyOnChild_HybridBatchToHybridConversion_InvokesStoreMetadataUpdate() {
    String storeName = Utils.getUniqueString("child-batch-to-hybrid");
    VeniceHelixAdmin admin = newChildAdminMock(storeName);

    UpdateStoreQueryParams params =
        new UpdateStoreQueryParams().setHybridRewindSeconds(86400L).setHybridOffsetLagThreshold(1000L);

    StoreConfigUpdater.applyOnChild(admin, clusterName, storeName, params);

    verify(admin, atLeastOnce()).storeMetadataUpdate(eq(clusterName), eq(storeName), any());
  }

  /**
   * Covers the regions-filter early-return at the top of {@code applyOnChild}: when the supplied
   * filter does not include the current region, the method must short-circuit before invoking
   * any setter. Drives a no-op by supplying {@code owner} alongside the filter; the assertion is
   * that {@code setStoreOwner} was never called.
   */
  @Test
  public void testApplyOnChild_RegionsFilterExcludesCurrentRegion_NoOps() {
    String storeName = Utils.getUniqueString("child-region-skipped");
    VeniceHelixAdmin admin = newChildAdminMock(storeName);
    VeniceControllerMultiClusterConfig multi = admin.getMultiClusterConfigs();
    doReturn("dc-current").when(multi).getRegionName();

    UpdateStoreQueryParams params = new UpdateStoreQueryParams().setRegionsFilter("dc-other").setOwner("ignored");

    StoreConfigUpdater.applyOnChild(admin, clusterName, storeName, params);

    verify(admin, never()).setStoreOwner(any(), any(), any());
  }

  /**
   * Covers the {@code targetRegionPromoted=false} no-op path in {@code applyOnChild}: the flag is
   * write-only-true, so an explicit {@code false} (e.g. from a replicateAllConfigs snapshot of an
   * un-promoted store) must be treated as a no-op — {@code storeMetadataUpdate} must not be called.
   */
  @Test
  public void testApplyOnChild_TargetRegionPromotedFalse_DoesNotTriggerMetadataUpdate() {
    String storeName = Utils.getUniqueString("child-trp-false");
    VeniceHelixAdmin admin = newChildAdminMock(storeName);

    UpdateStoreQueryParams params = new UpdateStoreQueryParams().setTargetRegionPromoted(false);

    StoreConfigUpdater.applyOnChild(admin, clusterName, storeName, params);

    verify(admin, never()).storeMetadataUpdate(any(), any(), any());
  }

  /**
   * Covers the {@code applyOnParent} wiring for {@code targetRegionPromoted}: when the flag is set
   * to {@code true} in params, the parent round-trip must (a) set {@code setStore.targetRegionPromoted=true}
   * in the Avro message and (b) add {@link com.linkedin.venice.controllerapi.ControllerApiConstants#TARGET_REGION_PROMOTED}
   * to {@code updatedConfigsList}.
   */
  @Test
  public void testApplyOnParent_TargetRegionPromoted_AppearsInAvroAndUpdatedConfigsList() {
    String storeName = Utils.getUniqueString("parent-trp");
    Store store = TestUtils.createTestStore(storeName, "test-owner", System.currentTimeMillis());
    doReturn(store).when(internalAdmin).getStore(clusterName, storeName);
    parentAdmin.initStorageCluster(clusterName);

    UpdateStoreQueryParams params = new UpdateStoreQueryParams().setTargetRegionPromoted(true);
    parentAdmin.updateStore(clusterName, storeName, params);

    UpdateStore msg = captureLastUpdateStore();
    assertTrue(msg.targetRegionPromoted, "Avro field targetRegionPromoted must be true");
    Set<String> updatedKeys = msg.updatedConfigsList.stream().map(CharSequence::toString).collect(Collectors.toSet());
    assertTrue(
        updatedKeys.contains(TARGET_REGION_PROMOTED),
        "updatedConfigsList must contain '" + TARGET_REGION_PROMOTED + "' but got: " + updatedKeys);
  }

  /**
   * Covers the pre-check null-version branch in {@code applyOnChild}: when there is no future version
   * (store has no versions), {@code storeMetadataUpdate} must be skipped — the inner lambda would be
   * a no-op, so calling it would only cause an unnecessary ZK write.
   */
  @Test
  public void testApplyOnChild_NoFutureVersion_SkipsStoreMetadataUpdate() {
    String storeName = Utils.getUniqueString("child-trp-no-version");
    VeniceHelixAdmin admin = newChildAdminMock(storeName);
    // newChildAdminMock returns a store with no versions; getLargestUsedVersionNumber() == 0,
    // getVersion(0) == null → pre-check must skip storeMetadataUpdate.

    UpdateStoreQueryParams params = new UpdateStoreQueryParams().setTargetRegionPromoted(true);
    StoreConfigUpdater.applyOnChild(admin, clusterName, storeName, params);

    verify(admin, never()).storeMetadataUpdate(any(), any(), any());
  }

  /**
   * Covers the pre-check branch in {@code applyOnChild}: when the latest version is already promoted,
   * {@code storeMetadataUpdate} must be skipped entirely to avoid an unnecessary ZK write.
   */
  @Test
  public void testApplyOnChild_VersionAlreadyPromoted_SkipsStoreMetadataUpdate() {
    String storeName = Utils.getUniqueString("child-trp-already");
    VeniceHelixAdmin admin = newChildAdminMock(storeName);
    // Add a real version and mark it as already promoted
    Store store = admin.getStore(clusterName, storeName);
    Version v1 = new VersionImpl(storeName, 1, "push-id-1");
    store.addVersion(v1);
    store.setVersionTargetRegionPromoted(1, true);

    UpdateStoreQueryParams params = new UpdateStoreQueryParams().setTargetRegionPromoted(true);
    StoreConfigUpdater.applyOnChild(admin, clusterName, storeName, params);

    verify(admin, never()).storeMetadataUpdate(any(), any(), any());
  }

  /**
   * Covers the {@code applyOnParent} no-op path for {@code targetRegionPromoted=false}: the flag is
   * write-only-true, so an explicit {@code false} must NOT appear in {@code updatedConfigsList} and
   * must NOT set {@code targetRegionPromoted=true} in the Avro message.
   */
  @Test
  public void testApplyOnParent_TargetRegionPromotedFalse_NotInUpdatedConfigsList() {
    String storeName = Utils.getUniqueString("parent-trp-false");
    Store store = TestUtils.createTestStore(storeName, "test-owner", System.currentTimeMillis());
    doReturn(store).when(internalAdmin).getStore(clusterName, storeName);
    parentAdmin.initStorageCluster(clusterName);

    // Pair with an owner update so the command has at least one real change; otherwise the parent
    // throws "command didn't change any specific store config".
    UpdateStoreQueryParams params = new UpdateStoreQueryParams().setTargetRegionPromoted(false).setOwner("new-owner");
    parentAdmin.updateStore(clusterName, storeName, params);

    UpdateStore msg = captureLastUpdateStore();
    Set<String> updatedKeys = msg.updatedConfigsList.stream().map(CharSequence::toString).collect(Collectors.toSet());
    assertFalse(
        updatedKeys.contains(TARGET_REGION_PROMOTED),
        "updatedConfigsList must NOT contain '" + TARGET_REGION_PROMOTED + "' when value is false");
    assertFalse(msg.targetRegionPromoted, "Avro field must not be set to true when param is false");
  }

  // ===== helpers =====

  private UpdateStore captureLastUpdateStore() {
    ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> schemaCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(veniceWriter, atLeastOnce())
        .put(any(), valueCaptor.capture(), schemaCaptor.capture(), any(), any(), anyLong(), any(), any(), any(), any());
    List<byte[]> values = valueCaptor.getAllValues();
    List<Integer> schemas = schemaCaptor.getAllValues();
    byte[] lastValue = values.get(values.size() - 1);
    int lastSchema = schemas.get(schemas.size() - 1);
    AdminOperation op = adminOperationSerializer.deserialize(ByteBuffer.wrap(lastValue), lastSchema);
    return (UpdateStore) op.payloadUnion;
  }

  private Object readAvroField(UpdateStore msg, String fieldName) throws Exception {
    Field f = UpdateStore.class.getField(fieldName);
    return f.get(msg);
  }

  private boolean eqUnboxed(Object a, Object b) {
    if (a == null) {
      return b == null;
    }
    if (b == null) {
      return false;
    }
    if (a instanceof Number && b instanceof Number) {
      return ((Number) a).longValue() == ((Number) b).longValue();
    }
    return a.equals(b);
  }

  /**
   * Invokes {@code verify(admin).<setterName>(eq(cluster), eq(store), eq(value))} via reflection,
   * so the per-field descriptor list stays the single source of truth.
   */
  private void invokeVerify(VeniceHelixAdmin admin, TrivialField f, String store) throws Exception {
    VeniceHelixAdmin verifyHandle = verify(admin, times(1));
    Method m = VeniceHelixAdmin.class.getMethod(f.childSetterName, String.class, String.class, f.childSetterValueType);
    m.invoke(verifyHandle, clusterName, store, f.childExpectedArg);
  }

  /**
   * Builds a {@code VeniceHelixAdmin} mock with the minimum stubs needed for {@link
   * StoreConfigUpdater#applyOnChild} to reach every trivial-field setter call:
   * <ul>
   *   <li>{@code getStore(cluster, store)} returns a real {@link Store} so the early-return guard
   *       passes.</li>
   *   <li>{@code getHelixVeniceClusterResources(cluster).getConfig()} returns a mock so the
   *       unconditional {@code clusterConfig} fetch (before the hybrid block) doesn't NPE.</li>
   *   <li>{@code getMultiClusterConfigs()} returns a deep-stub so the regions-filter guard (only
   *       entered when {@code regionsFilter} is set) stays a no-op.</li>
   * </ul>
   */
  private VeniceHelixAdmin newChildAdminMock(String storeName) {
    VeniceHelixAdmin admin = mock(VeniceHelixAdmin.class);
    Store store = TestUtils.createTestStore(storeName, "test-owner", System.currentTimeMillis());
    doReturn(store).when(admin).getStore(clusterName, storeName);

    HelixVeniceClusterResources childResources = mock(HelixVeniceClusterResources.class);
    VeniceControllerClusterConfig childConfig = mock(VeniceControllerClusterConfig.class);
    doReturn(childConfig).when(childResources).getConfig();
    doReturn(mock(ZkRoutersClusterManager.class)).when(childResources).getRoutersClusterManager();
    doReturn(childResources).when(admin).getHelixVeniceClusterResources(clusterName);

    VeniceControllerMultiClusterConfig multi = mock(VeniceControllerMultiClusterConfig.class, RETURNS_DEEP_STUBS);
    doReturn(multi).when(admin).getMultiClusterConfigs();
    doReturn(new PubSubTopicRepository()).when(admin).getPubSubTopicRepository();

    return admin;
  }

  /**
   * A lifecycle hook that records the {@link VeniceProperties} passed to its constructor.
   * Uses {@link AtomicReference} to avoid writing to a static field from an instance method.
   */
  public static class PropsCapturingHook extends StoreLifecycleHooks {
    static final AtomicReference<VeniceProperties> capturedProps = new AtomicReference<>();

    public PropsCapturingHook(VeniceProperties props) {
      super(props);
      capturedProps.set(props);
    }
  }

  /**
   * Verifies that {@code applyOnParent} passes the real controller {@link VeniceProperties} to
   * the lifecycle hook constructor, not {@code VeniceProperties.empty()}. This is the core
   * behavioral contract introduced by this fix: hooks that require real infrastructure config
   * in their constructor must receive it during pre-flight validation.
   */
  @Test
  public void testApplyOnParent_LifecycleHookReceivesRealControllerProps() {
    String storeName = Utils.getUniqueString("parent-props-hook");
    Store store = TestUtils.createTestStore(storeName, "test-owner", System.currentTimeMillis());
    doReturn(store).when(internalAdmin).getStore(clusterName, storeName);

    Properties rawProps = new Properties();
    rawProps.setProperty("grpc.endpoint", "test-host:1234");
    VeniceProperties controllerProps = new VeniceProperties(rawProps);
    VeniceControllerClusterConfig mockClusterConfig = mock(VeniceControllerClusterConfig.class);
    doReturn(controllerProps).when(mockClusterConfig).getProps();
    VeniceControllerMultiClusterConfig mockMultiConfig = mock(VeniceControllerMultiClusterConfig.class);
    doReturn(mockClusterConfig).when(mockMultiConfig).getCommonConfig();
    doReturn(mockMultiConfig).when(internalAdmin).getMultiClusterConfigs();
    parentAdmin.initStorageCluster(clusterName);

    PropsCapturingHook.capturedProps.set(null);
    String hookClassName = StoreConfigUpdaterTest.class.getName() + "$PropsCapturingHook";
    LifecycleHooksRecord hook = new LifecycleHooksRecordImpl(hookClassName, Collections.emptyMap());
    UpdateStoreQueryParams params =
        new UpdateStoreQueryParams().setStoreLifecycleHooks(Collections.singletonList(hook));

    parentAdmin.updateStore(clusterName, storeName, params);

    assertSame(
        controllerProps,
        PropsCapturingHook.capturedProps.get(),
        "Hook constructor must receive the exact controller VeniceProperties instance");
  }

  /**
   * Verifies that a non-existent lifecycle hook class name causes {@code applyOnParent} to throw
   * a {@link VeniceException} — the class-existence check is preserved.
   */
  @Test(expectedExceptions = VeniceException.class)
  public void testApplyOnParent_LifecycleHookWithMissingClass_Throws() {
    String storeName = Utils.getUniqueString("parent-missing-hook");
    Store store = TestUtils.createTestStore(storeName, "test-owner", System.currentTimeMillis());
    doReturn(store).when(internalAdmin).getStore(clusterName, storeName);
    doReturn(mock(VeniceControllerMultiClusterConfig.class, RETURNS_DEEP_STUBS)).when(internalAdmin)
        .getMultiClusterConfigs();
    parentAdmin.initStorageCluster(clusterName);

    LifecycleHooksRecord hook =
        new LifecycleHooksRecordImpl("com.example.NonExistentLifecycleHook", Collections.emptyMap());
    UpdateStoreQueryParams params =
        new UpdateStoreQueryParams().setStoreLifecycleHooks(Collections.singletonList(hook));

    parentAdmin.updateStore(clusterName, storeName, params);
  }
}
