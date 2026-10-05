package com.linkedin.venice.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.ZkStoreConfigAccessor;
import com.linkedin.venice.meta.ReadWriteSchemaRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.schema.GeneratedSchemaID;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.avro.DirectionalSchemaCompatibilityType;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.avro.Schema;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link StoreSchemaManager}, the child-controller store schema operations extracted from
 * {@link VeniceHelixAdmin}. The manager reaches back into the admin only for cluster-leadership checks and
 * per-cluster resource / meta-store accessors, all of which are mocked here.
 */
public class TestStoreSchemaManager {
  private static final String CLUSTER = "test-cluster";
  private static final String STORE = "test-store";
  private static final String STRING_SCHEMA = "\"string\"";
  private static final String RECORD_SCHEMA =
      "{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":[{\"name\":\"f1\",\"type\":\"int\",\"default\":0}]}";
  private static final String OTHER_RECORD_SCHEMA =
      "{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":[{\"name\":\"f2\",\"type\":\"string\",\"default\":\"\"}]}";

  private VeniceHelixAdmin admin;
  private HelixVeniceClusterResources resources;
  private ReadWriteSchemaRepository schemaRepo;
  private ZkStoreConfigAccessor storeConfigAccessor;
  private VeniceControllerClusterConfig config;
  private StoreSchemaManager manager;

  @BeforeMethod
  public void setUp() {
    admin = mock(VeniceHelixAdmin.class);
    resources = mock(HelixVeniceClusterResources.class);
    schemaRepo = mock(ReadWriteSchemaRepository.class);
    storeConfigAccessor = mock(ZkStoreConfigAccessor.class);
    config = mock(VeniceControllerClusterConfig.class);
    doReturn(resources).when(admin).getHelixVeniceClusterResources(CLUSTER);
    doReturn(storeConfigAccessor).when(admin).getStoreConfigAccessor(anyString());
    doReturn(false).when(storeConfigAccessor).containsConfig(anyString());
    doReturn(schemaRepo).when(resources).getSchemaRepository();
    doReturn(config).when(resources).getConfig();
    manager = new StoreSchemaManager(admin);
  }

  @Test
  public void testGetReplicationMetadataSchemaPresentAndAbsent() {
    doReturn(new RmdSchemaEntry(1, 1, RECORD_SCHEMA)).when(schemaRepo).getReplicationMetadataSchema(STORE, 1, 1);
    Optional<Schema> present = manager.getReplicationMetadataSchema(CLUSTER, STORE, 1, 1);
    assertTrue(present.isPresent());

    doReturn(null).when(schemaRepo).getReplicationMetadataSchema(STORE, 2, 1);
    assertFalse(manager.getReplicationMetadataSchema(CLUSTER, STORE, 2, 1).isPresent());
    verify(admin, org.mockito.Mockito.atLeastOnce()).checkControllerLeadershipFor(CLUSTER);
  }

  @Test
  public void testGetInUseValueSchemaIdsOnParentReturnsEmpty() {
    doReturn(true).when(admin).isParent();
    assertTrue(manager.getInUseValueSchemaIds(CLUSTER, STORE).isEmpty());
    verify(admin, never()).getStore(anyString(), anyString());
  }

  @Test
  public void testGetInUseValueSchemaIdsCollectsAcrossVersions() {
    doReturn(false).when(admin).isParent();
    Store store = mock(Store.class);
    Version v1 = mock(Version.class);
    doReturn(1).when(v1).getNumber();
    doReturn(Collections.singletonList(v1)).when(store).getVersions();
    doReturn(store).when(admin).getStore(CLUSTER, STORE);

    StoreMetaValue metaValue = new StoreMetaValue();
    metaValue.storeValueSchemaIdsWrittenPerStoreVersion = Arrays.asList(3, 7);
    doReturn(metaValue).when(admin).getMetaStoreValue(any(), eq(STORE));

    Set<Integer> ids = manager.getInUseValueSchemaIds(CLUSTER, STORE);
    assertEquals(ids, new java.util.HashSet<>(Arrays.asList(3, 7)));
  }

  @Test
  public void testGetInUseValueSchemaIdsThrowsWhenMetaValueMissing() {
    doReturn(false).when(admin).isParent();
    Store store = mock(Store.class);
    Version v1 = mock(Version.class);
    doReturn(1).when(v1).getNumber();
    doReturn(Collections.singletonList(v1)).when(store).getVersions();
    doReturn(store).when(admin).getStore(CLUSTER, STORE);
    doReturn(null).when(admin).getMetaStoreValue(any(), eq(STORE));

    assertThrows(VeniceException.class, () -> manager.getInUseValueSchemaIds(CLUSTER, STORE));
  }

  @Test
  public void testDeleteValueSchemasRemovesUnused() {
    StoreSchemaManager spied = org.mockito.Mockito.spy(manager);
    doReturn(Collections.singleton(1)).when(spied).getInUseValueSchemaIds(CLUSTER, STORE);

    spied.deleteValueSchemas(CLUSTER, STORE, new java.util.HashSet<>(Arrays.asList(2, 3)));
    verify(schemaRepo).removeValueSchema(STORE, 2);
    verify(schemaRepo).removeValueSchema(STORE, 3);
  }

  @Test
  public void testDeleteValueSchemasThrowsWhenSchemaInUse() {
    StoreSchemaManager spied = org.mockito.Mockito.spy(manager);
    doReturn(new java.util.HashSet<>(Arrays.asList(1, 2))).when(spied).getInUseValueSchemaIds(CLUSTER, STORE);

    assertThrows(
        VeniceException.class,
        () -> spied.deleteValueSchemas(CLUSTER, STORE, new java.util.HashSet<>(Arrays.asList(2, 4))));
    verify(schemaRepo, never()).removeValueSchema(anyString(), anyInt());
  }

  @Test
  public void testSimpleSchemaReadsDelegate() {
    SchemaEntry keySchema = new SchemaEntry(0, STRING_SCHEMA);
    doReturn(keySchema).when(schemaRepo).getKeySchema(STORE);
    assertEquals(manager.getKeySchema(CLUSTER, STORE), keySchema);

    List<SchemaEntry> valueSchemas = Collections.singletonList(new SchemaEntry(1, RECORD_SCHEMA));
    doReturn(valueSchemas).when(schemaRepo).getValueSchemas(STORE);
    assertEquals(manager.getValueSchemas(CLUSTER, STORE), valueSchemas);

    List<DerivedSchemaEntry> derived = Collections.singletonList(new DerivedSchemaEntry(1, 1, RECORD_SCHEMA));
    doReturn(derived).when(schemaRepo).getDerivedSchemas(STORE);
    assertEquals(manager.getDerivedSchemas(CLUSTER, STORE), derived);

    SchemaEntry valueSchema = new SchemaEntry(2, RECORD_SCHEMA);
    doReturn(valueSchema).when(schemaRepo).getValueSchema(STORE, 2);
    assertEquals(manager.getValueSchema(CLUSTER, STORE, 2), valueSchema);

    List<RmdSchemaEntry> rmdSchemas = Collections.singletonList(new RmdSchemaEntry(1, 1, RECORD_SCHEMA));
    doReturn(rmdSchemas).when(schemaRepo).getReplicationMetadataSchemas(STORE);
    assertEquals(manager.getReplicationMetadataSchemas(CLUSTER, STORE), rmdSchemas);
  }

  @Test
  public void testGetValueSchemaIdValidatesOnlyWhenFound() {
    doReturn(SchemaData.INVALID_VALUE_SCHEMA_ID).when(schemaRepo).getValueSchemaId(STORE, STRING_SCHEMA);
    assertEquals(manager.getValueSchemaId(CLUSTER, STORE, STRING_SCHEMA), SchemaData.INVALID_VALUE_SCHEMA_ID);

    doReturn(5).when(schemaRepo).getValueSchemaId(STORE, RECORD_SCHEMA);
    assertEquals(manager.getValueSchemaId(CLUSTER, STORE, RECORD_SCHEMA), 5);
  }

  @Test
  public void testGetDerivedSchemaIdValidatesOnlyWhenValid() {
    doReturn(GeneratedSchemaID.INVALID).when(schemaRepo).getDerivedSchemaId(STORE, STRING_SCHEMA);
    assertEquals(manager.getDerivedSchemaId(CLUSTER, STORE, STRING_SCHEMA), GeneratedSchemaID.INVALID);

    GeneratedSchemaID valid = new GeneratedSchemaID(1, 2);
    doReturn(valid).when(schemaRepo).getDerivedSchemaId(STORE, RECORD_SCHEMA);
    assertEquals(manager.getDerivedSchemaId(CLUSTER, STORE, RECORD_SCHEMA), valid);
  }

  @Test
  public void testAddValueSchemaResolvesRealIdOnDuplicate() {
    // Non-duplicate: returned id is used as-is.
    doReturn(new SchemaEntry(7, RECORD_SCHEMA)).when(schemaRepo)
        .addValueSchema(STORE, RECORD_SCHEMA, DirectionalSchemaCompatibilityType.FULL);
    SchemaEntry nonDup = manager.addValueSchema(CLUSTER, STORE, RECORD_SCHEMA, DirectionalSchemaCompatibilityType.FULL);
    assertEquals(nonDup.getId(), 7);

    // Duplicate: the real id is looked up.
    doReturn(new SchemaEntry(SchemaData.DUPLICATE_VALUE_SCHEMA_CODE, OTHER_RECORD_SCHEMA)).when(schemaRepo)
        .addValueSchema(STORE, OTHER_RECORD_SCHEMA, DirectionalSchemaCompatibilityType.FULL);
    doReturn(9).when(schemaRepo).getValueSchemaId(STORE, OTHER_RECORD_SCHEMA);
    SchemaEntry dup =
        manager.addValueSchema(CLUSTER, STORE, OTHER_RECORD_SCHEMA, DirectionalSchemaCompatibilityType.FULL);
    assertEquals(dup.getId(), 9);
  }

  @Test
  public void testAddValueSchemaWithExpectedIdMatchesAndMismatches() {
    // Matching id.
    doReturn(5).when(schemaRepo)
        .preCheckValueSchemaAndGetNextAvailableId(STORE, RECORD_SCHEMA, DirectionalSchemaCompatibilityType.FULL);
    doReturn(new SchemaEntry(5, RECORD_SCHEMA)).when(schemaRepo).addValueSchema(STORE, RECORD_SCHEMA, 5);
    assertEquals(
        manager.addValueSchema(CLUSTER, STORE, RECORD_SCHEMA, 5, DirectionalSchemaCompatibilityType.FULL).getId(),
        5);

    // Duplicate code short-circuits the mismatch guard.
    doReturn(SchemaData.DUPLICATE_VALUE_SCHEMA_CODE).when(schemaRepo)
        .preCheckValueSchemaAndGetNextAvailableId(STORE, OTHER_RECORD_SCHEMA, DirectionalSchemaCompatibilityType.FULL);
    doReturn(new SchemaEntry(8, OTHER_RECORD_SCHEMA)).when(schemaRepo)
        .addValueSchema(STORE, OTHER_RECORD_SCHEMA, SchemaData.DUPLICATE_VALUE_SCHEMA_CODE);
    assertEquals(
        manager.addValueSchema(CLUSTER, STORE, OTHER_RECORD_SCHEMA, 8, DirectionalSchemaCompatibilityType.FULL).getId(),
        8);

    // Mismatching id throws.
    doReturn(6).when(schemaRepo)
        .preCheckValueSchemaAndGetNextAvailableId(STORE, RECORD_SCHEMA, DirectionalSchemaCompatibilityType.BACKWARD);
    assertThrows(
        VeniceException.class,
        () -> manager.addValueSchema(CLUSTER, STORE, RECORD_SCHEMA, 5, DirectionalSchemaCompatibilityType.BACKWARD));
  }

  @DataProvider(name = "migrationSchemaContent")
  public static Object[][] migrationSchemaContent() {
    String withProperty = RECORD_SCHEMA.replace("\"fields\":", "\"metadata\":{\"a\":1,\"b\":2},\"fields\":");
    String namedAlias = RECORD_SCHEMA.replace("\"fields\":", "\"aliases\":[\"OldRecord\"],\"fields\":");
    String fieldAlias = RECORD_SCHEMA.replace("\"name\":\"f1\"", "\"name\":\"f1\",\"aliases\":[\"oldField\"]");
    String enumSchema = "{\"type\":\"enum\",\"name\":\"Choice\",\"symbols\":[\"A\",\"B\"],\"default\":\"A\"}";
    String logicalType = "{\"type\":\"int\",\"logicalType\":\"date\"}";
    String floatDefault =
        "{\"type\":\"record\",\"name\":\"Value\",\"fields\":[{\"name\":\"n\",\"type\":\"float\",\"default\":0.0}]}";
    return new Object[][] { { RECORD_SCHEMA, RECORD_SCHEMA, true },
        { withProperty,
            "{ \"fields\": [{ \"default\": 0, \"type\": {\"type\":\"int\"}, \"name\": \"f1\" }],"
                + " \"metadata\": {\"b\":2,\"a\":1}, \"name\": \"TestRecord\", \"type\": \"record\" }",
            true },
        { namedAlias, namedAlias.replace("OldRecord", "OtherRecord"), false },
        { fieldAlias, fieldAlias.replace("oldField", "otherField"), false },
        { enumSchema, enumSchema.replace("\"default\":\"A\"", "\"default\":\"B\""), false },
        { logicalType, logicalType.replace("date", "time-millis"), false },
        { withProperty, withProperty.replace("\"b\":2", "\"b\":3"), false },
        { "[\"string\",\"int\"]", "[\"int\",\"string\"]", false },
        { RECORD_SCHEMA, RECORD_SCHEMA.replace("\"default\":0", "\"default\":1"), false },
        { floatDefault, floatDefault.replace(":0.0}", ":0}"), true } };
  }

  private StoreConfig configureMigrationDestination() {
    StoreConfig migration = new StoreConfig(STORE);
    migration.setCluster("source");
    migration.setMigrationSrcCluster("source");
    migration.setMigrationDestCluster(CLUSTER);
    doReturn(true).when(storeConfigAccessor).containsConfig(STORE);
    doReturn(migration).when(storeConfigAccessor).getStoreConfig(STORE);
    return migration;
  }

  @Test
  public void testMigrationPreservesSourceIdsForDuplicateSchemas() {
    configureMigrationDestination();
    doReturn(SchemaData.DUPLICATE_VALUE_SCHEMA_CODE).when(schemaRepo)
        .preCheckValueSchemaAndGetNextAvailableId(STORE, RECORD_SCHEMA, DirectionalSchemaCompatibilityType.FULL);
    for (int id: new int[] { 274, 288, 315 }) {
      SchemaEntry entry = new SchemaEntry(id, RECORD_SCHEMA);
      doReturn(entry).when(schemaRepo).addValueSchema(STORE, RECORD_SCHEMA, id);
      assertEquals(
          manager.addValueSchema(CLUSTER, STORE, RECORD_SCHEMA, id, DirectionalSchemaCompatibilityType.FULL),
          entry);
      verify(schemaRepo).addValueSchema(STORE, RECORD_SCHEMA, id);
    }
  }

  @Test(dataProvider = "migrationSchemaContent")
  public void testMigrationValidatesExistingSchemaContent(
      String requestedSchema,
      String storedSchema,
      boolean matches) {
    configureMigrationDestination();
    SchemaEntry stored = new SchemaEntry(500, storedSchema);
    doReturn(stored).when(schemaRepo).getValueSchema(STORE, 500);
    if (matches) {
      assertSame(
          manager.addValueSchema(CLUSTER, STORE, requestedSchema, 500, DirectionalSchemaCompatibilityType.FULL),
          stored);
    } else {
      assertThrows(
          VeniceException.class,
          () -> manager.addValueSchema(CLUSTER, STORE, requestedSchema, 500, DirectionalSchemaCompatibilityType.FULL));
    }
    verify(schemaRepo, never()).addValueSchema(STORE, requestedSchema, 500);
  }

  @Test
  public void testMigrationRejectsNonPositiveSchemaId() {
    configureMigrationDestination();
    assertThrows(
        IllegalArgumentException.class,
        () -> manager.addValueSchema(CLUSTER, STORE, RECORD_SCHEMA, 0, DirectionalSchemaCompatibilityType.FULL));
    verify(schemaRepo, never()).addValueSchema(anyString(), anyString(), anyInt());
  }

  @Test
  public void testMigrationRequiresDestinationProvenance() {
    StoreConfig migration = configureMigrationDestination();
    manager.validateMigrationValueSchema(CLUSTER, STORE, RECORD_SCHEMA, 274, DirectionalSchemaCompatibilityType.FULL);
    migration.setMigrationDestCluster("elsewhere");
    assertThrows(
        VeniceException.class,
        () -> manager
            .validateMigrationValueSchema(CLUSTER, STORE, RECORD_SCHEMA, 274, DirectionalSchemaCompatibilityType.FULL));
  }

  @Test
  public void testMigrationRejectsMissingSourceMetadataBeforeSchemaWrite() {
    StoreConfig migration = configureMigrationDestination();
    for (String sourceCluster: new String[] { null, "" }) {
      migration.setMigrationSrcCluster(sourceCluster);
      assertThrows(
          VeniceException.class,
          () -> manager.addValueSchema(CLUSTER, STORE, RECORD_SCHEMA, 274, DirectionalSchemaCompatibilityType.FULL));
    }
    verify(schemaRepo, never()).addValueSchema(anyString(), anyString(), anyInt());
  }

  @Test
  public void testCompletedMigrationUsesNormalSchemaIdAllocation() {
    StoreConfig migration = configureMigrationDestination();
    migration.setCluster(CLUSTER);
    Store store = mock(Store.class);
    doReturn(store).when(admin).getStore(CLUSTER, STORE);
    assertFalse(manager.isStoreMigrationDestination(CLUSTER, STORE));
    doReturn(316).when(schemaRepo)
        .preCheckValueSchemaAndGetNextAvailableId(STORE, RECORD_SCHEMA, DirectionalSchemaCompatibilityType.FULL);
    assertThrows(
        VeniceException.class,
        () -> manager.addValueSchema(CLUSTER, STORE, RECORD_SCHEMA, 400, DirectionalSchemaCompatibilityType.FULL));
  }

  @Test
  public void testAddDerivedSchemaDelegates() {
    doReturn(new GeneratedSchemaID(1, 3)).when(schemaRepo).getDerivedSchemaId(STORE, RECORD_SCHEMA);
    DerivedSchemaEntry entry = manager.addDerivedSchema(CLUSTER, STORE, 1, RECORD_SCHEMA);
    assertEquals(entry.getValueSchemaID(), 1);
    assertEquals(entry.getId(), 3);
    verify(schemaRepo).addDerivedSchema(STORE, RECORD_SCHEMA, 1);

    DerivedSchemaEntry explicit = new DerivedSchemaEntry(1, 4, RECORD_SCHEMA);
    doReturn(explicit).when(schemaRepo).addDerivedSchema(STORE, RECORD_SCHEMA, 1, 4);
    assertEquals(manager.addDerivedSchema(CLUSTER, STORE, 1, 4, RECORD_SCHEMA), explicit);
  }

  @Test
  public void testRemoveDerivedSchemaDelegates() {
    DerivedSchemaEntry removed = new DerivedSchemaEntry(1, 2, RECORD_SCHEMA);
    doReturn(removed).when(schemaRepo).removeDerivedSchema(STORE, 1, 2);
    assertEquals(manager.removeDerivedSchema(CLUSTER, STORE, 1, 2), removed);
  }

  @Test
  public void testAddSupersetSchemaWhenSupersetMissing() {
    doReturn(null).when(schemaRepo).getValueSchema(STORE, 9);
    doReturn(new SchemaEntry(1, RECORD_SCHEMA)).when(schemaRepo).addValueSchema(eq(STORE), anyString(), anyInt());

    manager.addSupersetSchema(CLUSTER, STORE, RECORD_SCHEMA, 1, RECORD_SCHEMA, 9);
    // The missing superset schema is registered first.
    verify(schemaRepo).addValueSchema(STORE, RECORD_SCHEMA, 9);
    // Then the value schema is added.
    verify(schemaRepo).addValueSchema(STORE, RECORD_SCHEMA, 1);
  }

  @Test
  public void testAddSupersetSchemaWhenExistingMatches() {
    doReturn(new SchemaEntry(9, RECORD_SCHEMA)).when(schemaRepo).getValueSchema(STORE, 9);
    doReturn(new SchemaEntry(1, RECORD_SCHEMA)).when(schemaRepo).addValueSchema(STORE, RECORD_SCHEMA, 1);

    manager.addSupersetSchema(CLUSTER, STORE, RECORD_SCHEMA, 1, RECORD_SCHEMA, 9);
    // Existing superset matches, so it is NOT re-added; only the value schema is added.
    verify(schemaRepo, never()).addValueSchema(STORE, RECORD_SCHEMA, 9);
    verify(schemaRepo).addValueSchema(STORE, RECORD_SCHEMA, 1);
  }

  @Test
  public void testAddSupersetSchemaThrowsWhenExistingMismatch() {
    doReturn(new SchemaEntry(9, RECORD_SCHEMA)).when(schemaRepo).getValueSchema(STORE, 9);
    assertThrows(
        VeniceException.class,
        () -> manager.addSupersetSchema(CLUSTER, STORE, RECORD_SCHEMA, 1, OTHER_RECORD_SCHEMA, 9));
  }

  @Test
  public void testGetValueSchemaIdIgnoreFieldOrderFoundAndNotFound() {
    doReturn(Collections.singletonList(new SchemaEntry(4, RECORD_SCHEMA))).when(schemaRepo).getValueSchemas(STORE);

    Comparator<Schema> alwaysEqual = (s1, s2) -> 0;
    assertEquals(manager.getValueSchemaIdIgnoreFieldOrder(CLUSTER, STORE, RECORD_SCHEMA, alwaysEqual), 4);

    Comparator<Schema> neverEqual = (s1, s2) -> 1;
    assertEquals(
        manager.getValueSchemaIdIgnoreFieldOrder(CLUSTER, STORE, RECORD_SCHEMA, neverEqual),
        SchemaData.INVALID_VALUE_SCHEMA_ID);
  }

  @Test
  public void testCheckPreConditionForAddValueSchemaWithValidationDisabled() {
    doReturn(false).when(config).isControllerSchemaValidationEnabled();
    doReturn(11).when(schemaRepo)
        .preCheckValueSchemaAndGetNextAvailableId(STORE, RECORD_SCHEMA, DirectionalSchemaCompatibilityType.FULL);
    assertEquals(
        manager.checkPreConditionForAddValueSchemaAndGetNewSchemaId(
            CLUSTER,
            STORE,
            RECORD_SCHEMA,
            DirectionalSchemaCompatibilityType.FULL),
        11);
  }

  @Test
  public void testCheckPreConditionForAddValueSchemaWithValidationEnabledNoExistingSchemas() {
    doReturn(true).when(config).isControllerSchemaValidationEnabled();
    doReturn(Collections.emptyList()).when(schemaRepo).getValueSchemas(STORE);
    doReturn(12).when(schemaRepo)
        .preCheckValueSchemaAndGetNextAvailableId(STORE, RECORD_SCHEMA, DirectionalSchemaCompatibilityType.FULL);
    assertEquals(
        manager.checkPreConditionForAddValueSchemaAndGetNewSchemaId(
            CLUSTER,
            STORE,
            RECORD_SCHEMA,
            DirectionalSchemaCompatibilityType.FULL),
        12);
  }

  @Test
  public void testCheckPreConditionForAddDerivedSchemaDelegates() {
    doReturn(13).when(schemaRepo).preCheckDerivedSchemaAndGetNextAvailableId(STORE, 1, RECORD_SCHEMA);
    assertEquals(manager.checkPreConditionForAddDerivedSchemaAndGetNewSchemaId(CLUSTER, STORE, 1, RECORD_SCHEMA), 13);
  }

  @Test
  public void testCheckIfValueSchemaAlreadyHasRmdSchema() {
    doReturn(Collections.singletonList(new RmdSchemaEntry(2, 1, RECORD_SCHEMA))).when(schemaRepo)
        .getReplicationMetadataSchemas(STORE);
    assertTrue(manager.checkIfValueSchemaAlreadyHasRmdSchema(CLUSTER, STORE, 2, 1));
    assertFalse(manager.checkIfValueSchemaAlreadyHasRmdSchema(CLUSTER, STORE, 2, 9));
  }

  @Test
  public void testCheckIfMetadataSchemaAlreadyPresentMatchMissAndException() {
    RmdSchemaEntry entry = new RmdSchemaEntry(2, 1, RECORD_SCHEMA);
    doReturn(Collections.singletonList(entry)).when(schemaRepo).getReplicationMetadataSchemas(STORE);
    assertTrue(manager.checkIfMetadataSchemaAlreadyPresent(CLUSTER, STORE, new RmdSchemaEntry(2, 1, RECORD_SCHEMA)));
    assertFalse(manager.checkIfMetadataSchemaAlreadyPresent(CLUSTER, STORE, new RmdSchemaEntry(2, 9, RECORD_SCHEMA)));

    // Exception during lookup is swallowed and treated as "not present".
    doThrow(new VeniceException("boom")).when(schemaRepo).getReplicationMetadataSchemas(STORE);
    assertFalse(manager.checkIfMetadataSchemaAlreadyPresent(CLUSTER, STORE, entry));
  }

  @Test
  public void testAddReplicationMetadataSchemaAddsWhenAbsent() {
    doReturn(Collections.emptyList()).when(schemaRepo).getReplicationMetadataSchemas(STORE);
    RmdSchemaEntry added = new RmdSchemaEntry(2, 1, RECORD_SCHEMA);
    doReturn(added).when(schemaRepo).addReplicationMetadataSchema(STORE, 2, RECORD_SCHEMA, 1);

    RmdSchemaEntry result = manager.addReplicationMetadataSchema(CLUSTER, STORE, 2, 1, RECORD_SCHEMA);
    assertEquals(result.getValueSchemaID(), 2);
    verify(schemaRepo).addReplicationMetadataSchema(STORE, 2, RECORD_SCHEMA, 1);
  }

  @Test
  public void testAddReplicationMetadataSchemaSkipsWhenAlreadyPresent() {
    doReturn(Collections.singletonList(new RmdSchemaEntry(2, 1, RECORD_SCHEMA))).when(schemaRepo)
        .getReplicationMetadataSchemas(STORE);

    RmdSchemaEntry result = manager.addReplicationMetadataSchema(CLUSTER, STORE, 2, 1, RECORD_SCHEMA);
    assertEquals(result.getValueSchemaID(), 2);
    verify(schemaRepo, never()).addReplicationMetadataSchema(anyString(), anyInt(), anyString(), anyInt());
  }

  @Test
  public void testGetSupersetOrLatestValueSchemaPresentAndAbsent() {
    Store store = mock(Store.class);
    doReturn(STORE).when(store).getName();

    doReturn(new SchemaEntry(3, RECORD_SCHEMA)).when(schemaRepo).getSupersetOrLatestValueSchema(STORE);
    assertEquals(
        manager.getSupersetOrLatestValueSchema(CLUSTER, store).toString(),
        new SchemaEntry(3, RECORD_SCHEMA).getSchema().toString());

    doReturn(null).when(schemaRepo).getSupersetOrLatestValueSchema(STORE);
    assertNull(manager.getSupersetOrLatestValueSchema(CLUSTER, store));
  }
}
