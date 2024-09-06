package com.linkedin.venice.helix;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;

import com.linkedin.alpini.io.IOUtils;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.exceptions.InvalidVeniceSchemaException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.schema.GeneratedSchemaID;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.writer.update.UpdateBuilderImplTest;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.avro.Schema;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class HelixReadOnlySchemaRepositoryTest {
  private static final Logger LOGGER = LogManager.getLogger(UpdateBuilderImplTest.class);

  private static final Schema VALUE_SCHEMA =
      AvroCompatibilityHelper.parse(loadFileAsString("TestWriteComputeBuilder.avsc"));
  private static final Schema UPDATE_SCHEMA =
      WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(VALUE_SCHEMA);

  @BeforeClass
  public void beforeClass() {
    HelixReadOnlySchemaRepository.setForceRefreshSupersetSchemaMaxDelay(Duration.ofSeconds(2));
  }

  @AfterClass
  public void afterClass() {
    HelixReadOnlySchemaRepository.resetForceRefreshSupersetSchemaMaxDelay();
  }

  @Test
  public void testMaybeSubscribeAndPopulateSchema() {
    HelixReadOnlySchemaRepository schemaRepository = mock(HelixReadOnlySchemaRepository.class);
    doCallRealMethod().when(schemaRepository).maybeRegisterAndPopulateRmdSchema(any(), any());
    doCallRealMethod().when(schemaRepository).maybeRegisterAndPopulateUpdateSchema(any(), any());
    Store store = mock(Store.class);
    String storeName = "testStore";
    when(store.getName()).thenReturn(storeName);
    when(store.isWriteComputationEnabled()).thenReturn(true);
    when(store.isActiveActiveReplicationEnabled()).thenReturn(true);
    SchemaEntry schemaEntry = new SchemaEntry(1, VALUE_SCHEMA.toString());
    DerivedSchemaEntry derivedSchemaEntry = new DerivedSchemaEntry(1, 1, UPDATE_SCHEMA.toString());
    RmdSchemaEntry rmdSchemaEntry = new RmdSchemaEntry(1, 1, UPDATE_SCHEMA.toString());
    HelixSchemaAccessor schemaAccessor = mock(HelixSchemaAccessor.class);
    when(schemaRepository.getAccessor()).thenReturn(schemaAccessor);
    when(schemaAccessor.getAllValueSchemas(storeName)).thenReturn(Collections.singletonList(schemaEntry));
    when(schemaAccessor.getAllDerivedSchemas(storeName)).thenReturn(Collections.singletonList(derivedSchemaEntry));
    when(schemaAccessor.getAllReplicationMetadataSchemas(storeName))
        .thenReturn(Collections.singletonList(rmdSchemaEntry));
    SchemaData schemaData = new SchemaData(storeName, null);

    schemaRepository.maybeRegisterAndPopulateUpdateSchema(store, schemaData);
    verify(schemaAccessor, times(1)).subscribeDerivedSchemaCreationChange(anyString(), any());
    Assert.assertEquals(schemaData.getDerivedSchema(1, 1).getSchema(), UPDATE_SCHEMA);
    schemaRepository.maybeRegisterAndPopulateRmdSchema(store, schemaData);
    verify(schemaAccessor, times(1)).subscribeReplicationMetadataSchemaCreationChange(anyString(), any());
    Assert.assertEquals(schemaData.getReplicationMetadataSchema(1, 1).getSchema(), UPDATE_SCHEMA);
  }

  /**
   * N.B.: This test takes 4 minutes in the absence of {@link HelixReadOnlySchemaRepository#setForceRefreshSupersetSchemaMaxDelay(Duration)}
   */
  @Test
  public void testForceRefreshSchemaData() {
    HelixReadOnlySchemaRepository schemaRepository = mock(HelixReadOnlySchemaRepository.class);
    doCallRealMethod().when(schemaRepository).forceRefreshSchemaData(any(), any());
    ReadWriteLock lock = new ReentrantReadWriteLock();
    when(schemaRepository.getSchemaLock()).thenReturn(lock);
    Store store = mock(Store.class);
    String storeName = "testStore";
    when(store.getName()).thenReturn(storeName);
    when(store.isWriteComputationEnabled()).thenReturn(false);
    when(store.isActiveActiveReplicationEnabled()).thenReturn(false);
    SchemaData schemaData = new SchemaData(storeName, null);

    SchemaEntry schemaEntry = new SchemaEntry(1, VALUE_SCHEMA.toString());
    DerivedSchemaEntry derivedSchemaEntry = new DerivedSchemaEntry(1, 1, UPDATE_SCHEMA.toString());
    RmdSchemaEntry rmdSchemaEntry = new RmdSchemaEntry(1, 1, UPDATE_SCHEMA.toString());

    HelixSchemaAccessor schemaAccessor = mock(HelixSchemaAccessor.class);
    when(schemaRepository.getAccessor()).thenReturn(schemaAccessor);
    when(schemaAccessor.getAllValueSchemas(storeName)).thenReturn(Collections.singletonList(schemaEntry));
    when(schemaAccessor.getAllDerivedSchemas(storeName)).thenReturn(Collections.singletonList(derivedSchemaEntry));
    when(schemaAccessor.getAllReplicationMetadataSchemas(storeName))
        .thenReturn(Collections.singletonList(rmdSchemaEntry));

    schemaRepository.forceRefreshSchemaData(store, schemaData);
    Assert.assertEquals(schemaData.getValueSchema(1).getSchema(), VALUE_SCHEMA);
    when(store.isWriteComputationEnabled()).thenReturn(true);
    schemaRepository.forceRefreshSchemaData(store, schemaData);
    Assert.assertEquals(schemaData.getDerivedSchema(1, 1).getSchema(), UPDATE_SCHEMA);
    when(store.isActiveActiveReplicationEnabled()).thenReturn(true);
    schemaRepository.forceRefreshSchemaData(store, schemaData);
    Assert.assertEquals(schemaData.getReplicationMetadataSchema(1, 1).getSchema(), UPDATE_SCHEMA);

    HelixReadOnlyStoreRepository storeRepository = mock(HelixReadOnlyStoreRepository.class);
    when(storeRepository.getStore(storeName)).thenReturn(store);
    when(storeRepository.getStoreOrThrow(storeName)).thenReturn(store);
    when(schemaRepository.getStoreRepository()).thenReturn(storeRepository);

    Map<String, SchemaData> schemaDataMap = new VeniceConcurrentHashMap<>();
    schemaDataMap.put(storeName, new SchemaData(storeName, null));
    when(schemaRepository.getSupersetSchema(storeName)).thenCallRealMethod();
    when(schemaRepository.isSupersetSchemaReadyToServe(any(), any(), anyInt())).thenCallRealMethod();
    doCallRealMethod().when(schemaRepository).forceRefreshSupersetSchemaWithRetry(anyString());
    when(schemaRepository.getSchemaMap()).thenReturn(schemaDataMap);
    verify(schemaRepository, times(3)).forceRefreshSchemaData(any(), any());

    // A force refresh should update the schemas.
    when(store.getLatestSuperSetValueSchemaId()).thenReturn(1);
    Assert.assertTrue(schemaRepository.getSupersetSchema(storeName) != null);
    Assert.assertEquals(schemaRepository.getSupersetSchema(storeName).getSchema(), VALUE_SCHEMA);
    verify(schemaRepository, times(4)).forceRefreshSchemaData(any(), any());
    // 3 times force refresh still won't get the schema, exception should be thrown.
    when(store.getLatestSuperSetValueSchemaId()).thenReturn(2);
    Assert.assertThrows(InvalidVeniceSchemaException.class, () -> schemaRepository.getSupersetSchema(storeName));
    verify(schemaRepository, times(14)).forceRefreshSchemaData(any(), any());

    when(store.getLatestSuperSetValueSchemaId()).thenReturn(SchemaData.INVALID_VALUE_SCHEMA_ID);
    Assert.assertNull(schemaRepository.getSupersetSchema(storeName));
  }

  @Test(dataProviderClass = DataProviderUtils.class, dataProvider = "Two-True-and-False")
  public void getDerivedSchemaIdTest(boolean wcEnabled, boolean aaEnabled) {
    ReadOnlyStoreRepository storeRepository = mock(ReadOnlyStoreRepository.class);
    ZkClient zkClient = mock(ZkClient.class);
    HelixSchemaAccessor accessor = mock(HelixSchemaAccessor.class);
    HelixReadOnlySchemaRepository schemaRepository =
        new HelixReadOnlySchemaRepository(storeRepository, zkClient, accessor, 10, 100);
    String storeName = "store";
    String schemaStr = "int";
    Store store = mock(Store.class);
    when(store.isWriteComputationEnabled()).thenReturn(wcEnabled);
    when(store.isActiveActiveReplicationEnabled()).thenReturn(aaEnabled);
    when(storeRepository.getStoreOrThrow(storeName)).thenReturn(store);
    GeneratedSchemaID generatedSchemaID = schemaRepository.getDerivedSchemaId(storeName, schemaStr);
    assertNotNull(generatedSchemaID);
    assertEquals(generatedSchemaID, GeneratedSchemaID.INVALID);

    when(storeRepository.getStoreOrThrow(storeName)).thenThrow(new VeniceNoStoreException(storeName));
    assertThrows(VeniceNoStoreException.class, () -> schemaRepository.getDerivedSchemaId(storeName, schemaStr));
  }

  @Test(dataProviderClass = DataProviderUtils.class, dataProvider = "Two-True-and-False")
  public void getSupersetOrLatestValueSchemaTest(boolean wcEnabled, boolean aaEnabled) {
    ReadOnlyStoreRepository storeRepository = mock(ReadOnlyStoreRepository.class);
    ZkClient zkClient = mock(ZkClient.class);
    HelixSchemaAccessor accessor = mock(HelixSchemaAccessor.class);
    HelixReadOnlySchemaRepository schemaRepository =
        new HelixReadOnlySchemaRepository(storeRepository, zkClient, accessor, 10, 100);
    String storeName = "store";
    Store store = mock(Store.class);
    when(store.isWriteComputationEnabled()).thenReturn(wcEnabled);
    when(store.isActiveActiveReplicationEnabled()).thenReturn(aaEnabled);
    when(store.getLatestSuperSetValueSchemaId()).thenReturn(SchemaData.INVALID_VALUE_SCHEMA_ID);
    when(storeRepository.getStoreOrThrow(storeName)).thenReturn(store);

    SchemaEntry schemaEntryToReturn = new SchemaEntry(1, "\"int\"");
    List<SchemaEntry> listOfSchemaEntriesToReturn = new ArrayList<>();
    listOfSchemaEntriesToReturn.add(schemaEntryToReturn);
    when(accessor.getAllValueSchemas(storeName)).thenReturn(listOfSchemaEntriesToReturn);
    SchemaEntry schemaEntry = schemaRepository.getSupersetOrLatestValueSchema(storeName);
    assertNotNull(schemaEntry);
    assertEquals(schemaEntry, schemaEntryToReturn);
  }

  @Test
  public void testSchemaDeletion() {
    HelixSchemaAccessor accessor = mock(HelixSchemaAccessor.class);
    String storeName = "store";
    SchemaData schemaData = new SchemaData(storeName, null);
    Map<String, SchemaData> map = new HashMap<>();
    map.put(storeName, schemaData);
    List<String> list = new ArrayList<>();
    HelixReadOnlySchemaRepository.ValueSchemaChildListener listener =
        mock(HelixReadOnlySchemaRepository.ValueSchemaChildListener.class);
    doReturn(map).when(listener).getSchemaMap();
    doReturn(accessor).when(listener).getSchemaAccessor();
    SchemaEntry schemaEntry = new SchemaEntry(1, VALUE_SCHEMA.toString());
    doReturn(schemaEntry).when(accessor).getValueSchema(storeName, "1");
    list.add("1");
    doCallRealMethod().when(listener).handleSchemaChanges(storeName, list);
    listener.handleSchemaChanges(storeName, list);
    verify(accessor, times(1)).getValueSchema(anyString(), anyString());
  }

  private static String loadFileAsString(String fileName) {
    try {
      return IOUtils.toString(
          Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName)),
          StandardCharsets.UTF_8);
    } catch (Exception e) {
      LOGGER.error(e);
      return null;
    }
  }
}
