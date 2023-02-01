package com.linkedin.venice.helix;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.alpini.io.IOUtils;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.writer.update.UpdateBuilderImplTest;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;


public class HelixReadOnlySchemaRepositoryTest {
  private static final Logger LOGGER = LogManager.getLogger(UpdateBuilderImplTest.class);

  private static final Schema VALUE_SCHEMA =
      AvroCompatibilityHelper.parse(loadFileAsString("TestWriteComputeBuilder.avsc"));
  private static final Schema UPDATE_SCHEMA =
      WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(VALUE_SCHEMA);

  @Test
  public void testForceRefreshSchemaData() {
    HelixReadOnlySchemaRepository schemaRepository = mock(HelixReadOnlySchemaRepository.class);
    doCallRealMethod().when(schemaRepository).forceRefreshSchemaData(any(), any());
    HelixSchemaAccessor schemaAccessor = mock(HelixSchemaAccessor.class);
    when(schemaRepository.getAccessor()).thenReturn(schemaAccessor);
    Store store = mock(Store.class);
    String storeName = "testStore";
    when(store.getName()).thenReturn(storeName);
    when(store.isWriteComputationEnabled()).thenReturn(false);
    when(store.isActiveActiveReplicationEnabled()).thenReturn(false);
    SchemaData schemaData = new SchemaData(storeName);

    SchemaEntry schemaEntry = new SchemaEntry(1, VALUE_SCHEMA.toString());
    DerivedSchemaEntry derivedSchemaEntry = new DerivedSchemaEntry(1, 1, UPDATE_SCHEMA.toString());
    RmdSchemaEntry rmdSchemaEntry = new RmdSchemaEntry(1, 1, UPDATE_SCHEMA.toString());
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
    when(storeRepository.hasStore(storeName)).thenReturn(true);
    when(schemaRepository.getStoreRepository()).thenReturn(storeRepository);

    Map<String, SchemaData> schemaDataMap = new VeniceConcurrentHashMap<>();
    schemaDataMap.put(storeName, new SchemaData(storeName));
    ReadWriteLock lock = new ReentrantReadWriteLock();
    when(schemaRepository.getSchemaLock()).thenReturn(lock);
    when(schemaRepository.getSupersetSchema(storeName)).thenCallRealMethod();
    when(schemaRepository.getSchemaMap()).thenReturn(schemaDataMap);
    when(store.getLatestSuperSetValueSchemaId()).thenReturn(1);
    LOGGER.info(schemaRepository.getSupersetSchema(storeName));

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
