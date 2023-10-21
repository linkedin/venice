package com.linkedin.venice.meta;

import com.linkedin.venice.helix.HelixReadWriteSchemaRepository;
import com.linkedin.venice.helix.HelixSchemaAccessor;
import com.linkedin.venice.schema.GeneratedSchemaID;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.system.store.MetaStoreWriter;
import java.util.Arrays;
import java.util.Optional;
import org.apache.avro.Schema;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestHelixReadWriteSchemaRepository {
  private HelixReadWriteSchemaRepository helixReadWriteSchemaRepository;
  private final HelixSchemaAccessor accessor = Mockito.mock(HelixSchemaAccessor.class);
  private final ReadWriteStoreRepository storeRepository = Mockito.mock(ReadWriteStoreRepository.class);
  private final MetaStoreWriter metaStoreWriter = Mockito.mock(MetaStoreWriter.class);

  @BeforeMethod
  void setUp() {
    this.helixReadWriteSchemaRepository =
        new HelixReadWriteSchemaRepository(storeRepository, Optional.of(metaStoreWriter), accessor);
  }

  @Test
  public void getDerivedSchemaCanonical() {
    String storeName = "test";
    String schemaStr =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"company\",\"type\":\"string\"}]}";
    String schemaStr1 =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"company1\",\"type\":\"string\"}]}";
    Schema schema = Schema.parse(schemaStr);
    DerivedSchemaEntry entry = new DerivedSchemaEntry(1, 1, schema);
    Mockito.doReturn(true).when(storeRepository).hasStore(Mockito.anyString());
    Mockito.doReturn(Arrays.asList(entry)).when(accessor).getAllDerivedSchemas(Mockito.eq(storeName));
    GeneratedSchemaID derivedSchemaId = helixReadWriteSchemaRepository.getDerivedSchemaId(storeName, schemaStr);
    Assert.assertEquals(derivedSchemaId.getGeneratedSchemaVersion(), 1);
    derivedSchemaId = helixReadWriteSchemaRepository.getDerivedSchemaId(storeName, schemaStr);
    Assert.assertEquals(derivedSchemaId.getGeneratedSchemaVersion(), 1);
    derivedSchemaId = helixReadWriteSchemaRepository.getDerivedSchemaId(storeName, schemaStr1);
    Assert.assertEquals(derivedSchemaId.getGeneratedSchemaVersion(), -1);
  }
}
