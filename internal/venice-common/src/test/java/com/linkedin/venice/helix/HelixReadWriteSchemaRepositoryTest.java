package com.linkedin.venice.helix;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertThrows;

import com.linkedin.venice.exceptions.SchemaIncompatibilityException;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.schema.SchemaEntry;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import org.testng.annotations.Test;


public class HelixReadWriteSchemaRepositoryTest {
  @Test
  public void testEnumSchemaEvolution() {
    String valueSchemaWithEnumDefaultDefined = "{\n" + "  \"name\": \"EnumTestRecord\",\n"
        + "  \"namespace\": \"com.linkedin.avro.fastserde.generated.avro\",\n" + "  \"type\": \"record\",\n"
        + "  \"fields\": [\n" + "    {\n" + "      \"name\": \"testEnum\",\n" + "      \"type\": {\n"
        + "        \"type\": \"enum\",\n" + "        \"name\": \"TestEnum\",\n"
        + "        \"symbols\": [\"A\", \"B\", \"C\"],\n" + "        \"default\": \"A\"\n" + "      }\n" + "    }\n"
        + "  ]\n" + "}";
    String valueSchemaWithEnumEvolved = "{\n" + "  \"name\": \"EnumTestRecord\",\n"
        + "  \"namespace\": \"com.linkedin.avro.fastserde.generated.avro\",\n" + "  \"type\": \"record\",\n"
        + "  \"fields\": [\n" + "    {\n" + "      \"name\": \"testEnum\",\n" + "      \"type\": {\n"
        + "        \"type\": \"enum\",\n" + "        \"name\": \"TestEnum\",\n"
        + "        \"symbols\": [\"A\", \"B\", \"C\", \"D\", \"E\"],\n" + "        \"default\": \"A\"\n" + "      }\n"
        + "    }\n" + "  ]\n" + "}";

    String storeName = "testStore";
    ReadWriteStoreRepository mockStoreRepository = mock(ReadWriteStoreRepository.class);
    Store mockStore = mock(Store.class);
    doReturn(false).when(mockStore).isEnumSchemaEvolutionAllowed();
    doReturn(mockStore).when(mockStoreRepository).getStoreOrThrow(storeName);
    doReturn(true).when(mockStoreRepository).hasStore(storeName);
    HelixSchemaAccessor mockSchemaAccessor = mock(HelixSchemaAccessor.class);
    SchemaEntry entry1 = new SchemaEntry(1, valueSchemaWithEnumDefaultDefined);
    Collection<SchemaEntry> schemaEntries = Arrays.asList(entry1);
    doReturn(schemaEntries).when(mockSchemaAccessor).getAllValueSchemas(storeName);

    HelixReadWriteSchemaRepository readWriteSchemaRepository =
        new HelixReadWriteSchemaRepository(mockStoreRepository, Optional.empty(), mockSchemaAccessor);
    // Enum evolution is not allowed
    assertThrows(
        SchemaIncompatibilityException.class,
        () -> readWriteSchemaRepository.addValueSchema(storeName, valueSchemaWithEnumEvolved));

    // Allow enum schema evolution
    doReturn(true).when(mockStore).isEnumSchemaEvolutionAllowed();
    // No exception
    readWriteSchemaRepository.addValueSchema(storeName, valueSchemaWithEnumEvolved);
  }
}
