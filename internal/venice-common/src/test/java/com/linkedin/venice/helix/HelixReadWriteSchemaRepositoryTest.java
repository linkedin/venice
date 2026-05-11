package com.linkedin.venice.helix;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertThrows;

import com.linkedin.venice.exceptions.SchemaIncompatibilityException;
import com.linkedin.venice.meta.ReadOnlyStore;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.ValueSchemaCreatedListener;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
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

  @Test
  public void testValueSchemaCreatedListenerFiresForNewSchema() {
    String storeName = "testStore";
    HelixReadWriteSchemaRepository repo = newRepoWithStore(storeName, Collections.emptyList());

    ValueSchemaCreatedListener listener = mock(ValueSchemaCreatedListener.class);
    repo.registerValueSchemaCreatedListener(listener);

    repo.addValueSchema(storeName, "\"string\"", 1);

    verify(listener, times(1)).handleValueSchemaCreated(any(Store.class), any(SchemaEntry.class));
  }

  @Test
  public void testValueSchemaCreatedListenerSkippedForDuplicateSchema() {
    String storeName = "testStore";
    SchemaEntry existing = new SchemaEntry(1, "\"string\"");
    HelixReadWriteSchemaRepository repo = newRepoWithStore(storeName, Arrays.asList(existing));

    ValueSchemaCreatedListener listener = mock(ValueSchemaCreatedListener.class);
    repo.registerValueSchemaCreatedListener(listener);

    // DUPLICATE_VALUE_SCHEMA_CODE → repo computes next id, finds duplicate, returns DUPLICATE_VALUE_SCHEMA_CODE
    repo.addValueSchema(storeName, "\"string\"", SchemaData.DUPLICATE_VALUE_SCHEMA_CODE);

    verify(listener, never()).handleValueSchemaCreated(any(), any());
  }

  @Test
  public void testValueSchemaCreatedListenerFiresForDocFieldUpdate() {
    String storeName = "testStore";
    String original =
        "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"f\",\"type\":\"string\"," + "\"doc\":\"old\"}]}";
    String docOnlyChange =
        "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"f\",\"type\":\"string\"," + "\"doc\":\"new\"}]}";
    SchemaEntry existing = new SchemaEntry(1, original);
    HelixReadWriteSchemaRepository repo = newRepoWithStore(storeName, Arrays.asList(existing));

    ValueSchemaCreatedListener listener = mock(ValueSchemaCreatedListener.class);
    repo.registerValueSchemaCreatedListener(listener);

    repo.addValueSchema(storeName, docOnlyChange, SchemaData.DUPLICATE_VALUE_SCHEMA_CODE);

    verify(listener, times(1)).handleValueSchemaCreated(any(Store.class), any(SchemaEntry.class));
  }

  @Test
  public void testListenerExceptionDoesNotPreventOtherListeners() {
    String storeName = "testStore";
    HelixReadWriteSchemaRepository repo = newRepoWithStore(storeName, Collections.emptyList());

    ValueSchemaCreatedListener throwing = mock(ValueSchemaCreatedListener.class);
    doThrow(new RuntimeException("boom")).when(throwing).handleValueSchemaCreated(any(), any());
    ValueSchemaCreatedListener second = mock(ValueSchemaCreatedListener.class);

    repo.registerValueSchemaCreatedListener(throwing);
    repo.registerValueSchemaCreatedListener(second);

    repo.addValueSchema(storeName, "\"string\"", 1);

    verify(throwing, times(1)).handleValueSchemaCreated(any(Store.class), any(SchemaEntry.class));
    verify(second, times(1)).handleValueSchemaCreated(any(Store.class), any(SchemaEntry.class));
  }

  @Test
  public void testUnregisterValueSchemaCreatedListener() {
    String storeName = "testStore";
    HelixReadWriteSchemaRepository repo = newRepoWithStore(storeName, Collections.emptyList());

    ValueSchemaCreatedListener listener = mock(ValueSchemaCreatedListener.class);
    repo.registerValueSchemaCreatedListener(listener);
    repo.unregisterValueSchemaCreatedListener(listener);

    repo.addValueSchema(storeName, "\"string\"", 1);

    verify(listener, never()).handleValueSchemaCreated(any(), any());
  }

  @Test
  public void testListenerReceivesReadOnlyStoreSnapshot() {
    String storeName = "testStore";
    HelixReadWriteSchemaRepository repo = newRepoWithStore(storeName, Collections.emptyList());

    ValueSchemaCreatedListener listener = mock(ValueSchemaCreatedListener.class);
    repo.registerValueSchemaCreatedListener(listener);

    repo.addValueSchema(storeName, "\"string\"", 1);

    ArgumentCaptor<Store> storeCaptor = ArgumentCaptor.forClass(Store.class);
    verify(listener).handleValueSchemaCreated(storeCaptor.capture(), eq(new SchemaEntry(1, "\"string\"")));
    Assert.assertTrue(
        storeCaptor.getValue() instanceof ReadOnlyStore,
        "Listener must receive a ReadOnlyStore wrapper, got: " + storeCaptor.getValue().getClass().getName());
  }

  /**
   * Build a HelixReadWriteSchemaRepository wired with mocked store + accessor.
   * The accessor's getAllValueSchemas() returns a fresh copy of {@code initialSchemas} on each call,
   * mirroring zookeeper-backed reads — that allows tests to verify post-add state without polluting
   * cross-test mock state.
   */
  private HelixReadWriteSchemaRepository newRepoWithStore(String storeName, Collection<SchemaEntry> initialSchemas) {
    ReadWriteStoreRepository mockStoreRepo = mock(ReadWriteStoreRepository.class);
    Store mockStore = mock(Store.class);
    doReturn(storeName).when(mockStore).getName();
    doReturn(false).when(mockStore).isEnumSchemaEvolutionAllowed();
    doReturn(false).when(mockStore).isStoreMetaSystemStoreEnabled();
    doReturn(mockStore).when(mockStoreRepo).getStoreOrThrow(storeName);
    doReturn(mockStore).when(mockStoreRepo).getStore(storeName);
    doReturn(true).when(mockStoreRepo).hasStore(storeName);

    HelixSchemaAccessor mockAccessor = mock(HelixSchemaAccessor.class);
    doAnswer(inv -> new ArrayList<>(initialSchemas)).when(mockAccessor).getAllValueSchemas(storeName);

    return new HelixReadWriteSchemaRepository(mockStoreRepo, Optional.empty(), mockAccessor);
  }
}
