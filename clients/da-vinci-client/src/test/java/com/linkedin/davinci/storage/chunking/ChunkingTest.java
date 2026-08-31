package com.linkedin.davinci.storage.chunking;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.github.luben.zstd.Zstd;
import com.linkedin.davinci.compression.StorageEngineBackedCompressorFactory;
import com.linkedin.davinci.listener.response.NoOpReadResponseStats;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.davinci.store.record.ByteBufferValueRecord;
import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.serialization.AvroStoreDeserializerCache;
import com.linkedin.venice.serialization.RawBytesStoreDeserializerCache;
import com.linkedin.venice.serialization.StoreDeserializerCache;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.utils.ByteUtils;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class ChunkingTest {
  private static final int SCHEMA_ID = 1;
  private static final Schema SIMPLE_SCHEMA = new Schema.Parser()
      .parse("{\"type\":\"record\",\"name\":\"SimpleRecord\",\"fields\":[{\"name\":\"field\",\"type\":\"string\"}]}");

  @DataProvider(name = "recordProvider")
  public Object[][] recordProvider() {
    List<GenericRecord> params = new ArrayList<>();

    // First flavor:
    Schema schema1 = new Schema.Parser().parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"SomeRecordWithALongArray\",\n" + "  \"fields\": [\n"
            + "    {\n" + "      \"name\": \"test\",\n" + "      \"type\": {\n" + "        \"type\": \"array\",\n"
            + "        \"items\": \"long\"\n" + "      },\n" + "      \"default\": []\n" + "    }\n" + "  ]\n" + "}");

    for (int size = 10; size < 1000000; size = size * 2) {
      final int finalSize = size;
      GenericRecord record = new GenericData.Record(schema1) {
        @Override
        public String toString() {
          // Need to override the toString otherwise TestNG goes crazy with it :D ...
          return "SomeRecordWithALongArray of length " + finalSize;
        }
      };
      List<Long> list = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        list.add((long) (Math.random() * size));
      }
      record.put("test", list);
      params.add(record);
    }

    // Second flavor:
    Schema schema2 = new Schema.Parser().parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"SomeRecordWithALongArray\",\n" + "  \"fields\": [\n"
            + "    {\n" + "      \"name\": \"test\",\n" + "      \"type\": {\n" + "        \"type\": \"array\",\n"
            + "        \"items\": \"float\"\n" + "      },\n" + "      \"default\": []\n" + "    }\n" + "  ]\n" + "}");

    for (int size = 10; size < 1000000; size = size * 2) {
      final int finalSize = size;
      GenericRecord record = new GenericData.Record(schema2) {
        @Override
        public String toString() {
          // Need to override the toString otherwise TestNG goes crazy with it :D ...
          return "SomeRecordWithAFloatArray of length " + finalSize;
        }
      };
      List<Float> list = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        list.add((float) Math.random());
      }
      record.put("test", list);
      params.add(record);
    }

    // Third flavor:
    String schema3subRecordSchema = "{\n" + "          \"type\": \"record\",\n" + "          \"name\": \"SubRecord\",\n"
        + "          \"fields\": [\n" + "            {\n" + "              \"name\": \"someInt\",\n"
        + "              \"type\": \"int\"\n" + "            },\n" + "            {\n"
        + "              \"name\": \"someDouble\",\n" + "              \"type\": \"double\"\n" + "            },\n"
        + "            {\n" + "              \"name\": \"someBoolean\",\n" + "              \"type\": \"boolean\"\n"
        + "            }\n" + "          ]\n" + "        }";
    Schema schema3 = new Schema.Parser().parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"SomeRecordWithAnArrayOfSubRecords\",\n"
            + "  \"fields\": [\n" + "    {\n" + "      \"name\": \"test\",\n" + "      \"type\": {\n"
            + "        \"type\": \"array\",\n" + "        \"items\": " + schema3subRecordSchema + "\n" + "      },\n"
            + "      \"default\": []\n" + "    }\n" + "  ]\n" + "}");
    Schema schema3subRecord = new Schema.Parser().parse(schema3subRecordSchema);

    for (int size = 10; size < 1000000; size = size * 2) {
      final int finalSize = size;
      GenericRecord record = new GenericData.Record(schema3) {
        @Override
        public String toString() {
          // Need to override the toString otherwise TestNG goes crazy with it :D ...
          return "SomeRecordWithAnArrayOfSubRecords of length " + finalSize;
        }
      };
      List<GenericRecord> list = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        GenericRecord subRecord = new GenericData.Record(schema3subRecord);
        subRecord.put("someInt", ThreadLocalRandom.current().nextInt());
        subRecord.put("someDouble", ThreadLocalRandom.current().nextDouble());
        subRecord.put("someBoolean", (i & 1) == 0);
        list.add(subRecord);
      }

      record.put("test", list);
      params.add(record);
    }

    // Fourth flavor:
    Schema schema4 = new Schema.Parser().parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"SomeRecordWithAString\",\n" + "  \"fields\": [\n"
            + "    {\n" + "      \"name\": \"test\",\n" + "      \"type\": \"string\",\n" + "      \"default\": \"\"\n"
            + "    }\n" + "  ]\n" + "}");

    Random random = new Random();
    for (int size = 10; size < 1000000; size = size * 2) {
      final int finalSize = size;
      GenericRecord record = new GenericData.Record(schema4) {
        @Override
        public String toString() {
          // Need to override the toString otherwise TestNG goes crazy with it :D ...
          return "SomeRecordWithAString of length " + finalSize;
        }
      };
      List<Long> list = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        list.add((long) (Math.random() * size));
      }
      byte[] bytes = new byte[size];
      random.nextBytes(bytes);
      Utf8 expectedString = new Utf8(bytes);
      record.put("test", expectedString);
      params.add(record);
    }

    Object[][] paramsToReturn = new Object[params.size()][1];
    for (int i = 0; i < params.size(); i++) {
      paramsToReturn[i][0] = params.get(i);
    }
    return paramsToReturn;
  }

  private void runTest(
      GenericRecord record,
      AbstractAvroChunkingAdapter chunkingAdapter,
      boolean rawBytesStoreDeserializerCache,
      Function<Object, Void> assertions,
      boolean getWithSchemaId) {
    int partition = 9;
    String storeName = "test";
    byte[] keyBytes = ByteUtils.fromHexString("040647454ff4baf2630a5449544c45440010494d504c49434954");
    byte[] serializeNonChunkedKey = ByteUtils.fromHexString("040647454ff4baf2630a5449544c45440010494d504c494349540200");
    Schema schema = record.getSchema();

    byte[] serializedRecord = SerializerDeserializerFactory.getAvroGenericSerializer(schema).serialize(record);
    int cutOff = serializedRecord.length / 2;
    byte[] chunk1Bytes = new byte[cutOff + ValueRecord.SCHEMA_HEADER_LENGTH];
    byte[] chunk2Bytes = new byte[serializedRecord.length - cutOff + ValueRecord.SCHEMA_HEADER_LENGTH];
    ByteUtils.writeInt(chunk1Bytes, AvroProtocolDefinition.CHUNK.currentProtocolVersion.get(), 0);
    ByteUtils.writeInt(chunk2Bytes, AvroProtocolDefinition.CHUNK.currentProtocolVersion.get(), 0);
    System.arraycopy(serializedRecord, 0, chunk1Bytes, ValueRecord.SCHEMA_HEADER_LENGTH, cutOff);
    System.arraycopy(
        serializedRecord,
        cutOff,
        chunk2Bytes,
        ValueRecord.SCHEMA_HEADER_LENGTH,
        serializedRecord.length - cutOff);

    SchemaEntry schemaEntry = new SchemaEntry(SCHEMA_ID, schema);
    HelixReadOnlySchemaRepository schemaRepository = mock(HelixReadOnlySchemaRepository.class);
    doReturn(schemaEntry).when(schemaRepository).getValueSchema(storeName, SCHEMA_ID);
    doReturn(schemaEntry).when(schemaRepository).getSupersetOrLatestValueSchema(storeName);

    StorageEngine storageEngine = mock(StorageEngine.class);
    ByteBuffer firstKey = ByteBuffer.wrap(
        ByteUtils.fromHexString(
            "040647454FF4BAF2630A5449544C45440010494D504C494349540036EB0A5300374C6A9C5EEBB468C58E4300CE984E0001"));
    ByteBuffer secondKey = ByteBuffer.wrap(
        ByteUtils.fromHexString(
            "040647454FF4BAF2630A5449544C45440010494D504C494349540036EB0A5300374C6A9C5EEBB468C58E4300CE984E0201"));

    ChunkedValueManifest chunkedValueManifest = new ChunkedValueManifest();
    chunkedValueManifest.keysWithChunkIdSuffix = new ArrayList<>(2);
    chunkedValueManifest.keysWithChunkIdSuffix.add(firstKey);
    chunkedValueManifest.keysWithChunkIdSuffix.add(secondKey);
    chunkedValueManifest.schemaId = SCHEMA_ID;
    chunkedValueManifest.size = chunk1Bytes.length + chunk2Bytes.length
        - chunkedValueManifest.keysWithChunkIdSuffix.size() * ValueRecord.SCHEMA_HEADER_LENGTH;
    byte[] serializedCVM = SerializerDeserializerFactory.getAvroGenericSerializer(ChunkedValueManifest.SCHEMA$)
        .serialize(chunkedValueManifest);
    byte[] serializedCVMwithHeader = new byte[serializedCVM.length + ValueRecord.SCHEMA_HEADER_LENGTH];
    ByteUtils.writeInt(
        serializedCVMwithHeader,
        AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.currentProtocolVersion.get(),
        0);
    System.arraycopy(serializedCVM, 0, serializedCVMwithHeader, ValueRecord.SCHEMA_HEADER_LENGTH, serializedCVM.length);

    doReturn(serializedCVMwithHeader).when(storageEngine)
        .get(eq(partition), eq(ByteBuffer.wrap(serializeNonChunkedKey)));
    doReturn(chunk1Bytes).when(storageEngine).get(eq(partition), eq(firstKey));
    doReturn(chunk2Bytes).when(storageEngine).get(eq(partition), eq(secondKey));

    StoreDeserializerCache storeDeserializerCache = rawBytesStoreDeserializerCache
        ? RawBytesStoreDeserializerCache.getInstance()
        : new AvroStoreDeserializerCache(schemaRepository, storeName, true);
    int readerSchemaId = schemaEntry.getId();
    try (StorageEngineBackedCompressorFactory compressorFactory =
        new StorageEngineBackedCompressorFactory(mock(StorageMetadataService.class))) {
      VeniceCompressor compressor = compressorFactory.getCompressor(
          CompressionStrategy.NO_OP,
          storageEngine.getStoreVersionName(),
          Zstd.defaultCompressionLevel());
      Object retrievedObject;
      if (getWithSchemaId) {
        retrievedObject = chunkingAdapter.getWithSchemaId(
            storageEngine,
            partition,
            ByteBuffer.wrap(keyBytes),
            true,
            null,
            null,
            storeDeserializerCache,
            compressor,
            new ChunkedValueManifestContainer());
      } else {
        retrievedObject = chunkingAdapter.get(
            storageEngine,
            partition,
            ByteBuffer.wrap(keyBytes),
            true,
            null,
            null,
            NoOpReadResponseStats.SINGLETON,
            readerSchemaId,
            storeDeserializerCache,
            compressor,
            null);
      }
      assertions.apply(retrievedObject);
    }
  }

  @Test(dataProvider = "recordProvider")
  public void testGenericRecordChunkingAdapter(GenericRecord record) {
    runTest(record, GenericRecordChunkingAdapter.INSTANCE, false, (valueFromStorageEngine) -> {
      Assert.assertTrue(valueFromStorageEngine instanceof GenericRecord);
      Assert.assertEquals(valueFromStorageEngine, record);
      return null;
    }, false);
  }

  @Test(dataProvider = "recordProvider")
  public void testRawBytesChunkingAdapter(GenericRecord record) {
    byte[] serializedRecord =
        SerializerDeserializerFactory.getAvroGenericSerializer(record.getSchema()).serialize(record);
    runTest(record, RawBytesChunkingAdapter.INSTANCE, true, (valueFromStorageEngine) -> {
      Assert.assertTrue(valueFromStorageEngine instanceof ByteBuffer);
      Assert.assertEquals(ByteUtils.extractByteArray((ByteBuffer) valueFromStorageEngine), serializedRecord);
      return null;
    }, false);
  }

  @Test(dataProvider = "recordProvider")
  public void testRawBytesChunkingAdapterWithSchemaId(GenericRecord record) {
    byte[] serializedRecord =
        SerializerDeserializerFactory.getAvroGenericSerializer(record.getSchema()).serialize(record);
    runTest(record, RawBytesChunkingAdapter.INSTANCE, true, (valueFromStorageEngine) -> {
      Assert.assertTrue(valueFromStorageEngine instanceof ByteBufferValueRecord<?>);
      ByteBufferValueRecord byteBufferValueRecord = (ByteBufferValueRecord) valueFromStorageEngine;
      Object value = byteBufferValueRecord.value();
      Assert.assertTrue(value instanceof ByteBuffer);
      ByteBuffer bbValue = (ByteBuffer) value;
      Assert.assertEquals(ByteUtils.extractByteArray(bbValue), serializedRecord);
      Assert.assertEquals(byteBufferValueRecord.writerSchemaId(), SCHEMA_ID);
      return null;
    }, true);
  }

  /**
   * The A/A path reads the old value via {@code getWithSchemaId}. A vetoed read must come back with a {@code null}
   * value and, critically, without fetching any chunks — assembling a pathological multi-megabyte record just to
   * discover the write has to be rejected is exactly the ingestion cost nearline large-record skipping exists to
   * avoid. The wrapper itself is non-null, which is what {@code unwrapByteBufferFromOldValueProvider} relies on.
   */
  @Test
  public void testChunkedReadWithSchemaIdIsVetoedBeforeAssembly() {
    ChunkingFixture fixture = new ChunkingFixture();
    ChunkedValueManifestContainer container = new ChunkedValueManifestContainer(fixture.assembledSize - 1);

    ByteBufferValueRecord<?> retrieved = (ByteBufferValueRecord<?>) fixture.readWithSchemaId(container);

    Assert.assertNull(retrieved.value(), "An over-ceiling value must not be assembled");
    Assert.assertTrue(container.isSizeLimitExceeded(), "The caller must be able to tell this from 'not found'");
    Assert.assertEquals(container.getManifest().size, fixture.assembledSize);
    fixture.verifyNoChunksFetched();
  }

  /** The non-A/A path reads the old value via the plain {@code get}, which returns the value directly. */
  @Test
  public void testChunkedPlainReadIsVetoedBeforeAssembly() {
    ChunkingFixture fixture = new ChunkingFixture();
    ChunkedValueManifestContainer container = new ChunkedValueManifestContainer(fixture.assembledSize - 1);

    Object retrieved = fixture.readPlain(container);

    Assert.assertNull(retrieved, "An over-ceiling value must not be returned");
    Assert.assertTrue(container.isSizeLimitExceeded());
    fixture.verifyNoChunksFetched();
  }

  @Test
  public void testChunkedReadUnderCeilingStillAssembles() {
    ChunkingFixture fixture = new ChunkingFixture();
    ChunkedValueManifestContainer container = new ChunkedValueManifestContainer(fixture.assembledSize);

    ByteBufferValueRecord<?> retrieved = (ByteBufferValueRecord<?>) fixture.readWithSchemaId(container);

    Assert.assertNotNull(retrieved.value(), "A value exactly at the ceiling is within the limit");
    Assert.assertFalse(container.isSizeLimitExceeded());
    fixture.verifyChunksFetched();
  }

  @Test
  public void testChunkedReadWithoutCeilingIsUnaffected() {
    ChunkingFixture fixture = new ChunkingFixture();
    ChunkedValueManifestContainer container = new ChunkedValueManifestContainer();

    ByteBufferValueRecord<?> retrieved = (ByteBufferValueRecord<?>) fixture.readWithSchemaId(container);

    Assert.assertNotNull(retrieved.value(), "Read paths that declare no ceiling must behave exactly as before");
    Assert.assertFalse(container.isSizeLimitExceeded());
    fixture.verifyChunksFetched();
  }

  /** Minimal two-chunk value in a mocked storage engine, so chunk fetches can be asserted on. */
  private static final class ChunkingFixture {
    private static final int PARTITION = 9;
    private final StorageEngine storageEngine = mock(StorageEngine.class);
    private final byte[] keyBytes = ByteUtils.fromHexString("040647454ff4baf2630a5449544c45440010494d504c49434954");
    private final ByteBuffer firstKey;
    private final ByteBuffer secondKey;
    private final int assembledSize;

    private ChunkingFixture() {
      byte[] serializeNonChunkedKey =
          ByteUtils.fromHexString("040647454ff4baf2630a5449544c45440010494d504c494349540200");
      firstKey = ByteBuffer.wrap(
          ByteUtils.fromHexString(
              "040647454FF4BAF2630A5449544C45440010494D504C494349540036EB0A5300374C6A9C5EEBB468C58E4300CE984E0001"));
      secondKey = ByteBuffer.wrap(
          ByteUtils.fromHexString(
              "040647454FF4BAF2630A5449544C45440010494D504C494349540036EB0A5300374C6A9C5EEBB468C58E4300CE984E0201"));

      GenericRecord record = new GenericData.Record(SIMPLE_SCHEMA);
      record.put("field", new Utf8("a value long enough to split across two chunks"));
      byte[] serializedRecord = SerializerDeserializerFactory.getAvroGenericSerializer(SIMPLE_SCHEMA).serialize(record);
      int cutOff = serializedRecord.length / 2;
      byte[] chunk1Bytes = new byte[cutOff + ValueRecord.SCHEMA_HEADER_LENGTH];
      byte[] chunk2Bytes = new byte[serializedRecord.length - cutOff + ValueRecord.SCHEMA_HEADER_LENGTH];
      ByteUtils.writeInt(chunk1Bytes, AvroProtocolDefinition.CHUNK.currentProtocolVersion.get(), 0);
      ByteUtils.writeInt(chunk2Bytes, AvroProtocolDefinition.CHUNK.currentProtocolVersion.get(), 0);
      System.arraycopy(serializedRecord, 0, chunk1Bytes, ValueRecord.SCHEMA_HEADER_LENGTH, cutOff);
      System.arraycopy(
          serializedRecord,
          cutOff,
          chunk2Bytes,
          ValueRecord.SCHEMA_HEADER_LENGTH,
          serializedRecord.length - cutOff);

      ChunkedValueManifest manifest = new ChunkedValueManifest();
      manifest.keysWithChunkIdSuffix = new ArrayList<>(2);
      manifest.keysWithChunkIdSuffix.add(firstKey);
      manifest.keysWithChunkIdSuffix.add(secondKey);
      manifest.schemaId = SCHEMA_ID;
      manifest.size = chunk1Bytes.length + chunk2Bytes.length
          - manifest.keysWithChunkIdSuffix.size() * ValueRecord.SCHEMA_HEADER_LENGTH;
      assembledSize = manifest.size;

      byte[] serializedCVM =
          SerializerDeserializerFactory.getAvroGenericSerializer(ChunkedValueManifest.SCHEMA$).serialize(manifest);
      byte[] serializedCVMwithHeader = new byte[serializedCVM.length + ValueRecord.SCHEMA_HEADER_LENGTH];
      ByteUtils.writeInt(
          serializedCVMwithHeader,
          AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.currentProtocolVersion.get(),
          0);
      System
          .arraycopy(serializedCVM, 0, serializedCVMwithHeader, ValueRecord.SCHEMA_HEADER_LENGTH, serializedCVM.length);

      doReturn(serializedCVMwithHeader).when(storageEngine)
          .get(eq(PARTITION), eq(ByteBuffer.wrap(serializeNonChunkedKey)));
      doReturn(chunk1Bytes).when(storageEngine).get(eq(PARTITION), eq(firstKey));
      doReturn(chunk2Bytes).when(storageEngine).get(eq(PARTITION), eq(secondKey));
    }

    private Object readWithSchemaId(ChunkedValueManifestContainer container) {
      try (StorageEngineBackedCompressorFactory compressorFactory =
          new StorageEngineBackedCompressorFactory(mock(StorageMetadataService.class))) {
        VeniceCompressor compressor = compressorFactory.getCompressor(
            CompressionStrategy.NO_OP,
            storageEngine.getStoreVersionName(),
            Zstd.defaultCompressionLevel());
        return RawBytesChunkingAdapter.INSTANCE.getWithSchemaId(
            storageEngine,
            PARTITION,
            ByteBuffer.wrap(keyBytes),
            true,
            null,
            null,
            RawBytesStoreDeserializerCache.getInstance(),
            compressor,
            container);
      }
    }

    private Object readPlain(ChunkedValueManifestContainer container) {
      try (StorageEngineBackedCompressorFactory compressorFactory =
          new StorageEngineBackedCompressorFactory(mock(StorageMetadataService.class))) {
        VeniceCompressor compressor = compressorFactory.getCompressor(
            CompressionStrategy.NO_OP,
            storageEngine.getStoreVersionName(),
            Zstd.defaultCompressionLevel());
        return RawBytesChunkingAdapter.INSTANCE.get(
            storageEngine,
            PARTITION,
            ByteBuffer.wrap(keyBytes),
            true,
            null,
            null,
            NoOpReadResponseStats.SINGLETON,
            SCHEMA_ID,
            RawBytesStoreDeserializerCache.getInstance(),
            compressor,
            container);
      }
    }

    private void verifyNoChunksFetched() {
      verify(storageEngine, never()).get(eq(PARTITION), eq(firstKey));
      verify(storageEngine, never()).get(eq(PARTITION), eq(secondKey));
    }

    private void verifyChunksFetched() {
      verify(storageEngine).get(eq(PARTITION), eq(firstKey));
      verify(storageEngine).get(eq(PARTITION), eq(secondKey));
    }
  }
}
