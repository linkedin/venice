package com.linkedin.davinci.utils;

import com.linkedin.davinci.serializer.avro.MapOrderPreservingSerDeFactory;
import com.linkedin.davinci.serializer.avro.fast.MapOrderPreservingFastSerDeFactory;
import com.linkedin.venice.client.schema.StoreSchemaFetcher;
import com.linkedin.venice.schema.rmd.v1.RmdSchemaGeneratorV1;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.collections.BiIntKeyCache;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


/**
 * Similar to {@link com.linkedin.davinci.replication.merge.RmdSerDe} except it's intended to be used as a utility on
 * client side to serialize/deserialize RMD record. The main difference is that it doesn't need to handle RMD protocol
 * evolution and uses the only and latest RMD protocol {@link com.linkedin.venice.schema.rmd.v1.RmdSchemaGeneratorV1}
 * to generate the corresponding RMD schema used for serialization and deserialization. In the future we can support
 * RMD protocol evolution by extending {@link StoreSchemaFetcher} to fetch the RMD protocol since we don't want clients
 * to use the controller client as part of a utility for fetching the RMD protocol.
 */
public class ClientRmdSerDe {
  private static final RmdSchemaGeneratorV1 GENERATOR_V1 = new RmdSchemaGeneratorV1();
  private final StoreSchemaFetcher schemaFetcher;
  private final VeniceConcurrentHashMap<Integer, Schema> rmdSchemaIndexedByValueSchemaId;
  private final VeniceConcurrentHashMap<Integer, RecordSerializer<GenericRecord>> rmdSerializerIndexedByValueSchemaId;
  private final boolean fastAvroEnabled;
  private BiIntKeyCache<RecordDeserializer<GenericRecord>> deserializerCache;

  public ClientRmdSerDe(StoreSchemaFetcher schemaFetcher) {
    this(schemaFetcher, true);
  }

  public ClientRmdSerDe(StoreSchemaFetcher schemaFetcher, boolean fastAvroEnabled) {
    this.schemaFetcher = schemaFetcher;
    this.fastAvroEnabled = fastAvroEnabled;
    this.rmdSchemaIndexedByValueSchemaId = new VeniceConcurrentHashMap<>();
    this.rmdSerializerIndexedByValueSchemaId = new VeniceConcurrentHashMap<>();
    this.deserializerCache = new BiIntKeyCache<>((writerSchemaId, readerSchemaId) -> {
      Schema rmdWriterSchema = generateRmdSchema(writerSchemaId);
      Schema rmdReaderSchema = generateRmdSchema(readerSchemaId);
      return this.fastAvroEnabled
          ? MapOrderPreservingFastSerDeFactory.getDeserializer(rmdWriterSchema, rmdReaderSchema)
          : MapOrderPreservingSerDeFactory.getDeserializer(rmdWriterSchema, rmdReaderSchema);
    });
  }

  public Schema getRmdSchema(final int valueSchemaId) {
    return this.rmdSchemaIndexedByValueSchemaId.computeIfAbsent(valueSchemaId, this::generateRmdSchema);
  }

  public GenericRecord deserializeRmdBytes(final int writerSchemaId, final int readerSchemaId, ByteBuffer rmdBytes) {
    return this.deserializerCache.get(writerSchemaId, readerSchemaId).deserialize(rmdBytes);
  }

  public ByteBuffer serializeRmdRecord(final int valueSchemaId, GenericRecord rmdRecord) {
    RecordSerializer<GenericRecord> serializer =
        this.rmdSerializerIndexedByValueSchemaId.computeIfAbsent(valueSchemaId, this::generateRmdSerializer);
    byte[] rmdBytes = serializer.serialize(rmdRecord);
    return ByteBuffer.wrap(rmdBytes);
  }

  private Schema generateRmdSchema(final int valueSchemaId) {
    Schema valueSchema = schemaFetcher.getValueSchema(valueSchemaId);
    return GENERATOR_V1.generateMetadataSchema(valueSchema);
  }

  private RecordSerializer<GenericRecord> generateRmdSerializer(int valueSchemaId) {
    Schema replicationMetadataSchema = getRmdSchema(valueSchemaId);
    return fastAvroEnabled
        ? MapOrderPreservingFastSerDeFactory.getSerializer(replicationMetadataSchema)
        : MapOrderPreservingSerDeFactory.getSerializer(replicationMetadataSchema);
  }
}
