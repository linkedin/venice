package com.linkedin.davinci.utils;

import com.linkedin.davinci.listener.response.NoOpReadResponseStats;
import com.linkedin.davinci.storage.chunking.RawBytesChunkingAdapter;
import com.linkedin.davinci.store.memory.InMemoryStorageEngine;
import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.serialization.RawBytesStoreDeserializerCache;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.utils.lazy.Lazy;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/*
 * This class serves as a utility to deserialize and assemble chunks consumed from a Kafka topic
 */
public class ChunkAssembler {
  private static final Logger LOGGER = LogManager.getLogger(ChunkAssembler.class);

  protected final String storeName;

  // This storage engine serves as a buffer for records which are chunked and have to be buffered before they can
  // be returned to the client. We leverage the storageEngine interface here in order to take better advantage
  // of the chunking and decompressing adapters that we've already built (which today are built around this interface)
  // as chunked records are assembled we will eagerly evict all keys from the storage engine in order to keep the memory
  // footprint as small as we can. We could use the object cache storage engine here in order to get LRU behavior
  // but then that runs the risk of a parallel subscription having record chunks getting evicted before we have a chance
  // to assemble them. So we rely on the simpler and concrete implementation as opposed to the abstraction in order
  // to control and guarantee the behavior we're expecting.
  protected final InMemoryStorageEngine inMemoryStorageEngine;

  public ChunkAssembler(String storeName) {
    this.storeName = storeName;

    // The in memory storage engine only relies on the name of store and nothing else. We use an unversioned store name
    // here in order to reduce confusion (as this storage engine can be used across version topics).
    this.inMemoryStorageEngine = new InMemoryStorageEngine(storeName);
    // disable noisy logs
    this.inMemoryStorageEngine.suppressLogs(true);
  }

  public <T> T bufferAndAssembleRecord(
      PubSubTopicPartition pubSubTopicPartition,
      int schemaId,
      byte[] keyBytes,
      ByteBuffer valueBytes,
      long recordOffset,
      Lazy<RecordDeserializer<T>> recordDeserializer,
      int readerSchemaId,
      VeniceCompressor compressor) {
    T assembledRecord = null;

    if (!inMemoryStorageEngine.containsPartition(pubSubTopicPartition.getPartitionNumber())) {
      inMemoryStorageEngine.addStoragePartition(pubSubTopicPartition.getPartitionNumber());
    }
    // If this is a record chunk, store the chunk and return null for processing this record
    if (schemaId == AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion()) {
      inMemoryStorageEngine.put(
          pubSubTopicPartition.getPartitionNumber(),
          keyBytes,
          ValueRecord.create(schemaId, valueBytes.array()).serialize());
      return null;
    } else if (schemaId == AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion()) {
      // This is the last value. Store it, and now read it from the in memory store as a fully assembled value
      inMemoryStorageEngine.put(
          pubSubTopicPartition.getPartitionNumber(),
          keyBytes,
          ValueRecord.create(schemaId, valueBytes.array()).serialize());
      try {
        assembledRecord = decompressAndDeserialize(
            recordDeserializer.get(),
            compressor,
            RawBytesChunkingAdapter.INSTANCE.get(
                inMemoryStorageEngine,
                pubSubTopicPartition.getPartitionNumber(),
                ByteBuffer.wrap(keyBytes),
                false,
                null,
                null,
                NoOpReadResponseStats.SINGLETON,
                readerSchemaId,
                RawBytesStoreDeserializerCache.getInstance(),
                compressor,
                null));
      } catch (Exception ex) {
        // We might get an exception if we haven't persisted all the chunks for a given key. This
        // can actually happen if the client seeks to the middle of a chunked record either by
        // only tailing the records or through direct offset management. This is ok, we just won't
        // return this record since this is a course grained approach we can drop it.
        LOGGER.warn(
            "Encountered error assembling chunked record, this can happen when seeking between chunked records. Skipping offset {} on topic {}",
            recordOffset,
            pubSubTopicPartition.getPubSubTopic().getName());
      }
    } else {
      // this is a fully specified record, no need to buffer and assemble it, just decompress and deserialize it
      try {
        assembledRecord = decompressAndDeserialize(recordDeserializer.get(), compressor, valueBytes);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    // We only buffer one record at a time for a given partition. If we've made it this far
    // we either just finished assembling a large record, or, didn't specify anything. So we'll clear
    // the cache. Kafka might give duplicate delivery, but it won't give out of order delivery, so
    // this is safe to do in all such contexts.
    inMemoryStorageEngine.dropPartition(pubSubTopicPartition.getPartitionNumber());
    return assembledRecord;
  }

  protected <T> T decompressAndDeserialize(
      RecordDeserializer<T> deserializer,
      VeniceCompressor compressor,
      ByteBuffer value) throws IOException {
    return deserializer.deserialize(compressor.decompress(value));
  }

  public void clearInMemoryDB() {
    inMemoryStorageEngine.drop();
  }
}
