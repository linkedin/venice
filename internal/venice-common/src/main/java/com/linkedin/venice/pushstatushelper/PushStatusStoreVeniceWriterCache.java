package com.linkedin.venice.pushstatushelper;

import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Wrapper class for acquiring VeniceWriter for push status stores.
 */
public class PushStatusStoreVeniceWriterCache implements AutoCloseable {
  private static final Logger LOGGER = LogManager.getLogger(PushStatusStoreVeniceWriterCache.class);

  private final VeniceWriterFactory writerFactory;
  // Local cache of VeniceWriters.
  private final Map<String, VeniceWriter> veniceWriters = new VeniceConcurrentHashMap<>();

  // writerFactory Used for instantiating VeniceWriter
  public PushStatusStoreVeniceWriterCache(VeniceWriterFactory writerFactory) {
    this.writerFactory = writerFactory;
  }

  public VeniceWriter prepareVeniceWriter(String storeName) {
    return veniceWriters.computeIfAbsent(storeName, s -> {
      String rtTopic = Version.composeRealTimeTopic(VeniceSystemStoreUtils.getDaVinciPushStatusStoreName(storeName));
      Schema valueSchema = AvroProtocolDefinition.PUSH_STATUS_SYSTEM_SCHEMA_STORE.getCurrentProtocolVersionSchema();
      Schema writeComputeSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchema);
      VeniceWriterOptions options = new VeniceWriterOptions.Builder(rtTopic)
          .setKeySerializer(
              new VeniceAvroKafkaSerializer(
                  AvroProtocolDefinition.PUSH_STATUS_SYSTEM_SCHEMA_STORE_KEY.getCurrentProtocolVersionSchema()))
          .setValueSerializer(new VeniceAvroKafkaSerializer(valueSchema))
          .setWriteComputeSerializer(new VeniceAvroKafkaSerializer(writeComputeSchema))
          .setChunkingEnabled(false)
          .setPartitionCount(1)
          .build();

      return writerFactory.createVeniceWriter(options);
    });
  }

  public void removeVeniceWriter(String storeName) {
    VeniceWriter writer = veniceWriters.get(storeName);
    if (writer != null) {
      writer.close();
      veniceWriters.remove(storeName);
    }
  }

  public VeniceWriter getVeniceWriterFromMap(String storeName) {
    return veniceWriters.get(storeName);
  }

  @Override
  public void close() {
    veniceWriters.forEach((k, v) -> {
      try {
        v.close();
      } catch (Exception e) {
        LOGGER.error("Can not close VeniceWriter. ", e);
      }
    });
  }
}
