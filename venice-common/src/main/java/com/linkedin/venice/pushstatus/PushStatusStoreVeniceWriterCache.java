package com.linkedin.venice.pushstatus;

import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.schema.WriteComputeSchemaAdapter;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.log4j.Logger;


/**
 * Wrapper class for acquiring VeniceWriter for push status stores.
 */
public class PushStatusStoreVeniceWriterCache implements AutoCloseable {
  private static final Logger logger = Logger.getLogger(PushStatusStoreVeniceWriterCache.class);
  public static final Schema
      WRITE_COMPUTE_SCHEMA = WriteComputeSchemaAdapter.parse(PushStatusValue.SCHEMA$.toString()).getTypes().get(0);

  private final VeniceWriterFactory writerFactory;
  // Local cache of VeniceWriters.
  private final Map<String, VeniceWriter> veniceWriters = new VeniceConcurrentHashMap<>();

  // writerFactory Used for instantiating VeniceWriter
  public PushStatusStoreVeniceWriterCache(VeniceWriterFactory writerFactory) {
    this.writerFactory = writerFactory;
  }

  public VeniceWriter getVeniceWriter(String storeName) {
    return veniceWriters.computeIfAbsent(storeName, s -> {
      String rtTopic = Version.composeRealTimeTopic(VeniceSystemStoreUtils.getDaVinciPushStatusStoreName(storeName));
      VeniceWriter writer = writerFactory.createVeniceWriter(rtTopic, new VeniceAvroKafkaSerializer(PushStatusKey.SCHEMA$.toString()),
          new VeniceAvroKafkaSerializer(PushStatusValue.SCHEMA$.toString()), new VeniceAvroKafkaSerializer(WRITE_COMPUTE_SCHEMA.toString()), Optional
              .empty(), SystemTime.INSTANCE);
      return writer;
    });
  }

  @Override
  public void close() {
    veniceWriters.forEach((k, v) -> {
      try {
        v.close();
      } catch (Exception e) {
        logger.error("Can not close VeniceWriter. ", e);
      }
    });
  }
}
