package com.linkedin.venice.pushstatushelper;

import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.util.Map;
import java.util.Optional;
import org.apache.log4j.Logger;


/**
 * Wrapper class for acquiring VeniceWriter for push status stores.
 */
public class PushStatusStoreVeniceWriterCache implements AutoCloseable {
  private static final Logger logger = Logger.getLogger(PushStatusStoreVeniceWriterCache.class);

  private final VeniceWriterFactory writerFactory;
  // Local cache of VeniceWriters.
  private final Map<String, VeniceWriter> veniceWriters = new VeniceConcurrentHashMap<>();
  // avro-schemas does not support union type, using a static schemaStr to construct VeniceWriter
  private final static String WRITE_COMPUTE_SCHEMA = "[{\"type\":\"record\",\"name\":\"PushStatusValueWriteOpRecord\",\"namespace\":\"com.linkedin.venice.pushstatus\",\"fields\":[{\"name\":\"instances\",\"type\":[{\"type\":\"record\",\"name\":\"NoOp\",\"fields\":[]},{\"type\":\"record\",\"name\":\"instancesMapOps\",\"fields\":[{\"name\":\"mapUnion\",\"type\":{\"type\":\"map\",\"values\":\"int\"},\"default\":{}},{\"name\":\"mapDiff\",\"type\":{\"type\":\"array\",\"items\":\"string\"},\"default\":[]}]},{\"type\":\"map\",\"values\":\"int\"}],\"default\":{}},{\"name\":\"reportTimestamp\",\"type\":[\"NoOp\",\"null\",\"long\"],\"doc\":\"heartbeat.\",\"default\":{}}]},{\"type\":\"record\",\"name\":\"DelOp\",\"namespace\":\"com.linkedin.venice.pushstatus\",\"fields\":[]}]";

  // writerFactory Used for instantiating VeniceWriter
  public PushStatusStoreVeniceWriterCache(VeniceWriterFactory writerFactory) {
    this.writerFactory = writerFactory;
  }

  public VeniceWriter prepareVeniceWriter(String storeName) {
    return veniceWriters.computeIfAbsent(storeName, s -> {
      String rtTopic = Version.composeRealTimeTopic(VeniceSystemStoreUtils.getDaVinciPushStatusStoreName(storeName));
      VeniceWriter writer = writerFactory.createVeniceWriter(rtTopic,
          new VeniceAvroKafkaSerializer(AvroProtocolDefinition.PUSH_STATUS_SYSTEM_SCHEMA_STORE_KEY.getCurrentProtocolVersionSchema()),
          new VeniceAvroKafkaSerializer(AvroProtocolDefinition.PUSH_STATUS_SYSTEM_SCHEMA_STORE.getCurrentProtocolVersionSchema()),
          new VeniceAvroKafkaSerializer(WRITE_COMPUTE_SCHEMA),
          Optional.empty(),
          SystemTime.INSTANCE);
      return writer;
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
        logger.error("Can not close VeniceWriter. ", e);
      }
    });
  }
}
