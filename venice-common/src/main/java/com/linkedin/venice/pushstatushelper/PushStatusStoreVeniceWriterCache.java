package com.linkedin.venice.pushstatushelper;

import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushstatus.PushStatusKey;
import com.linkedin.venice.pushstatus.PushStatusValue;
import com.linkedin.venice.pushstatus.PushStatusValueWriteOpRecord;
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

  // writerFactory Used for instantiating VeniceWriter
  public PushStatusStoreVeniceWriterCache(VeniceWriterFactory writerFactory) {
    this.writerFactory = writerFactory;
  }

  public VeniceWriter prepareVeniceWriter(String storeName) {
    return veniceWriters.computeIfAbsent(storeName, s -> {
      String rtTopic = Version.composeRealTimeTopic(VeniceSystemStoreUtils.getDaVinciPushStatusStoreName(storeName));
      VeniceWriter writer = writerFactory.createVeniceWriter(rtTopic, new VeniceAvroKafkaSerializer(PushStatusKey.SCHEMA$.toString()),
          new VeniceAvroKafkaSerializer(PushStatusValue.SCHEMA$.toString()), new VeniceAvroKafkaSerializer(
              PushStatusValueWriteOpRecord.SCHEMA$.toString()), Optional
              .empty(), SystemTime.INSTANCE);
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
