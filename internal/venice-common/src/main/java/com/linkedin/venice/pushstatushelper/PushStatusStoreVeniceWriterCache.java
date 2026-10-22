package com.linkedin.venice.pushstatushelper;

import static com.linkedin.venice.common.VeniceSystemStoreUtils.CLUSTER_PUSH_STATUS_STORE_PARTITION_COUNT;
import static com.linkedin.venice.common.VeniceSystemStoreUtils.DEFAULT_USER_SYSTEM_STORE_PARTITION_COUNT;

import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.utils.Utils;
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
  private final Schema valueSchema;
  private final Schema updateSchema;

  // writerFactory Used for instantiating VeniceWriter
  public PushStatusStoreVeniceWriterCache(VeniceWriterFactory writerFactory, Schema valueSchema, Schema updateSchema) {
    this.writerFactory = writerFactory;
    this.valueSchema = valueSchema;
    this.updateSchema = updateSchema;
  }

  public VeniceWriter prepareVeniceWriter(String storeName) {
    return prepareWriter(
        storeName,
        VeniceSystemStoreUtils.getDaVinciPushStatusStoreName(storeName),
        DEFAULT_USER_SYSTEM_STORE_PARTITION_COUNT);
  }

  public VeniceWriter prepareClusterVeniceWriter(String clusterName) {
    String systemStoreName = VeniceSystemStoreUtils.getDaVinciPushStatusStoreNameForCluster(clusterName);
    return prepareWriter(systemStoreName, systemStoreName, CLUSTER_PUSH_STATUS_STORE_PARTITION_COUNT);
  }

  private VeniceWriter prepareWriter(String cacheKey, String systemStoreName, int partitionCount) {
    return veniceWriters.computeIfAbsent(cacheKey, s -> {
      String rtTopic = Utils.composeRealTimeTopic(systemStoreName);
      VeniceWriterOptions options = new VeniceWriterOptions.Builder(rtTopic)
          .setKeyPayloadSerializer(
              new VeniceAvroKafkaSerializer(
                  AvroProtocolDefinition.PUSH_STATUS_SYSTEM_SCHEMA_STORE_KEY.getCurrentProtocolVersionSchema()))
          .setValuePayloadSerializer(new VeniceAvroKafkaSerializer(valueSchema))
          .setWriteComputePayloadSerializer(new VeniceAvroKafkaSerializer(updateSchema))
          .setChunkingEnabled(false)
          .setPartitionCount(partitionCount)
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
