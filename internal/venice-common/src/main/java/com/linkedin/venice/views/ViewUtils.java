package com.linkedin.venice.views;

import static com.linkedin.venice.pubsub.api.PubSubMessageHeaders.VENICE_VIEW_PARTITIONS_MAP_HEADER;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ViewConfig;
import com.linkedin.venice.pubsub.api.PubSubMessageHeader;
import com.linkedin.venice.pubsub.api.PubSubMessageHeaders;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.ReflectUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;


public class ViewUtils {
  public static final String PARTITION_COUNT = "sub.partition.count";
  public static final String USE_FAST_KAFKA_OPERATION_TIMEOUT = "use.fast.kafka.operation.timeout";

  public static final String LOG_COMPACTION_ENABLED = "log.compaction.enabled";

  public static final String ETERNAL_TOPIC_RETENTION_ENABLED = "eternal.topic.retention.enabled";

  public static final String NEARLINE_PRODUCER_COMPRESSION_ENABLED = "nearline.producer.compression.enabled";

  public static final String NEARLINE_PRODUCER_COUNT_PER_WRITER = "nearline.producer.count.per.writer";

  public static VeniceView getVeniceView(
      String viewClass,
      Properties params,
      String veniceStoreName,
      Map<String, String> extraParameters) {
    VeniceView view = ReflectUtils.callConstructor(
        ReflectUtils.loadClass(viewClass),
        new Class<?>[] { Properties.class, String.class, Map.class },
        new Object[] { params, veniceStoreName, extraParameters });
    return view;
  }

  public static String flatViewConfigMapString(Map<String, ViewConfig> viewConfigMap) throws JsonProcessingException {
    ObjectMapper mapper = ObjectMapperFactory.getInstance();
    Map<String, String> flatMap = new HashMap<>();
    for (Map.Entry<String, ViewConfig> mapEntry: viewConfigMap.entrySet()) {
      flatMap.put(mapEntry.getKey(), mapper.writeValueAsString(mapEntry.getValue()));
    }
    return mapper.writeValueAsString(flatMap);
  }

  public static Map<String, ViewConfig> parseViewConfigMapString(String flatViewConfigMapString)
      throws JsonProcessingException {
    ObjectMapper mapper = ObjectMapperFactory.getInstance();
    Map<String, String> flatMap = mapper.readValue(flatViewConfigMapString, Map.class);
    Map<String, ViewConfig> viewConfigMap = new HashMap<>();
    for (Map.Entry<String, String> entry: flatMap.entrySet()) {
      viewConfigMap.put(entry.getKey(), mapper.readValue(entry.getValue(), ViewConfig.class));
    }
    return viewConfigMap;
  }

  public static Map<String, VeniceProperties> getViewTopicsAndConfigs(
      Collection<ViewConfig> viewConfigs,
      Properties veniceViewProperties,
      String storeName,
      int version) {
    Map<String, VeniceProperties> viewTopicNamesAndConfigs = new HashMap<>();
    for (ViewConfig rawView: viewConfigs) {
      VeniceView veniceView =
          getVeniceView(rawView.getViewClassName(), veniceViewProperties, storeName, rawView.getViewParameters());
      viewTopicNamesAndConfigs.putAll(veniceView.getTopicNamesAndConfigsForVersion(version));
    }
    return viewTopicNamesAndConfigs;
  }

  public static Map<String, Set<Integer>> extractViewPartitionMap(PubSubMessageHeaders pubSubMessageHeaders) {
    Map<String, Set<Integer>> viewPartitionMap = null;
    PubSubMessageHeader header = pubSubMessageHeaders.get(VENICE_VIEW_PARTITIONS_MAP_HEADER);
    if (header != null) {
      try {
        TypeReference<Map<String, Set<Integer>>> typeReference = new TypeReference<Map<String, Set<Integer>>>() {
        };
        viewPartitionMap = ObjectMapperFactory.getInstance().readValue(header.value(), typeReference);
      } catch (IOException e) {
        throw new VeniceException(
            "Failed to parse view partition map from the record's VENICE_VIEW_PARTITIONS_MAP_HEADER",
            e);
      }
    }
    if (viewPartitionMap == null) {
      throw new VeniceException("Unable to find VENICE_VIEW_PARTITIONS_MAP_HEADER in the record's message headers");
    }
    return viewPartitionMap;
  }

  public static PubSubMessageHeader getViewDestinationPartitionHeader(
      Map<String, Set<Integer>> destinationPartitionMap) {
    if (destinationPartitionMap == null) {
      return null;
    }
    try {
      // We could explore more storage efficient ways to pass this information.
      byte[] value = ObjectMapperFactory.getInstance().writeValueAsBytes(destinationPartitionMap);
      return new PubSubMessageHeader(VENICE_VIEW_PARTITIONS_MAP_HEADER, value);
    } catch (JsonProcessingException e) {
      throw new VeniceException("Failed to serialize view destination partition map", e);
    }
  }
}
