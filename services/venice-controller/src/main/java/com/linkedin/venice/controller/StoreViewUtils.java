package com.linkedin.venice.controller;

import com.linkedin.venice.controller.kafka.protocol.admin.StoreViewConfigRecord;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.VeniceJsonSerializer;
import com.linkedin.venice.meta.ViewConfig;
import com.linkedin.venice.meta.ViewConfigImpl;
import com.linkedin.venice.systemstore.schemas.StoreViewConfig;
import com.linkedin.venice.utils.CollectionUtils;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class StoreViewUtils {
  private static final Logger LOGGER = LogManager.getLogger(StoreViewUtils.class);
  private static final VeniceJsonSerializer<ViewConfig> viewConfigVeniceJsonSerializer =
      new VeniceJsonSerializer<>(ViewConfig.class);

  public static Map<String, StoreViewConfig> convertStringMapViewToStoreViewConfigMap(Map<String, String> stringMap) {
    Map<String, StoreViewConfig> mergedViewConfigRecords = new HashMap<>();
    if (!stringMap.isEmpty()) {
      for (Map.Entry<String, String> stringViewConfig: stringMap.entrySet()) {
        StoreViewConfig newViewConfig;
        try {
          ViewConfig viewConfig =
              viewConfigVeniceJsonSerializer.deserialize(stringViewConfig.getValue().getBytes(), "");
          newViewConfig = new StoreViewConfig(
              viewConfig.getViewClassName(),
              CollectionUtils.getStringKeyCharSequenceValueMapFromStringMap(viewConfig.getViewParameters()));
        } catch (IOException e) {
          LOGGER.error("Failed to serialize provided view config: {}", stringViewConfig.getValue());
          throw new VeniceException("Failed to serialize provided view config:" + stringViewConfig.getValue(), e);
        }
        mergedViewConfigRecords.put(stringViewConfig.getKey(), newViewConfig);
      }
    }
    return mergedViewConfigRecords;
  }

  public static Map<String, ViewConfig> convertStringMapViewToViewConfigMap(Map<String, String> stringMap) {
    return convertStringMapViewToStoreViewConfigMap(stringMap).entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> new ViewConfigImpl(e.getValue())));
  }

  public static Map<String, StoreViewConfigRecord> convertViewConfigMapToStoreViewRecordMap(
      Map<String, ViewConfig> viewConfigMap) {
    return viewConfigMap.entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> convertViewConfigToStoreViewConfigRecord(e.getValue())));
  }

  public static StoreViewConfigRecord convertViewConfigToStoreViewConfigRecord(ViewConfig viewConfig) {
    return new StoreViewConfigRecord(viewConfig.getViewClassName(), viewConfig.dataModel().getViewParameters());
  }
}
