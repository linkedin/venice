package com.linkedin.venice.controller;

import com.linkedin.venice.controller.kafka.protocol.admin.StoreViewConfigRecord;
import com.linkedin.venice.meta.ViewConfig;
import com.linkedin.venice.meta.ViewConfigImpl;
import com.linkedin.venice.meta.ViewType;
import com.linkedin.venice.systemstore.schemas.StoreViewConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;


public class StoreViewUtils {
  static Map<String, StoreViewConfigRecord> convertStringMapViewToStoreViewConfigRecord(Map<String, String> stringMap) {
    // TODO: Today we only support one kind of view and it takes no arguments. So all this returns is a map with a
    // single element that is using ChangeCapture as the output type and whatever key is passed in.
    Map<String, StoreViewConfigRecord> mergedViewConfigRecords = new HashMap<>();
    if (!stringMap.isEmpty()) {
      StoreViewConfigRecord newViewConfigRecord = new StoreViewConfigRecord(ViewType.CHANGE_CAPTURE.value, null);
      mergedViewConfigRecords.put(stringMap.entrySet().iterator().next().getKey(), newViewConfigRecord);
    }
    return mergedViewConfigRecords;
  }

  static Map<String, StoreViewConfig> convertStringMapViewToStoreViewConfig(Map<String, String> stringMap) {
    // TODO: Today we only support one kind of view and it takes no arguments. So all this returns is a map with a
    // single element that is using ChangeCapture as the output type and whatever key is passed in.
    Map<String, StoreViewConfig> mergedViewConfigs = new HashMap<>();
    if (!stringMap.isEmpty()) {
      StoreViewConfig newStoreViewConfig = new StoreViewConfig(ViewType.CHANGE_CAPTURE.value, null);
      mergedViewConfigs.put(stringMap.entrySet().iterator().next().getKey(), newStoreViewConfig);
    }
    return mergedViewConfigs;
  }

  static Map<String, ViewConfig> convertStringMapViewToViewConfig(Map<String, String> stringMap) {
    return convertStringMapViewToStoreViewConfig(stringMap).entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> new ViewConfigImpl(e.getValue())));
  }
}
