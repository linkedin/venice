package com.linkedin.venice.meta;

import com.linkedin.venice.systemstore.schemas.StoreViewConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;


public class ViewConfigImpl implements ViewConfig {
  private final StoreViewConfig viewConfig;

  public ViewConfigImpl(StoreViewConfig viewConfig) {
    this.viewConfig = viewConfig;
  }

  @Override
  public StoreViewConfig dataModel() {
    return this.viewConfig;
  }

  @Override
  public ViewType getViewType() {
    return ViewType.getViewTypeFromInt(this.viewConfig.getViewType());
  }

  @Override
  public Map<String, String> getParams() {
    if (this.viewConfig.getViewParameters() == null) {
      return new HashMap<>();
    } else {
      return this.viewConfig.getViewParameters()
          .entrySet()
          .stream()
          .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
    }
  }
}
