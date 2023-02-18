package com.linkedin.venice.meta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.venice.systemstore.schemas.StoreViewConfig;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;


@JsonIgnoreProperties(ignoreUnknown = true)
public class ViewConfigImpl implements ViewConfig {
  private final StoreViewConfig viewConfig;

  @JsonCreator
  public ViewConfigImpl(
      @JsonProperty("viewClassName") String viewClassName,
      @JsonProperty("viewParameters") Map<String, String> viewParameters) {
    Map<String, CharSequence> params;
    if (viewParameters == null || viewParameters.isEmpty()) {
      params = Collections.emptyMap();
    } else {
      params = viewParameters.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
    this.viewConfig = new StoreViewConfig(viewClassName, params);
  }

  public ViewConfigImpl(StoreViewConfig viewConfig) {
    this.viewConfig = viewConfig;
  }

  @Override
  public StoreViewConfig dataModel() {
    return this.viewConfig;
  }

  @Override
  public String getViewClassName() {
    return this.viewConfig.getViewClassName().toString();
  }

  @Override
  public Map<String, String> getViewParameters() {
    if (this.viewConfig.getViewParameters() == null) {
      return new HashMap<>();
    } else {
      return this.viewConfig.getViewParameters()
          .entrySet()
          .stream()
          .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
    }
  }

  @Override
  public void setViewClassName(String className) {
    this.viewConfig.setViewClassName(className);
  }

  @Override
  public void setViewParameters(Map<String, String> viewParameters) {
    Map<String, CharSequence> params;
    if (viewParameters == null || viewParameters.isEmpty()) {
      params = Collections.emptyMap();
    } else {
      params = viewParameters.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
    this.viewConfig.setViewParameters(params);
  }
}
