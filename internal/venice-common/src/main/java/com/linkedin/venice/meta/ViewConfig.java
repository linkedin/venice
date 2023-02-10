package com.linkedin.venice.meta;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.linkedin.venice.systemstore.schemas.StoreViewConfig;
import java.util.Map;


@JsonDeserialize(as = ViewConfigImpl.class)
public interface ViewConfig extends DataModelBackedStructure<StoreViewConfig> {
  String getViewClassName();

  Map<String, String> getViewParameters();

  void setViewClassName(String className);

  void setViewParameters(Map<String, String> params);

}
