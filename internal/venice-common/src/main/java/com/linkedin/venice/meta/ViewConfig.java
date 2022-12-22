package com.linkedin.venice.meta;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.linkedin.venice.systemstore.schemas.StoreViewConfig;
import java.util.Map;


@JsonDeserialize(as = ViewConfigImpl.class)
public interface ViewConfig extends DataModelBackedStructure<StoreViewConfig> {
  String getClassName();

  Map<String, String> getParams();
}
