package com.linkedin.davinci.store.view;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.ViewConfig;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;


public class VeniceViewWriterFactory {
  private final VeniceConfigLoader properties;
  private final VeniceWriterFactory veniceWriterFactory;

  public VeniceViewWriterFactory(VeniceConfigLoader properties, VeniceWriterFactory veniceWriterFactory) {
    this.properties = properties;
    this.veniceWriterFactory = veniceWriterFactory;
  }

  public Map<String, VeniceViewWriter> buildStoreViewWriters(Store store, int version, Schema keySchema) {
    Map<String, VeniceViewWriter> storeViewWriters = new HashMap<>();
    // Should only be invoked at time of ingestion task creation, so shouldn't be necessary to check for existence.
    Version storeVersion = store.getVersionOrThrow(version);
    for (Map.Entry<String, ViewConfig> viewConfig: storeVersion.getViewConfigs().entrySet()) {
      String className = viewConfig.getValue().getViewClassName();
      Map<String, String> extraParams = viewConfig.getValue().getViewParameters();
      VeniceViewWriter viewWriter = ViewWriterUtils
          .getVeniceViewWriter(className, properties, store, version, keySchema, extraParams, veniceWriterFactory);
      storeViewWriters.put(viewConfig.getKey(), viewWriter);
    }
    return storeViewWriters;
  }
}
