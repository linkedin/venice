package com.linkedin.davinci.store.view;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.ReflectUtils;
import com.linkedin.venice.views.VeniceView;
import com.linkedin.venice.views.ViewUtils;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.Schema;


public class ViewWriterUtils extends ViewUtils {
  public static VeniceViewWriter getVeniceViewWriter(
      String viewClass,
      VeniceConfigLoader configLoader,
      Store store,
      int version,
      Schema keySchema,
      Map<String, String> extraViewParameters,
      VeniceWriterFactory veniceWriterFactory) {
    Properties params = configLoader.getCombinedProperties().toProperties();
    VeniceView view = ReflectUtils.callConstructor(
        ReflectUtils.loadClass(viewClass),
        new Class<?>[] { Properties.class, String.class, Map.class },
        new Object[] { params, store.getName(), extraViewParameters });

    // Make a copy of the view parameters map to insert producer configs
    Map<String, String> viewParamsWithProducerConfigs = new HashMap<>(extraViewParameters);
    viewParamsWithProducerConfigs
        .put(NEARLINE_PRODUCER_COMPRESSION_ENABLED, Boolean.toString(store.isNearlineProducerCompressionEnabled()));
    viewParamsWithProducerConfigs
        .put(NEARLINE_PRODUCER_COUNT_PER_WRITER, Integer.toString(store.getNearlineProducerCountPerWriter()));
    return ReflectUtils.callConstructor(
        ReflectUtils.loadClass(view.getWriterClassName()),
        new Class<?>[] { VeniceConfigLoader.class, Version.class, Schema.class, Map.class, VeniceWriterFactory.class },
        new Object[] { configLoader, store.getVersionOrThrow(version), keySchema, viewParamsWithProducerConfigs,
            veniceWriterFactory });
  }
}
