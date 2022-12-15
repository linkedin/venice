package com.linkedin.davinci.store.view;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.ReflectUtils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.views.VeniceView;
import com.linkedin.venice.views.ViewUtils;
import java.util.Map;
import org.apache.avro.Schema;


public class ViewWriterUtils extends ViewUtils {
  public static VeniceViewWriter getVeniceViewWriter(
      String viewClass,
      VeniceConfigLoader configLoader,
      Store store,
      Schema keySchema,
      Map<String, String> extraViewParameters) {
    VeniceProperties params = configLoader.getCombinedProperties();
    VeniceView view = ReflectUtils.callConstructor(
        ReflectUtils.loadClass(viewClass),
        new Class<?>[] { VeniceProperties.class, Store.class, Map.class },
        new Object[] { params, store, extraViewParameters });

    VeniceViewWriter viewWriter = ReflectUtils.callConstructor(
        ReflectUtils.loadClass(view.getWriterClassName()),
        new Class<?>[] { VeniceConfigLoader.class, Store.class, Schema.class, Map.class },
        new Object[] { configLoader, store, keySchema, extraViewParameters });

    return viewWriter;
  }
}
