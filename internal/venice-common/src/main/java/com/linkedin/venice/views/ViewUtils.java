package com.linkedin.venice.views;

import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.ReflectUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ViewUtils {
  private static final Logger LOGGER = LogManager.getLogger(ViewUtils.class);

  public static VeniceView getVeniceView(
      String viewClass,
      VeniceProperties params,
      Store veniceStore,
      Map<String, String> extraParameters) {
    VeniceView view = ReflectUtils.callConstructor(
        ReflectUtils.loadClass(viewClass),
        new Class<?>[] { VeniceProperties.class, Store.class, Map.class },
        new Object[] { params, veniceStore, extraParameters });
    return view;
  }
}
