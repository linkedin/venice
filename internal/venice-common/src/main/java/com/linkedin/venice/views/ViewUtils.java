package com.linkedin.venice.views;

import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.ReflectUtils;
import com.linkedin.venice.utils.VeniceProperties;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ViewUtils {
  private static final Logger LOGGER = LogManager.getLogger(ViewUtils.class);

  public static VeniceView getVenicePartitioner(String viewClass, VeniceProperties params, Store veniceStore) {
    VeniceView view = ReflectUtils.callConstructor(
        ReflectUtils.loadClass(viewClass),
        new Class<?>[] { VeniceProperties.class, Schema.class },
        new Object[] { params, veniceStore });
    return view;
  }
}
