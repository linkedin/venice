package com.linkedin.venice.views;

import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.ReflectUtils;
import java.util.Map;
import java.util.Properties;


public class ViewUtils {
  public static final String PARTITION_COUNT = "sub.partition.count";
  public static final String USE_FAST_KAFKA_OPERATION_TIMEOUT = "use.fast.kafka.operation.timeout";

  public static final String LOG_COMPACTION_ENABLED = "log.compaction.enabled";

  public static final String ETERNAL_TOPIC_RETENTION_ENABLED = "eternal.topic.retention.enabled";

  public static VeniceView getVeniceView(
      String viewClass,
      Properties params,
      Store veniceStore,
      Map<String, String> extraParameters) {
    VeniceView view = ReflectUtils.callConstructor(
        ReflectUtils.loadClass(viewClass),
        new Class<?>[] { Properties.class, Store.class, Map.class },
        new Object[] { params, veniceStore, extraParameters });
    return view;
  }
}
