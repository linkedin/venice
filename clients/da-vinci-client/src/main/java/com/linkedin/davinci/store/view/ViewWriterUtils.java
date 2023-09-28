package com.linkedin.davinci.store.view;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.utils.ReflectUtils;
import com.linkedin.venice.views.VeniceView;
import com.linkedin.venice.views.ViewUtils;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.Schema;


public class ViewWriterUtils extends ViewUtils {
  public static VeniceViewWriter getVeniceViewWriter(
      String viewClass,
      VeniceConfigLoader configLoader,
      Store store,
      Schema keySchema,
      Map<String, String> extraViewParameters) {
    Properties params = configLoader.getCombinedProperties().toProperties();
    VeniceView view = ReflectUtils.callConstructor(
        ReflectUtils.loadClass(viewClass),
        new Class<?>[] { Properties.class, Store.class, Map.class },
        new Object[] { params, store, extraViewParameters });

    VeniceViewWriter viewWriter = ReflectUtils.callConstructor(
        ReflectUtils.loadClass(view.getWriterClassName()),
        new Class<?>[] { VeniceConfigLoader.class, Store.class, Schema.class, Map.class },
        new Object[] { configLoader, store, keySchema, extraViewParameters });

    return viewWriter;
  }

  public static PubSubProducerCallback createViewCountDownCallback(int viewCount, Runnable baseCallback) {
    return new PubSubProducerCallback() {
      AtomicInteger count = new AtomicInteger(viewCount);
      Runnable callback = baseCallback;

      @Override
      public void onCompletion(PubSubProduceResult produceResult, Exception exception) {
        if (exception != null) {
          // Need to figure out exceptions
        } else {
          if (count.decrementAndGet() <= 0) {
            callback.run();
          }
        }
      }
    };
  }
}
