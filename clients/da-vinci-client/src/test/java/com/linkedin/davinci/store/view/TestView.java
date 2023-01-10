package com.linkedin.davinci.store.view;

import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.views.VeniceView;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;


public class TestView extends VeniceView {
  private static TestView thisView = null;

  public TestView(Properties props, Store store, Map<String, String> viewParameters) {
    super(props, store, viewParameters);
    synchronized (thisView) {
      if (thisView == null) {
        thisView = new TestView(props, store, viewParameters);
      }
    }
  }

  @Override
  public Map<String, VeniceProperties> getTopicNamesAndConfigsForVersion(int version) {
    return Collections.emptyMap();
  }

  @Override
  public String getWriterClassName() {
    return TestViewWriter.class.getCanonicalName();
  }

  @Override
  public void close() {
    // close out anything which should be shutdown
  }

  public static TestView getInstance() {
    return thisView;
  }

}
