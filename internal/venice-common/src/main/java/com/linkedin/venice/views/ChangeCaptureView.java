package com.linkedin.venice.views;

import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;


public class ChangeCaptureView extends VeniceView {
  public static String CHANGE_CAPTURE_TOPIC_SUFFIX = "_cc";
  public static String CHANGE_CAPTURE_VIEW_WRITER_CLASS_NAME =
      "com.linkedin.davinci.store.view.ChangeCaptureViewWriter";

  public ChangeCaptureView(Properties props, Store store, Map<String, String> viewParameters) {
    super(props, store, viewParameters);
  }

  @Override
  public Map<String, VeniceProperties> getTopicNamesAndConfigsForVersion(int version) {
    return Collections.singletonMap(
        Version.composeKafkaTopic(store.getName(), version) + CHANGE_CAPTURE_TOPIC_SUFFIX,
        new VeniceProperties());
  }

  @Override
  public String getWriterClassName() {
    return CHANGE_CAPTURE_VIEW_WRITER_CLASS_NAME;
  }

}
