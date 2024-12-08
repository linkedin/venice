package com.linkedin.venice.controller;

import com.linkedin.venice.meta.Store;
import com.linkedin.venice.views.VeniceView;
import java.util.Map;
import java.util.Properties;


public class VeniceViewForTest extends VeniceView {
  public VeniceViewForTest(Properties props, Store store, Map<String, String> viewParameters) {
    super(props, store, viewParameters);
  }
}
