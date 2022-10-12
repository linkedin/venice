package com.linkedin.venice.hadoop.utils;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestControllerUtils {
  private final static String TEST_URL = "/test/url";
  private final static String TEST_DISCOVER_URL = "/test/discover/url";

  @Test
  public void testGetVeniceControllerUrl() {
    Properties validProps = new Properties();
    validProps.put(VenicePushJob.VENICE_URL_PROP, TEST_URL);
    VeniceProperties valid = new VeniceProperties(validProps);
    Assert.assertEquals(TEST_URL, ControllerUtils.getVeniceControllerUrl(valid));
  }

  @Test
  public void testGetVeniceControllerDiscoverUrl() {
    Properties validProps = new Properties();
    validProps.put(VenicePushJob.VENICE_URL_PROP, TEST_URL);
    validProps.put(VenicePushJob.VENICE_DISCOVER_URL_PROP, TEST_DISCOVER_URL);
    VeniceProperties valid = new VeniceProperties(validProps);
    Assert.assertEquals(TEST_DISCOVER_URL, ControllerUtils.getVeniceControllerUrl(valid));
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testGetVeniceControllerUrlThrowsException() {
    ControllerUtils.getVeniceControllerUrl(new VeniceProperties());
  }
}
