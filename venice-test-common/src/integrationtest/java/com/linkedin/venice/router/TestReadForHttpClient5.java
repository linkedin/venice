package com.linkedin.venice.router;

import com.linkedin.venice.router.httpclient.StorageNodeClientType;
import com.linkedin.venice.utils.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class TestReadForHttpClient5 extends TestRead {
  private final Logger LOGGER = LogManager.getLogger(this.getClass());
  @Override
  protected StorageNodeClientType getStorageNodeClientType() {
    return StorageNodeClientType.HTTP_CLIENT_5_CLIENT;
  }

  @Override
  protected boolean isHttp2Enabled() {
    return true;
  }

  @Override
  protected boolean isTestEnabled() {
    boolean testEnabled = Utils.getJavaMajorVersion() >= 11;
    if (!testEnabled) {
      LOGGER.info("All the tests are disabled since StorageNodeClientType: {} with HTTP/2 enabled"
          + " requires JDK11 or above", StorageNodeClientType.HTTP_CLIENT_5_CLIENT);
    }
    return testEnabled;
  }
}
