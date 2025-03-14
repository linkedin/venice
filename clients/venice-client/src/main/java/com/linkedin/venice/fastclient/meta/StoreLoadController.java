package com.linkedin.venice.fastclient.meta;

import com.linkedin.venice.client.exceptions.VeniceClientRateExceededException;
import com.linkedin.venice.fastclient.ClientConfig;
import com.linkedin.venice.reliability.LoadController;
import com.linkedin.venice.utils.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is used to control the load on the store, and it will treat every quota rejected request as
 * a signal to reject more requests on the client side to avoid overloading the server.
 */
public class StoreLoadController {
  private final static Logger LOGGER = LogManager.getLogger(StoreLoadController.class);
  private final LoadController loadController;

  public StoreLoadController(ClientConfig clientConfig) {
    if (clientConfig.isStoreLoadControllerEnabled()) {
      this.loadController = LoadController.newBuilder()
          .setWindowSizeInSec(clientConfig.getStoreLoadControllerWindowSizeInSec())
          .setAcceptMultiplier(clientConfig.getStoreLoadControllerAcceptMultiplier())
          .setMaxRejectionRatio(clientConfig.getStoreLoadControllerMaxRejectionRatio())
          .setRejectionRatioUpdateIntervalInSec(clientConfig.getStoreLoadControllerRejectionRatioUpdateIntervalInSec())
          .build();
    } else {
      this.loadController = null;
    }
  }

  public void recordRejectedRequest() {
    if (loadController != null) {
      loadController.recordRequest();
    }
  }

  public void recordResponse(Throwable exception) {
    if (loadController == null) {
      return;
    }
    if (exception == null || !ExceptionUtils.recursiveClassEquals(exception, VeniceClientRateExceededException.class)) {
      loadController.recordAccept();
    }
    loadController.recordRequest();
  }

  public boolean shouldRejectRequest() {
    return loadController != null ? loadController.shouldRejectRequest() : false;
  }

  public double getRejectionRatio() {
    return loadController != null ? loadController.getRejectionRatio() : 0;
  }
}
