package com.linkedin.venice.fastclient.meta;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.reliability.LoadController;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class maintains per-instance {@link LoadController} to track the overload status of each instance.
 */
public class InstanceLoadController {
  private static final RedundantExceptionFilter REDUNDANT_EXCEPTION_FILTER =
      RedundantExceptionFilter.getRedundantExceptionFilter();
  private static final Logger LOGGER = LogManager.getLogger(InstanceLoadController.class);
  private final Map<String, LoadController> instanceLoadControllerMap = new VeniceConcurrentHashMap<>();
  private final InstanceHealthMonitorConfig config;
  private final boolean loadControllerEnabled;

  public InstanceLoadController(InstanceHealthMonitorConfig config) {
    this.config = config;
    this.loadControllerEnabled = config.isLoadControllerEnabled();
  }

  private LoadController getLoadController(String instanceId) {
    return instanceLoadControllerMap.computeIfAbsent(
        instanceId,
        ignored -> LoadController.newBuilder()
            .setWindowSizeInSec(config.getLoadControllerWindowSizeInSec())
            .setAcceptMultiplier(config.getLoadControllerAcceptMultiplier())
            .setMaxRejectionRatio(config.getLoadControllerMaxRejectionRatio())
            .setRejectionRatioUpdateIntervalInSec(config.getLoadControllerRejectionRatioUpdateIntervalInSec())
            .build());
  }

  private void logOverloadedInstance(String instanceId, LoadController loadController) {
    if (!loadControllerEnabled) {
      return;
    }
    if (!loadController.isOverloaded()) {
      return;
    }
    String logMessage = String
        .format("Instance: %s is overloaded, start rejecting request on client side, rejection ratio: {}", instanceId);
    if (!REDUNDANT_EXCEPTION_FILTER.isRedundantException(logMessage)) {
      LOGGER.warn(logMessage, loadController.getRejectionRatio());
    }
  }

  public void recordResponse(String instanceId, int responseStatus) {
    if (!loadControllerEnabled) {
      return;
    }
    LoadController loadController = getLoadController(instanceId);
    if (responseStatus != HttpConstants.SC_SERVICE_OVERLOADED) {
      loadController.recordAccept();
    }
    loadController.recordRequest();
    logOverloadedInstance(instanceId, loadController);
  }

  /**
   * This method will check whether the incoming request to the specified instance should be rejected or not.
   * If it gets rejected, it will record the request in the {@link LoadController} to track the overload status.
   */
  public boolean shouldRejectRequest(String instanceId) {
    if (!loadControllerEnabled) {
      return false;
    }
    LoadController loadController = getLoadController(instanceId);
    logOverloadedInstance(instanceId, loadController);

    if (loadController.shouldRejectRequest()) {
      loadController.recordRequest();
      return true;
    }
    return false;
  }

  public int getTotalNumberOfOverLoadedInstances() {
    return (int) instanceLoadControllerMap.values().stream().filter(LoadController::isOverloaded).count();
  }

  public double getRejectionRatio(String instanceId) {
    if (!loadControllerEnabled) {
      return 0;
    }
    return getLoadController(instanceId).getRejectionRatio();
  }

}
