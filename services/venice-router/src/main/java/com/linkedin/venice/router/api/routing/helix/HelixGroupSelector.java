package com.linkedin.venice.router.api.routing.helix;

import com.linkedin.alpini.base.concurrency.TimeoutProcessor;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixInstanceConfigRepository;
import com.linkedin.venice.router.stats.HelixGroupStats;
import io.tehuti.metrics.MetricsRepository;
import java.util.concurrent.TimeUnit;


/**
 * This class is a thin wrapper of {@link HelixInstanceConfigRepository} and {@link HelixGroupSelectionStrategy}, and it
 * will delegate all the related API calls to the corresponding objects.
 * Besides that, this class is also in charge of emitting metrics for each Helix Group.
 */
public class HelixGroupSelector implements HelixGroupSelectionStrategy {
  /**
   * The timeout to reset group counter.
   * So far, there is no need to make it very tight, so we will hard-code it to be 10 seconds.
   * If there is a need to make it configurable, please refactor it.
   */
  private final long HELIX_GROUP_COUNTER_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(10);

  private final HelixInstanceConfigRepository instanceConfigRepository;
  private final HelixGroupSelectionStrategy selectionStrategy;
  private final HelixGroupStats helixGroupStats;

  public HelixGroupSelector(
      MetricsRepository metricsRepository,
      HelixInstanceConfigRepository instanceConfigRepository,
      HelixGroupSelectionStrategyEnum strategyEnum,
      TimeoutProcessor timeoutProcessor) {
    this.helixGroupStats = new HelixGroupStats(metricsRepository);
    this.instanceConfigRepository = instanceConfigRepository;
    Class<? extends HelixGroupSelectionStrategy> strategyClass = strategyEnum.getStrategyClass();
    if (strategyClass.equals(HelixGroupLeastLoadedStrategy.class)) {
      this.selectionStrategy =
          new HelixGroupLeastLoadedStrategy(timeoutProcessor, HELIX_GROUP_COUNTER_TIMEOUT_MS, helixGroupStats);
    } else {
      try {
        this.selectionStrategy = strategyClass.getDeclaredConstructor().newInstance();
      } catch (Exception e) {
        throw new VeniceException("Failed to construct strategy: " + strategyClass.getSimpleName(), e);
      }
    }
  }

  public int getInstanceGroupId(String instanceId) {
    return instanceConfigRepository.getInstanceGroupId(instanceId);
  }

  public int getGroupCount() {
    return instanceConfigRepository.getGroupCount();
  }

  @Override
  public int selectGroup(long requestId, int groupNum) {
    helixGroupStats.recordGroupNum(groupNum);
    int assignedGroupId = selectionStrategy.selectGroup(requestId, groupNum);
    helixGroupStats.recordGroupRequest(assignedGroupId);
    return assignedGroupId;
  }

  @Override
  public void finishRequest(long requestId, int groupId, double latency) {
    selectionStrategy.finishRequest(requestId, groupId, latency);
  }
}
