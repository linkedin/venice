package com.linkedin.alpini.base.queuing;

import com.linkedin.alpini.consts.QOS;
import com.linkedin.alpini.consts.config.ConfigBuilder;
import java.util.EnumMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Enumerates all the QOS Policies supported.
 * @author aauradka
 *
 */
public enum QOSPolicy {
  HIGHEST_PRIORITY_W_FAIR_ALLOCATION {
    @Override
    protected <T extends QOSBasedRequestRunnable> SimpleQueue<T> buildQueue(StaticConfig qosPolicyConfig) {
      return new QOSBasedQueue<>(qosPolicyConfig.getFairRatio());
    }
  },
  HIGHEST_PRIORITY_W_FAIR_ALLOCATION_MULTI_QUEUE {
    @Override
    protected <T extends QOSBasedRequestRunnable> SimpleQueue<T> buildQueue(StaticConfig qosPolicyConfig) {
      return new QOSBasedMultiQueue<>(qosPolicyConfig.getMaxQueueLength(), qosPolicyConfig.getFairRatio());
    }
  },
  FCFS {
    @Override
    protected <T extends QOSBasedRequestRunnable> SimpleQueue<T> buildQueue(StaticConfig qosPolicyConfig) {
      return new QOSFCFSQueue<>();
    }
  };

  private static final Logger LOG = LogManager.getLogger(QOSPolicy.class);

  protected abstract <T extends QOSBasedRequestRunnable> SimpleQueue<T> buildQueue(
      QOSPolicy.StaticConfig qosPolicyConfig);

  public static <T extends QOSBasedRequestRunnable> SimpleQueue<T> getQOSPolicy(
      QOSPolicy.StaticConfig qosPolicyConfig) {
    final QOSPolicy qosPolicy;
    final String policy = qosPolicyConfig.getQosPolicy();

    try {
      qosPolicy = QOSPolicy.valueOf(policy);
      LOG.debug("QOS policy selected is {}", qosPolicy);
    } catch (Exception ex) {
      throw new IllegalArgumentException("Invalid qos policy " + ((policy != null) ? policy : "null"), ex);
    }

    return qosPolicy.buildQueue(qosPolicyConfig);
  }

  public static class StaticConfig {
    private final String _qosPolicy;
    private final Map<QOS, Integer> _fairRatio;
    private final int _maxQueueLength;

    public StaticConfig(String qosPolicy, String fairRatio, int maxQueueLength) {
      _qosPolicy = qosPolicy;
      String[] fairRatioArr = fairRatio.split(":");
      if (fairRatioArr.length != 3) {
        throw new IllegalArgumentException(
            "Fair Ratio " + fairRatio + " is incorrectly specified."
                + " It should be of the form LOW:NORMAL:HIGH e.g. 5:15:80");
      }

      _fairRatio = new EnumMap<>(QOS.class);
      _fairRatio.put(QOS.LOW, Integer.valueOf(fairRatioArr[0]));
      _fairRatio.put(QOS.NORMAL, Integer.valueOf(fairRatioArr[1]));
      _fairRatio.put(QOS.HIGH, Integer.valueOf(fairRatioArr[2]));

      _maxQueueLength = maxQueueLength;
    }

    public String getQosPolicy() {
      return _qosPolicy;
    }

    public Map<QOS, Integer> getFairRatio() {
      return _fairRatio;
    }

    public int getMaxQueueLength() {
      return _maxQueueLength;
    }
  }

  public static class Config implements ConfigBuilder<StaticConfig> {
    private String _qosPolicy = QOSPolicy.HIGHEST_PRIORITY_W_FAIR_ALLOCATION.name();
    private String _fairRatio = "5:15:80";
    private int _maxQueueLength = 0;

    @Override
    public StaticConfig build() {
      return new StaticConfig(getQosPolicy(), getFairAllocationRatio(), getMaxQueueLength());
    }

    public int getMaxQueueLength() {
      return _maxQueueLength;
    }

    public void setMaxQueueLength(int maxQueueLength) {
      this._maxQueueLength = maxQueueLength;
    }

    public String getQosPolicy() {
      return _qosPolicy;
    }

    public void setQosPolicy(String qosPolicy) {
      _qosPolicy = qosPolicy;
    }

    public void setFairAllocationRatio(String fairAllocationRatio) {
      _fairRatio = fairAllocationRatio;
    }

    public String getFairAllocationRatio() {
      return _fairRatio;
    }

    @Override
    public String toString() {
      return "{policy=" + getQosPolicy() + ",fairAllocationRatio=" + getFairAllocationRatio() + ",maxQueueLength="
          + getMaxQueueLength() + '}';
    }
  }
}
