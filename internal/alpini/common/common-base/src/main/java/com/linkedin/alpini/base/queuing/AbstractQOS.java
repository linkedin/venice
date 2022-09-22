package com.linkedin.alpini.base.queuing;

import com.linkedin.alpini.base.misc.CollectionUtil;
import com.linkedin.alpini.consts.QOS;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Created by acurtis on 7/27/17.
 */
public abstract class AbstractQOS {
  /** QOS order starting from LOW */
  static final List<QOS> LOW_FIRST = Collections.unmodifiableList(Arrays.asList(QOS.LOW, QOS.HIGH, QOS.NORMAL));
  /** QOS order starting from NORMAL */
  static final List<QOS> NORMAL_FIRST = Collections.unmodifiableList(Arrays.asList(QOS.NORMAL, QOS.HIGH, QOS.LOW));
  /** QOS order starting from HIGH */
  static final List<QOS> HIGH_FIRST = Collections.unmodifiableList(Arrays.asList(QOS.HIGH, QOS.NORMAL, QOS.LOW));

  protected final Logger _log = LogManager.getLogger(getClass());

  protected final Map<QOS, Integer> _qosBasedAllocations;
  protected final int _allocationTotal;

  protected AbstractQOS(Map<QOS, Integer> qosBasedAllocations) {
    _qosBasedAllocations = Collections.unmodifiableMap(new EnumMap<>(qosBasedAllocations));
    _allocationTotal = _qosBasedAllocations.values().stream().mapToInt(Integer::intValue).sum();

    for (QOS qos: QOS.values()) {
      if (!_qosBasedAllocations.containsKey(qos)) {
        throw new IllegalArgumentException(
            "The QOSAllocations must have an entry for all QOS levels. LOW, NORMAL and HIGH. " + "Missing entry for : "
                + qos);
      }
    }
  }

  /**
   * Computes and returns the order in which the Queues are to be polled/peeked for this request
   * @return queue order
   */
  protected List<QOS> getQueuePollOrder() {
    int nextInt = ThreadLocalRandom.current().nextInt(_allocationTotal);

    if ((nextInt -= _qosBasedAllocations.get(QOS.LOW)) < 0) { // SUPPRESS CHECKSTYLE InnerAssignment
      return LOW_FIRST;
    } else if (nextInt < _qosBasedAllocations.get(QOS.NORMAL)) {
      return NORMAL_FIRST;
    } else {
      return HIGH_FIRST;
    }
  }

  public static Map<QOS, Integer> getDefaultQOSAllocation() {
    return CollectionUtil.mapOf(QOS.LOW, 5, QOS.NORMAL, 15, QOS.HIGH, 80);
  }

}
